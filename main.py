import sys, os, time, logging, datetime
import pyarrow as pa
import pyarrow.parquet as pq
import psycopg
from dotenv import load_dotenv
from collections import OrderedDict
import adbc_driver_postgresql.dbapi
import duckdb

_query = None


tt = time.time()
gt = time.time()


def debug_w_time(msg):
    global tt
    logging.debug(
        f"{msg} ~~~ time: {time.time() - tt :.2f}, accumulated time: {time.time() - gt :.2f} ~~~"
    )
    tt = time.time()


def get_connection_dsn():
    return os.getenv("DB_DSN", "postgresql://postgres@127.0.0.1/postgres")


def write_batch(writer, name_types, schema):
    batch = pa.record_batch(
        [
            pa.array(name_types[k]["values"], type=name_types[k]["type"])
            for k in name_types.keys()
        ],
        schema,
    )
    writer.write_batch(batch)


def get_query(query_file):
    global _query
    if _query:
        return _query
    with open(query_file, "r", encoding="utf-8") as f:
        _query = f.read()
        return _query


def get_query_with_limit(query_file):
    query = get_query(query_file)
    # remove everything after where
    if "WHERE" in query:
        query = query[: query.index("WHERE")]

    if "limit" not in query:
        return query + " limit 1"
    else:
        lidx = query.index("limit") + 6
        q = query[:lidx] + "1"
        return q


if __name__ == "__main__":
    load_dotenv()

    loglevel = os.getenv("LOGLEVEL", "INFO")
    logging.basicConfig(
        format="%(asctime)s %(levelname)s %(message)s",
        level=getattr(logging, loglevel),
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    debug_w_time("Starting with debug")

    if len(sys.argv) != 3:
        print("Usage: python main.py <query_file> <output_folder>")
        sys.exit(1)
    query_file = sys.argv[1]
    output_folder = sys.argv[2]

    try:
        res = duckdb.sql(
            f"SELECT max(id) FROM read_parquet('{output_folder}/**/*.parquet')"
        )
        min_id = res.fetchone()[0]
    except duckdb.duckdb.IOException:
        min_id = 0

    debug_w_time(f"Existing max_id = {min_id}")

    batch_size = int(os.getenv("BATCH_SIZE", "10000"))
    logging.info("Starting with batch size of {}".format(batch_size))

    debug_w_time("Trying to connect to db and read query column types")
    with adbc_driver_postgresql.dbapi.connect(get_connection_dsn()) as conn:
        with conn.cursor() as cur:
            cur.execute(get_query_with_limit(query_file))
            name_types = OrderedDict(
                {x[0]: {"type": x[1], "values": []} for x in cur.description}
            )

    name_type_keys = list(name_types.keys())

    schema = pa.schema([(x[0], x[1]["type"]) for x in name_types.items()])
    debug_w_time(schema)

    compression = os.getenv("COMPRESSION", "NONE")
    now = datetime.datetime.now()
    now_str = now.strftime("%Y%m%dT%H%M%S")

    output_file = os.path.join(output_folder, f"output-{now_str}.parquet")
    with pq.ParquetWriter(
        output_file, schema=schema, compression=compression
    ) as writer:
        with psycopg.connect(get_connection_dsn()) as conn:
            debug_w_time("Connected, starting to execute query...")
            with conn.cursor("pg-parquet-cursor") as cur:
                cur.itersize = batch_size
                cur.execute(get_query(query_file), [min_id])

                debug_w_time("Query executed...")
                for idx, record in enumerate(cur):
                    for ridx, value in enumerate(record):
                        name_types[name_type_keys[ridx]]["values"].append(value)

                    if idx % batch_size == 0:
                        debug_w_time("Writing batch {}...".format(idx // batch_size))
                        write_batch(writer, name_types, schema)

                        id = []
                        for x in name_type_keys:
                            name_types[x]["values"] = []

                write_batch(writer, name_types, schema)

    logging.info("DONE!")
