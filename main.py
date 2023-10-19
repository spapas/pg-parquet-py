import pyarrow as pa
import pyarrow.parquet as pq
import psycopg
from dotenv import load_dotenv
import os
from collections import OrderedDict

import adbc_driver_postgresql.dbapi

_query = None


def get_connection_dsn():
    return os.getenv("DB_DSN", "postgresql://postgres@127.0.0.1/postgres")


def write_batch(name_types, schema):
    batch = pa.record_batch(
        [
            pa.array(name_types[k]["values"], type=name_types[k]["type"])
            for k in name_types.keys()
        ],
        schema,
    )
    writer.write_batch(batch)


def get_query():
    global _query
    if _query:
        return _query
    with open("query.sql", "r", encoding="utf-8") as f:
        _query = f.read()
        return _query


def get_query_with_limit():
    query = get_query()
    if "limit" not in query:
        return query + " limit 1"
    else:
        lidx = query.index("limit") + 6
        q = query[:lidx] + "1"
        return q


if __name__ == "__main__":
    load_dotenv()
    batch_size = int(os.getenv("BATCH_SIZE", "10000"))

    with adbc_driver_postgresql.dbapi.connect(get_connection_dsn()) as conn:
        with conn.cursor() as cur:
            cur.execute(get_query_with_limit())
            name_types = OrderedDict(
                {x[0]: {"type": x[1], "values": []} for x in cur.description}
            )

    name_type_keys = list(name_types.keys())

    schema = pa.schema([(x[0], x[1]["type"]) for x in name_types.items()])
    print(schema)
    
    with pq.ParquetWriter(
        "bigfile.parquet", schema=schema, compression="gzip"
    ) as writer:
        with psycopg.connect(get_connection_dsn()) as conn:
            with conn.cursor("passenger-cursor-enum") as cur:
                cur.itersize = batch_size
                cur.execute(get_query())

                print("Query executed...")
                for idx, record in enumerate(cur):
                    for ridx, value in enumerate(record):
                        name_types[name_type_keys[ridx]]["values"].append(value)

                    if idx % batch_size == 0:
                        print("Writing batch {}...".format(idx // batch_size))
                        write_batch(name_types, schema)

                        id = []
                        for x in name_type_keys:
                            name_types[x]["values"] = []

                write_batch(name_types, schema)
