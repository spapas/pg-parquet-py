import pyarrow as pa
import pyarrow.parquet as pq
import psycopg
from dotenv import load_dotenv
import os
from collections import OrderedDict

import adbc_driver_postgresql.dbapi

load_dotenv()
BATCH_SIZE = 10000


def get_connection_dsn():
    return os.getenv("DB_DSN", "postgresql://postgres@127.0.0.1/postgres")


if __name__ == "__main__":
    with adbc_driver_postgresql.dbapi.connect(get_connection_dsn()) as conn:
        print(0)
        with conn.cursor() as cur:
            cur.execute(
                """
                            SELECT p.id, p.departureport, p.arrivalport, s.depdate, s.id as sid, p.price
                                FROM passenger p LEFT JOIN sailing s on p.sailing_fk = s.id
                                limit 1
                    """
            )
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
                cur.itersize = BATCH_SIZE
                cur.execute(
                    """
                            SELECT p.id, p.departureport, p.arrivalport, s.depdate, s.id, p.price
                                FROM passenger p LEFT JOIN sailing s on p.sailing_fk = s.id
                                order by p.id
                                limit 1000000
                    """
                )

                print("Query executed...")
                for idx, record in enumerate(cur):
                    for ridx, value in enumerate(record):
                        name_types[name_type_keys[ridx]]["values"].append(value)

                    if idx % BATCH_SIZE == 0:
                        print("Writing batch {}...".format(idx // BATCH_SIZE))

                        batch = pa.record_batch(
                            [
                                pa.array(
                                    name_types[x]["values"], type=name_types[x]["type"]
                                )
                                for x in name_type_keys
                            ],
                            schema,
                        )
                        writer.write_batch(batch)
                        id = []
                        for x in name_type_keys:
                            name_types[x]["values"] = []
