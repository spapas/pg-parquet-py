# Postgres to parquet with python

Export a query from postgres to [parquet](https://parquet.apache.org/) with python. Apache parquet is 
open source, column-oriented data file format designed for efficient data storage and retrieval. The
parquet file can then be used with a columnar database or even queried directly using something like
[duckdb](https://duckdb.org/docs/data/parquet/overview.html).

## Requirements

This project works with supported versions of python 3. All requirements are listed in the `requirements.txt` file.
It uses the following dependencies:

* pyarrow: the parquet file is created with [apache arrow](https://arrow.apache.org/docs/index.html); this is its python bindings
* adbc-driver-postgresql: this is the [arrowdb connect - adbc](https://arrow.apache.org/docs/format/ADBC.html) driver for postgresql; it is used to retrieve the types of the columns of the queries so they can be re-used on the parquet file
* psycopg: the library to query postgresql
* python-dotenv: to load configuration from an `.env` file

## Installation

Create a virtualenv and install the requirements:

```bash
$ python -m venv venv
$ source venv/bin/activate
$ pip install -r requirements.txt
```

or in windows

```plain
> py -3 -m venv vevn
> venv\Scripts\activate.bat
> pip install -r requirements.txt
```


## Usage

Copy over `.env.template` to `.env` and setup your database dsn and any other options you want. You can also use environment variables instead of the `.env` file.

Then run `python main.py query_file.sql` where the query_file should contain the SQL query whose contents you want to export to the parquet file. See the file `query.sql` for an example. The output file will be named `output.parquet` by default.

## Configuration

You can set the `LOGLEVEL` to `DEBUG` to see more messages including timings or to `ERROR` to see only error messages. The default is `INFO`. You can also set the `COMPRESSION` to `SNAPPY` (most common), `GZIP`, `BROTLI`, `LZ4` or `ZSTD`. The default is `NONE`. The `BATCH_SIZE` is the number of rows to fetch at a time from the database. The default is `10000`. Finally, the `DB_DSN` must have the format `DB_DSN=postgresql://user:pass@host/db` with correct values for user, pass, host and db name.

## Why?

After you've created the parquet file of your data you import it at a columnar database or even query it directly using something like 
[duckdb](https://duckdb.org/docs/data/parquet/overview.html). Duckdb [binaries](https://duckdb.org/docs/installation/) are available for most systems or you can use a library to query the parquet file from within your app. Clickhouse db can also [query or import parquet] files (https://clickhouse.com/docs/en/integrations/data-formats/parquet).

For example run something `duckdb -c "select count(*) from output.parquet"`.

To give you an example of the timing differences: I had a table with ~ 150M rows. It took ~ 45 minutes to create the parquet file for this table resulting in an 1.3GB file (with SNAPPY compression). Then I could run aggregates for this data (group by, sum, count, etc) in seconds. 

The same aggregates on the original table took hours. To consider the difference, to run a `count(*)` on the original table needs more than **10 minutes**(!). For a simple group by two columns and a count it takes **like 18 minutes**. The `count(*)` query for the parquet file takes **half a second** and the group by query takes **3 seconds**. 

