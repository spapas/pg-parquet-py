# Postgres to parquet with python

Export a query from postgres to parquet with python

## Requirements

This project works with supported versions of python 3.

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
