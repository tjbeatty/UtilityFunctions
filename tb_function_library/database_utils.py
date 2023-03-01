from __future__ import annotations
import os
import sys

dir_path = os.path.dirname(os.path.realpath(__file__))
sys.path.append(dir_path)

import pandas as pd
from pandas import DataFrame as DF
from multiprocessing.connection import Connection
from psycopg2.sql import SQL
from connection_utils import LastpassManager, AwsDatabaseConnectionManager
from queries_as_functions import (
    row_count_query,
    tables_in_schema_query,
    columns_dtypes_of_table_query,
    generic_sql_query,
)
from aws_db_details import CONFIGURED_CLUSTER_LIST

# Initiate logging
from log_config import get_logger

log = get_logger(__name__)


def results_to_df(conn: Connection, query_func: SQL) -> DF:
    """Returns a dataframe from SQL query results

    Parameters
    ----------
    conn : Connection
        a database connection
    query_func: SQL
        a SQL query formatted as a Psycopg2 SQL object


    Returns
    -------
    DataFrame
        the output of running the input query on the input database, as a Pandas DataFrame
    """
    data, cur = query_table(conn, query_func)

    # Identify column names for dataframe
    cols = []
    for col in cur.description:
        cols.append(col[0])

    df = pd.DataFrame(data=data, columns=cols)

    return df


def connect_to_db_with_psycopg2_lpass(lpass_manager: LastpassManager) -> Connection:
    """A function to create a Psycopg2 database connection using a LastPass entry

    Parameters
    ----------
    lpass_manager : LastpassManager
        a LastPass entry name


    Returns
    -------
    Connection
        a Psycopg2 database connection
    """
    conn = lpass_manager.create_psycopg2_connection()
    conn.autocommit = True

    log.info(f"Connected to {lpass_manager.database}")

    return conn


def connect_to_db_with_sqlalchemy_lpass(lpass_manager: LastpassManager) -> Connection:
    """A function to create a SQLAlchemy database connection using a LastPass entry

    Parameters
    ----------
    lpass_manager : LastpassManager
        a LastPass entry name

    Returns
    -------
    Connection
        a SQLAlchemy database connection
    """
    conn = lpass_manager.create_sqlalchemy_connection()
    conn.autocommit = True

    return conn


def connect_to_db_with_psycopg2_nickname(
    db_nickname: str,
) -> Connection:
    f"""A function to create a Psycopg2 database connection using a database nickname that accesses an AwsClusterSecretsMap object

    Parameters
    ----------
    db_nickname : str
        a database nickname. Valid options include: {CONFIGURED_CLUSTER_LIST}

    Returns
    -------
    Connection
        a Psycopg2 database connection
    """
    msp_db = AwsDatabaseConnectionManager(db_nickname)
    conn = msp_db.create_psycopg2_connection()
    conn.autocommit = True

    log.info(f"Connected to {msp_db.database}")

    return conn


def connect_to_db_with_sqlalchemy_nickname(
    db_nickname: AwsDatabaseConnectionManager,
) -> Connection:
    f"""A function to create a SQLAlchemy database connection using a database nickname that accesses an AwsClusterSecretsMap object

    Parameters
    ----------
    db_nickname : str
        a database nickname. Valid options include: {CONFIGURED_CLUSTER_LIST}

    Returns
    -------
    Connection
        a SQLAlchemy database connection
    """
    msp_db = AwsDatabaseConnectionManager(db_nickname)
    conn = msp_db.create_sqlalchemy_connection()
    conn.autocommit = True
    log.info(f"Connected to {msp_db.database}")

    return conn


def columns_from_table(schema: str, table: str, conn: Connection):
    """Return the column names from a table

    Parameters
    ----------
    schema : str
        database schema
    table: str

    Returns
    -------
    Connection
        a SQLAlchemy database connection"""
    df_columns_dtypes = results_to_df(
        conn, columns_dtypes_of_table_query(schema, table)
    )

    return df_columns_dtypes


def prettify_query(ugly_query: str) -> str:
    """Format a query so it can be printed to the console

    Parameters
    ----------
    ugly_query : str
        a SQL query

    Returns
    -------
    str
        a SQL query, but formatted prettier so it can be printed to the console
    """
    query_as_list = ugly_query.split("\n")
    query_as_list = [l.strip() for l in query_as_list]
    num_blanks = query_as_list.count("")
    for i in range(num_blanks):
        query_as_list.remove("")

    return "\n".join(query_as_list)


def query_table(conn: Connection, query_func: SQL):
    """Queries a SQL table

    Parameters
    ----------
    conn : Connection
        a database connection

    query_func : SQL
        a SQL query formatted as a Psycopg2 SQL object

    Returns
    -------
    data, cur
        returns the results from the query, and the connection cursor"""

    with conn.cursor() as cur:
        query_string = prettify_query(query_func.as_string(conn))
        log.info(f"Running Query:\n\n{query_string}\n")

        cur.execute(query_func)
        query_string = prettify_query(cur.query.decode())
        data = cur.fetchall()

    return data, cur


def check_if_schema_exists(schema: str, conn: Connection) -> DF:
    """Queries the given schema and returns a DataFrame of all tables available. If schema doesn't exist, returns False

    Parameters
    ----------
    schema : str
        a database schema

    conn : Connection
        a database connection

    Returns
    -------
    DataFrame
        returns a DataFrame of all tables available in a given schema"""

    # Get list of tables in schema
    df_tables_in_schema = results_to_df(conn, tables_in_schema_query(schema))
    if df_tables_in_schema.empty:
        log.warning(f"Schema `{schema}` does not exist.")
        return False
    else:
        return df_tables_in_schema


def get_table_row_count(schema: str, table_name: str, conn: Connection) -> int:
    """Returns the row count for a given table

    Parameters
    ----------
    schema : str
        a database schema

    table_name: str
        the name of a table in the above schema

    conn : Connection
        a database connection

    Returns
    -------
    int
        row count for the table"""

    df_count = results_to_df(conn, row_count_query(schema, table_name))

    return df_count.iloc[0, 0]


def check_if_table_exists(schema: str, table_name: str, df_tables_in_schema: DF) -> str:
    """Returns a table name to query based on the inputs. If the base name exists as a table, it will return that.

    Parameters
    ----------
    schema : str
        a database schema

    table_name: str
        the name of a table in the above schema

    df_tables_in_schema : DataFrame
        a DataFrame of all the tables available in the Schema, likely output by check_if_schema_exists()

    Returns
    -------
    str
        if the table exists in the schema, it returns the table name, else returns False
    """

    # Does the table as written exist in the schema?
    if (df_tables_in_schema["table_name"] == table_name).any():
        log.info(f"Found '{table_name}' in the '{schema}' schema")

        return table_name

    elif table_name == "":
        log.error("You entered a blank name for the table")
    else:
        log.info(f"'{table_name}' does not yet exist in '{schema}'")
        return False


def insert_df_to_db(
    df: DF, conn: Connection, schema: str, table_name: str, if_exists: str = "fail"
):
    """Used to create a new table and insert data into it.

    Parameters
    ----------
    df : DataFrame
        a database schema

    conn : Connection
        a database connection

    schema : str
        a database schema

    table_name: str
        the name of a table in the above schema


    if_exists : {'fail', 'replace', 'append'}, default: 'fail'
        How to behave if the table already exists.
        - fail: Raise a ValueError.
        - replace: Drop the table before inserting new values.
        - append: Insert new values to the existing table.


    Returns
    -------
    int
        The number of rows inserted into the table
    """
    log.info(f"Load to '{table_name}' starting...")
    result = df.to_sql(table_name, conn, schema, if_exists=if_exists, index=False)
    log.info(f"Load to '{table_name}' COMPLETE!!!")
    return result


def find_table_to_query(schema: str, base_table_name: str, conn: Connection) -> str:
    """Returns a table name to query based on the inputs.

    If the base name exists as a table, it will return that.
    If there are multiple, dated tables with the base name,
    it will return the most recent table created

    Parameters
    ----------
    schema : str
        a database schema

    base_table_name: str
        the name of a table in the above schema, with any date suffixes removed

    conn : Connection
        a database connection

    Returns
    -------
    str
        most-recent table that matches the input base_table_name
    """

    # Get list of tables in schema
    df_tables_in_schema = results_to_df(conn, tables_in_schema_query(schema))

    if df_tables_in_schema.empty:
        raise ValueError(f"Could not find the schema '{schema}'")
    table_to_query = False
    # Does the table as written exist in the schema (i.e. w/o a date appended)?
    if (df_tables_in_schema["table_name"] == base_table_name).any():
        table_to_query = base_table_name
        log.info(f"Found {table_to_query} in the database")

        return table_to_query
    else:
        # If the base table name doesn't exist, assume dated tables get generated daily
        # and retrieve the table with the largest table name that matches base pattern (i.e most recent)
        filter_pattern = "^" + base_table_name + "_" + "\d{8}"
        table_to_query = (
            df_tables_in_schema[
                df_tables_in_schema["table_name"].str.contains(
                    filter_pattern, regex=True
                )
            ]
            .max()
            .values[0]
        )

        if type(table_to_query) != str and math.isnan(table_to_query):
            raise ValueError(
                f"Could not find a table matching the pattern '{base_table_name}'"
            )

        log.info(
            f'Found "{table_to_query}" as the newest table matching "{base_table_name}"'
        )
        return table_to_query


def query_into_df(
    schema: str,
    table: str,
    conn: Connection,
    clause: str = None,
    limit: int = False,
    random: bool = False,
) -> DF:
    """
    Basic function to query BEDAP and return all columns.

    You can specify a LIMIT and if you want pseudo-random results.
    You only enter the schema and basename of the table (e.g. "beneficiaries" for "beneficiaries_YYYYMMDD")
    and it will query the most up-to-date table.

    Parameters
    ----------
    schema : str
        a database schema

    table: str
        the name of a table to query in the above schema

    conn : Connection
        a database connection

    clause : str, default NONE
        SQL WHERE clause
    limit : int
        SQL LIMIT
    random : bool {True, False}, default False
        if you want a pseudo-random result

    Returns
    -------
    DataFrame
        DataFrame of query results
    """

    # Query table and return df
    df_query_results = results_to_df(
        conn,
        generic_sql_query(schema, table, clause, limit=limit, random=random),
    )

    num_results = len(df_query_results.index)

    if df_query_results.empty:
        log.info("The query did not return any results")
    else:
        log.info(f"Query returned {num_results} results")
        if num_results > 10:
            log.info(f"Preview of first 10 rows:\n{df_query_results.head(10)}")
        else:
            log.info(f"Results:\n{df_query_results}")

    return df_query_results
