from psycopg2 import sql
from psycopg2.sql import SQL

# TODO: Implement Query Class!!


def tables_in_schema_query(schema: str) -> SQL:
    """Query to identify tables in the schema provided

    Parameters
    ----------
    schema : str
        a schema

    Returns
    -------
    SQL
        Returns formatted Psycopg2 SQL object
    """

    query_template = """
        SELECT table_name,'TABLE' as table_or_view
        FROM information_schema.tables
        WHERE table_schema = {schema} AND table_type='BASE TABLE'
        UNION
        SELECT table_name,'VIEW' as table_or_view
        FROM information_schema.views
        WHERE table_schema = {schema}
        """

    params = {"schema": sql.Literal(schema)}

    query = params_in_query_template(query_template, params)
    return query


def columns_dtypes_of_table_query(schema: str, table: str) -> SQL:
    """Query to get a list of columns and data types from the table specified

    Parameters
    ----------
    schema : str
        a schema

    table : str
        a table name

    Returns
    -------
    SQL
        Returns formatted Psycopg2 SQL object"""

    query_template = """
        SELECT c.column_name, c.udt_name as dtype
        FROM information_schema.columns c
        WHERE c.table_schema = {schema} AND c.table_name = {table}
    """
    params = {
        "schema": sql.Literal(schema),
        "table": sql.Literal(table),
    }

    query = params_in_query_template(query_template, params)
    return query


def row_count_query(schema: str, table: str) -> SQL:
    """Query to get the row count of a tbale

    Parameters
    ----------
    schema : str
        a schema

    table : str
        a table name

    Returns
    -------
    SQL
        Returns formatted Psycopg2 SQL object"""

    query_template = """
    SELECT count(*)
    FROM {schema}.{table}
    """
    params = {
        "schema": sql.Identifier(schema),
        "table": sql.Identifier(table),
    }

    query = params_in_query_template(query_template, params)
    return query


def generic_sql_query(
    schema: str,
    table: str,
    clause: str = None,
    limit: int = False,
    random: bool = False,
) -> SQL:
    """Generic SELECT query, with optional limit and pseudo-random flag

    Parameters
    ----------
    schema : str
        a schema

    table : str
        a table name

    clause : str (optional) default = None
        WHERE clause of a query

    limit : int (optional), default = None
        LIMIT of a query

    random : bool (optional), default = False
        whether you want pseudo-random results or not

    Returns
    -------
    SQL
        Returns formatted Psycopg2 SQL object
    """
    query_template = """
        SELECT *
        FROM {schema}.{table}
        """
    params = {
        "schema": sql.Identifier(schema),
        "table": sql.Identifier(table),
    }

    if clause:
        query_template = query_template + "\n" + clause
        if random:
            query_template = query_template + " AND RANDOM() < 0.1"
    else:
        if random:
            query_template = query_template + "\nWHERE RANDOM() < 0.1"

    if limit:
        query_template = query_template + "\nLIMIT {limit}"
        params["limit"] = sql.Literal(limit)

    query = params_in_query_template(query_template, params)
    return query


def params_in_query_template(query_template: str, params: dict) -> SQL:
    """A function to insert parameters into query template

    Parameters
    ----------
    query_template : str
        a query template with placeholder values

    params : dict
        a dictionary of parameters to be inserted into the query template

    Returns
    -------
    SQL
        Returns formatted Psycopg2 SQL object
    """
    return SQL(query_template).format(**params)
