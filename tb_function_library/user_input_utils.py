from __future__ import annotations
import os
from multiprocessing.connection import Connection
from pyclbr import Function
from pandas import DataFrame as DF
from .connection_utils import LastpassManager
from .database_utils import check_if_schema_exists
from .file_utils import check_if_file_exists


# Initiate logging
from .log_config import get_logger

log = get_logger(__name__)


def yes_true_else_false(message: str) -> bool:
    """Function to be used in main for default behavior of user input (Y(es) = True, else False)

    Parameters
    ----------
    message : str
        A question to display to the user for which they will choose True or False

    Returns
    -------
    bool

    """
    user_input = input(f"{message} (Y or Yes for 'Yes'): ")

    if user_input.lower() in ("y", "yes", True, "true"):
        user_input = True
    else:
        user_input = False

    return user_input


def enter_for_default(message: str, default: str) -> str:
    """Function to be used in main for default behavior of user input (Enter = default value)

    Parameters
    ----------
    message : str
        A question to display to the user for which they will choose a response.

    defualt : str
        A defualt option for the user to choose if they hit Enter

    Returns
    -------
    str
        The option the user chose based on the prompt

    """
    user_input = input(f"{message} (Enter for `{default}`): ")

    if user_input == "":
        user_input = default

    return user_input


def ensure_positive_int(message: str, default: int, zero_behavior) -> int:
    """Function used to ensure user enters a positive integer.

    Parameters
    ----------
    message : str
        A question to display to the user for which they will choose a response

    default : int
        A defualt integer for the user to choose if they hit Enter

    zero_behavior
        some alternative response if the user enters 0 as their response

    Returns
    -------
    int
        Returns a positive integer selected by the user

    """
    while True:
        try:
            input = enter_for_default(message, default)

            input = int(input)
            if input < 0:
                raise AssertionError
            elif input == 0:
                input = zero_behavior
            break
        except KeyboardInterrupt:
            break
        except:
            print("That's not a positive integer. Please try again.")

    return input


def ensure_lastpass_entry_exists(lastpass_entry: str) -> LastpassManager:
    """A function that ENSURES a LastPass entry exists in the user's vault

    If the entered LastPass name doesn't exists, it will continually loop and ask again until a LastPass entry selected exists

    Parameters
    ----------
    lastpass_entry : str
        a LastPass entry name

    Returns
    -------
    LastPass Manager
        Returns a LastpassManager object if the entry exists
    """
    while True:
        try:
            database = enter_for_default(
                "What is the name of the LastPass entry which contains the database credentials?",
                lastpass_entry,
            )
            lpass = LastpassManager(database)

            return lpass
        except KeyboardInterrupt:
            break
        except:
            pass


def ensure_schema_exists(schema: str, conn: Connection):
    """A function that ENSURES a schema exists in a database.

    If the entered schema doesn't exists, it will continually loop and ask again until a schema the exists is selected

    Parameters
    ----------
    schema : str
        a schema name
    conn : Connection
        a database connection

    Returns
    -------
    str, DataFrame
        returns the schema and a DataFrame of all the tables that exist in the schema.
    """
    while True:
        schema = enter_for_default("What is the schema?", schema)

        df_tables_in_schema = check_if_schema_exists(schema, conn)
        try:
            if not df_tables_in_schema.empty:
                return schema, df_tables_in_schema
        except:
            pass


def ensure_not_blank(function: Function):
    """A function that ENSURES an entry from the user is not blank.

    If the user entry is empty, it will continually loop and ask again until a entry is not empty

    Parameters
    ----------
    schema : str
        a schema name
    conn : Connection
        a database connection

    Returns
    -------
    str, DataFrame
        returns the schema and a DataFrame of all the tables that exist in the schema.
    """
    while True:
        response = function
        if response != "":
            return response


def ensure_file_exists(file_name: str, directory_name: str, default_directory: str):
    """A function that ENSURES a file exists

    If the entered filename doesn't exists, it will continually loop and ask again until a file that exists is entered

    Parameters
    ----------
    filename : str
        a schema name
    directory_name : str
        a database connection
    default_directory : str
        default directory to display to the user

    Returns
    -------
    filename, directory, filepath
        returns the filename, directory, and entire filepath to the file in question.
    """

    while True:
        directory = enter_for_default(directory_name, default_directory)

        # Print all files in directory
        dir_list = os.listdir(directory)
        if len(dir_list) != 0:
            print(f"Here are the files in {directory}:\n")
            for file in sorted(dir_list):
                if file[0] != ".":
                    print(file)
            print("\n")

        filename = input(f"{file_name}: ")
        filepath = check_if_file_exists(directory, filename)

        if filepath:
            return filename, directory, filepath
