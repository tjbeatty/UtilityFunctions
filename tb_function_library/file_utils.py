from __future__ import annotations
import os
import sys
import pandas as pd

dir_path = os.path.dirname(os.path.realpath(__file__))
sys.path.append(dir_path)

import os
from pandas import DataFrame as DF

# Initiate logging
from log_config import get_logger

log = get_logger(__name__)


def results_to_csv(df: DF, csv_name: str, results_folder: str = "./results/") -> None:
    """Store a DataFrameto a CSV in a defined folder.

    Parameters
    ----------
    df : DataFrame
        a DataFrame of query results

    csv_name : str
        The name you would like the results CSV to be called

    results_folder : str, default: './results/'
        The folder in which you would like to store the CSV

    Returns
    -------
        None
    """
    make_dir_if_not_exists(results_folder)

    file = results_folder + csv_name
    df.to_csv(file, index=False)


def check_if_file_exists(directory: str, filename: str):
    """Check if path and file exist

    Parameters
    ----------
    directory : str
        the directory where you would expect a file to exist

    filename: str
        the name of the file you would like to determine if it exists

    Returns
    -------
    str
        returns the filepath to the file in question, or False if the file does not exist
    """

    directory = ensure_file_slash(directory)

    if filename_is_blank(filename):
        return False

    file_path = directory + filename

    if os.path.exists(directory):
        if os.path.exists(file_path):
            return file_path
        else:
            log.warning(f"`\n{filename}` does not exist in {directory}.\n")
            return False
    else:
        log.warning(f"\n{directory} does not exist.\n")
        return False


def ensure_file_slash(directory: str):
    """A function to ensure a filepath ends in a '/'

    Parameters
    ----------
    directory : str
        a directory in your filesystem

    Returns
    -------
    str
        returns the filepath with a slash"""

    if directory != "" and directory[-1] != "/":
        directory = directory + "/"

    return directory


def filename_is_blank(filename: str):
    """Checks if a filename is blanks

    Parameters
    ----------
    filename : str
        a failname to check

    Returns
    -------
    bool
        True, if filename is empty; False, if filename is not empty
    """

    if filename == "":
        log.error("You entered a blank filename\n")
        return True
    else:
        return False


def make_dir_if_not_exists(folder: str) -> None:
    """A function that creates a directory, if it does not already exist in the filesystem. If the folder already exists, do nothing.

    Parameters
    ----------
    folder : str
        a directory to create

    Returns
    -------
    None
    """
    if not os.path.exists(folder):
        # If folder doesn't exist, create it.
        os.makedirs(folder)
