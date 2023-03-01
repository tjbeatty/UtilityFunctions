import subprocess


def bash_cmd(cmd: str) -> str:
    """A function that will run a command line command in Python and store the output to be used later


    Parameters
    ----------
    cmd : str
        The command one would run in the command line

    Returns
    -------
    str
        The output from the input in command, captured for future use"""

    result = subprocess.run(cmd, shell=True, check=True, text=True, capture_output=True)
    return result.stdout.strip()
