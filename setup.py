from setuptools import setup, find_packages
import pathlib

here = pathlib.Path(__file__).parent.resolve()

VERSION = "0.0.1"
SHORT_DESCRIPTION = "Commonly used functions by Timmy Beatty"
LONG_DESCRIPTION = (here / "README.md").read_text(encoding="utf-8")


setup(
    name="tb_function_library",
    version=VERSION,
    author="Timmy Beatty",
    author_email="tim.beatty@gmail.com",
    description=SHORT_DESCRIPTION,
    packages=find_packages(),
    python_requires=">=3.7, <4",
    license="LICENSE.txt",
    install_requires=[
        "pandas",
        "aws-psycopg2",
        "python-dotenv",
        "pre-commit",
        "boto3",
        "black",
        "sqlalchemy",
    ],
)
