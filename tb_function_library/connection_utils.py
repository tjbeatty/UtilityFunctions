import os
import sys
import subprocess

dir_path = os.path.dirname(os.path.realpath(__file__))
sys.path.append(dir_path)

"""Utilities for common database access tasks.
"""
import subprocess
import psycopg2
import sqlalchemy
import os
import boto3
from botocore.exceptions import ClientError
from dotenv import load_dotenv
from dataclasses import dataclass
from sqlalchemy.engine import Connection
from command_line_utils import bash_cmd
from aws_db_details import CONFIGURED_CLUSTER_LIST

# Initiate logging
from log_config import get_logger

log = get_logger(__name__)

# Load environmental file
load_dotenv()
LASTPASS_USERNAME = os.environ.get("LASTPASS_USERNAME")


class AwsDbConnectionDetails:
    """Class for storing the relevant database/cluster information for connecting to the service

    Attributes
    ----------
    host : str
        database host address
    password : str
        database password
    db : str
        database name
    user: str
        database username
    cluster_type : str
        database type (Redshift, Postgresql, etc.)
    port : int
        databse port number
    """

    def __init__(
        self,
        host,
        password,
        db,
        user,
        cluster_type,
        port,
    ):
        self.host = host
        self.password = password
        self.db = db
        self.user = user
        self.type = cluster_type
        self.port = port


@dataclass
class LastpassManager:
    """Class for creating a database engine with a Lastpass entry."""

    lpass_entry: str
    driver: str = "psycopg2"
    dialect: str = "postgresql"

    def __post_init__(self) -> None:
        self._authenticate()

        try:
            self.db_type = self._lpass_cred("--field Type")
            self.database = self._lpass_cred("--field Database")
            self.user = self._lpass_cred("--username")
            self.password = self._lpass_cred("--password")
            self.host = self._lpass_cred("--field Hostname")
            self.port = self._lpass_cred("--field Port")

            if self.driver is not None:
                self.engine_str = f"{self.dialect}+{self.driver}://{self.user}:{self.password}@{self.host}:{self.port}/{self.database}"
            else:
                self.engine_str = f"{self.dialect}://{self.user}:{self.password}@{self.host}:{self.port}/{self.database}"
        except:
            log.warning(f"'{self.lpass_entry}' Last Pass entry does not exist.")
            raise ValueError

    def _authenticate(self):
        try:
            bash_cmd("lpass status")
        except subprocess.CalledProcessError:
            lpass_login = LASTPASS_USERNAME
            bash_cmd(f"lpass login {lpass_login}")

    def _lpass_cred(self, args: str) -> str:
        """Fetch a single credential from the lastpass CLI.

        Args:
            entry (str): Lastpass entry to request.
            args (str): Flags to pass to the CLI.

        Returns:
            str: Requested Lastpass credential.
        """
        result = bash_cmd(f"lpass show '{self.lpass_entry}' {args}")
        return result

    def create_psycopg2_connection(
        self,
    ) -> Connection:
        """Build a database connection with credentials from Lastpass.

        Args:
            cred_key (str): Lastpass entry to request.
            dbname (str): name of the database connection to use.

        Returns:
            Database connection.

        """

        return psycopg2.connect(
            host=self.host,
            database=self.database,
            port=int(self.port),
            user=self.user.lower(),
            password=self.password,
        )

    def create_sqlalchemy_connection(
        self,
    ) -> Connection:
        """Build a database connection with credentials from Lastpass.

        Args:
            cred_key (str): Lastpass entry to request.
            dbname (str): name of the database connection to use.

        Returns:
            Database connection.

        """

        return sqlalchemy.create_engine(self.engine_str).connect()


@dataclass
class AwsDatabaseConnectionManager:
    f"""Class for creating a database engine with an AWS database.

        Attributes
    ----------
    name : str
        the connection short name Valid options: {[option.lower() for option in CONFIGURED_CLUSTER_LIST.keys()]}
    """

    name: str
    driver: str = "psycopg2"

    def __post_init__(self) -> None:
        connection_options_lower = [
            option.lower() for option in CONFIGURED_CLUSTER_LIST.keys()
        ]
        connection_name = self.name.lower()

        if connection_name not in connection_options_lower:
            raise ValueError(
                f"Invalid connection name. Valid connections: {CONFIGURED_CLUSTER_LIST}"
            )

        try:
            cluster_secrets_map = CONFIGURED_CLUSTER_LIST[self.name.upper()]
            cluster_identifier = cluster_secrets_map.cluster_identifer
            password_secret_name = cluster_secrets_map.password_secret_name

            connection_details = get_cluster_connection_details(
                cluster_identifier, password_secret_name
            )
            self.host = connection_details.host
            self.password = connection_details.password
            self.port = connection_details.port
            self.database = connection_details.db
            self.user = connection_details.user
            self.dialect = connection_details.type

            if self.driver is not None:
                self.engine_str = f"{self.dialect}+{self.driver}://{self.user}:{self.password}@{self.host}:{self.port}/{self.database}"
            else:
                self.engine_str = f"{self.dialect}://{self.user}:{self.password}@{self.host}:{self.port}/{self.database}"

        except:
            log.warning(
                f"Invalid connection name. Valid connections: {CONFIGURED_CLUSTER_LIST}"
            )
            raise ValueError

    def create_psycopg2_connection(
        self,
    ) -> Connection:
        """Build a database connection with credentials pulled from Cluster response.

        Returns:
            Psycopg2 Database connection.

        """

        return psycopg2.connect(
            host=self.host,
            database=self.database,
            port=int(self.port),
            user=self.user.lower(),
            password=self.password,
        )

    def create_sqlalchemy_connection(
        self,
    ) -> Connection:
        """Build a database connection with credentials from Cluster response.

        Returns:
            SQLAlchemy Database connection.

        """

        return sqlalchemy.create_engine(self.engine_str).connect()


def connect_to_aws_service(service="s3"):
    """Connects to AWS service using environment variables injected from Kion/Cloudtamer.

    Parameters
    -----------
    service : str
        AWS service with which one would like to connect

    Returns
    -----------
    boto3.resource
        AWS Resource Connection

    """

    log.info(f"Connecting to {service}...")

    # Use the credentials from environment variables to make a connection to Amazon S3
    s3_resource = boto3.resource(
        service,
    )

    log.info(f"Connected to {service}...")
    return s3_resource


def get_secret_from_secrets_manager(
    secret_name: str, region_name: str = "us-east-1"
) -> str:
    """Retrieves a secret from AWS Secrets Manager.

    Parameters
    -----------
    secret_name : str
        Secret Name, as defined in AWS Secrets Manager
    region_name : str
        AWS database region (defaults to us-east-1)

    Returns
    -----------
    str
        Secret String, returned from AWS Secrets Manager
    """

    # Create a Secrets Manager client
    session = boto3.session.Session()
    client = session.client(service_name="secretsmanager", region_name=region_name)

    try:
        get_secret_value_response = client.get_secret_value(SecretId=secret_name)
    except ClientError as e:
        raise e

    # Decrypts secret using the associated KMS key.
    secret = get_secret_value_response["SecretString"]

    return secret


def get_cluster_connection_details(
    cluster_name: str,
    password_secret_name: str,
    read_only: bool = False,
) -> AwsDbConnectionDetails:
    f"""Retrieves AWS cluster connection details to be used to connect to a database.

    Parameters
    -----------
    cluster_identifier: str
        Cluster Identifier, as defined in aws_db_credentials.py (Valid Options: {CONFIGURED_CLUSTER_LIST})
    password_secret_name : str
        AWS Secrets Manager name for the stored secret password.
    read_only : bool
        True or False. If True and the DB is in RDS, a ReaderEndpoint will be opened.

    Returns
    -----------
        AwsDbConnectionDetails(host_address, password, db_name, user, type, port)

    """
    try:
        client = boto3.client("rds", region_name="us-east-1")
        response = client.describe_db_clusters(DBClusterIdentifier=cluster_name)
        if read_only:
            host_address = response["DBClusters"][0]["ReaderEndpoint"]
        else:
            host_address = response["DBClusters"][0]["Endpoint"]

        user = response["DBClusters"][0]["MasterUsername"]
        db_name = response["DBClusters"][0]["DatabaseName"]
        port = response["DBClusters"][0]["Port"]
        engine = response["DBClusters"][0]["Engine"]
        if "postgresql" in engine:
            type = "postgresql"
        else:
            raise TypeError(
                f"Engine Type Unexpected. Expected postgresql in RDS, but got {engine}"
            )
    except:
        try:
            client = boto3.client("redshift", region_name="us-east-1")
            response = client.describe_clusters(ClusterIdentifier=cluster_name)
            host_address = response["Clusters"][0]["Endpoint"]["Address"]
            user = response["Clusters"][0]["MasterUsername"]
            db_name = response["Clusters"][0]["DBName"]
            port = response["Clusters"][0]["Endpoint"]["Port"]
            type = "redshift"
        except ClientError as err:
            if err.response["Error"]["Code"] == "DBInstanceNotFound":
                log.info(f"Instance {cluster_name} does not exist.")
            else:
                log.error(
                    "Couldn't get DB instance %s. Here's why: %s: %s",
                    cluster_name,
                    err.response["Error"]["Code"],
                    err.response["Error"]["Message"],
                )
                raise

    password = get_secret_from_secrets_manager(password_secret_name)
    return AwsDbConnectionDetails(host_address, password, db_name, user, type, port)
