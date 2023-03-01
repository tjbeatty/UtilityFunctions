class AwsClusterSecretsMap:
    """Class for mapping an AWS cluster nickname to the official cluster name and password Secrets Manager secret name

    Attributes
    ----------
    cluster_identifier : str
        a string containing the cluster name, as denoted on AWS RDS or Redshift
    password_secret_name : str
        a string containing the corresponding secret name for the password for cluster

    """

    def __init__(
        self,
        cluster_identifier: str,
        password_secret_name: str,
    ):
        self.cluster_identifer = cluster_identifier
        self.password_secret_name = password_secret_name


# Define new cluster setups with details below
MSP_STAGING = AwsClusterSecretsMap("msp-staging-cluster", "msp/staging-db-password")
MSP_FINAL = AwsClusterSecretsMap("msp-final-cluster", "msp/final-db-password")
MEDICARE_EVENTS = AwsClusterSecretsMap(
    "medicare-events-cluster", "medicare-events/redshift-password"
)

# Add new clusters to the list dictionary below to enable them across BDAMAX
CONFIGURED_CLUSTER_LIST = {
    "MSP_STAGING": MSP_STAGING,
    "MSP_FINAL": MSP_FINAL,
    "MEDICARE_EVENTS": MEDICARE_EVENTS,
}
