import os
from opensearchpy import OpenSearch


def get_aws_auth(region="us-east-1", service="aoss"):
    """Get AWS authentication for OpenSearch"""
    import boto3
    from requests_aws4auth import AWS4Auth

    credentials = boto3.Session().get_credentials()
    aws_auth = AWS4Auth(
        credentials.access_key,
        credentials.secret_key,
        region,
        service,
        session_token=credentials.token,
    )
    return aws_auth


def get_os_client(use_aws_auth=False, region="us-east-1", timeout=1000):
    """
    Initialize OpenSearch client

    Args:
        use_aws_auth (bool): Whether to use AWS authentication
        region (str): AWS region for authentication
        timeout (int): Client timeout in seconds

    Returns:
        OpenSearch client instance
    """
    if use_aws_auth:
        from opensearchpy import RequestsHttpConnection

        hosts = os.environ.get("HOSTS", "localhost:9200")
        client = OpenSearch(
            hosts=hosts,
            http_auth=get_aws_auth(region=region),
            use_ssl=True,
            verify_certs=True,
            connection_class=RequestsHttpConnection,
            timeout=timeout,
        )
    else:
        client = OpenSearch(
            hosts=os.environ.get("HOSTS", "localhost:9200"), timeout=timeout
        )

    return client
