import os
import logging
from typing import Any

from aws_requests_auth.aws_auth import AWSRequestsAuth  # type: ignore
from elasticsearch import Elasticsearch, RequestsHttpConnection  # type: ignore

import common
import cloudfront
import alb
import cloudtrail

# Global setup
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Sign the request
es_host = os.environ["ES_HOSTNAME"]
region = es_host.split(".")[1]

log_type = os.environ["LOG_TYPE"]
transform_fn: common.TransformFn
if log_type == "cloudfront":
    check_filename_fn = cloudfront.check_filename
    transform_fn = cloudfront.transform
elif log_type == "alb":
    check_filename_fn = alb.check_filename
    transform_fn = alb.transform
elif log_type == "cloudtrail":
    check_filename_fn = cloudtrail.check_filename
    transform_fn = cloudtrail.transform
else:
    raise ValueError("Unhandled LOG_TYPE '%s'" % log_type)


def handler(event: Any, _context: Any) -> None:
    # As per https://github.com/DavidMuller/aws-requests-auth#elasticsearch-py-client-usage-example
    auth = AWSRequestsAuth(
        aws_access_key=os.environ["AWS_ACCESS_KEY_ID"],
        aws_secret_access_key=os.environ["AWS_SECRET_ACCESS_KEY"],
        aws_token=os.environ["AWS_SESSION_TOKEN"],
        aws_host=es_host,
        aws_region=region,
        aws_service="es",
    )
    es_client = Elasticsearch(
        host=es_host,
        port=443,
        use_ssl=True,
        connection_class=RequestsHttpConnection,
        http_auth=auth,
    )
    for record in event["Records"]:
        bucket = record["s3"]["bucket"]["name"]
        key = record["s3"]["object"]["key"]
        if check_filename_fn(key):
            common.s3_to_es(
                bucket=bucket, key=key, transform_fn=transform_fn, es_client=es_client
            )
        else:
            logger.warning("Skipping object %r", key)
