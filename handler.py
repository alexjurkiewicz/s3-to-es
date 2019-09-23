import os
import logging
from typing import Any

from aws_requests_auth.aws_auth import AWSRequestsAuth  # type: ignore
from elasticsearch import Elasticsearch, RequestsHttpConnection  # type: ignore
from aws_xray_sdk.core import xray_recorder  # type: ignore
from aws_xray_sdk.core import patch_all

import common
import cloudfront
import alb

# Global setup
logger = logging.getLogger()
logger.setLevel(logging.INFO)
xray_recorder.configure(sampling=False)
patch_all()
logging.getLogger("aws_xray_sdk").setLevel(logging.INFO)

# Sign the request
es_host = os.environ["ES_HOSTNAME"]
region = es_host.split(".")[1]
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

log_type = os.environ["LOG_TYPE"]
transform_fn: common.TransformFn
if log_type == "cloudfront":
    transform_fn = cloudfront.transform
elif log_type == "alb":
    transform_fn = alb.transform
else:
    raise ValueError("Unhandled LOG_TYPE '%s'" % log_type)


def handler(event: Any, _context: Any) -> None:
    xray_recorder.begin_subsegment("Handler")
    for record in event["Records"]:
        xray_recorder.begin_subsegment("Record")
        common.s3_to_es(
            bucket=record["s3"]["bucket"]["name"],
            key=record["s3"]["object"]["key"],
            transform_fn=transform_fn,
            es_client=es_client,
        )
        xray_recorder.end_subsegment()
    xray_recorder.end_subsegment()
