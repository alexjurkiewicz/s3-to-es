import boto3
from moto import mock_s3

import common

BUCKET = "mybucket"
KEY = "mykey"


@mock_s3
def test_s3_object_lines():
    conn = boto3.client("s3")
    conn.create_bucket(Bucket=BUCKET)
    conn.put_object(Bucket=BUCKET, Key=KEY, Body=b"a\nb\nc\n")

    assert list(common._s3_object_lines(BUCKET, KEY)) == ["a", "b", "c"]


def test_hash_es_doc():
    assert (
        common._hash_es_doc({"a": 1, "b": 2})
        == "XKu6a8Nd4Crs18uTxgt4KFw8QzjkZWJWg3hra2Tl7+I="
    )


def test_transform_lines():
    lines = ["a", "b", "c"]
    transform_fn = lambda i, _n: [{"_id": i}]
    assert [i for i in common._transform_lines(lines, transform_fn)] == [
        {"_id": i} for i in lines
    ]


def test_buffering_iterator():
    buffer = {}
    assert list(common.buffering_iterator([{},{}], buffer)) == [{},{}]
