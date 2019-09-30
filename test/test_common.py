from typing import Dict, Callable, Iterable, Union
import gzip

import boto3  # type: ignore
from moto import mock_s3  # type: ignore
import pytest  # type: ignore

import common

BUCKET = "mybucket"
KEY_RAW = "mykey"
KEY_GZIP = "mykey.gz"
BODY_RAW = b"a\nb\nc\n"
BODY_GZIP = gzip.compress(BODY_RAW)


@mock_s3  # type: ignore
def test_s3_object_lines_raw() -> None:
    conn = boto3.client("s3")
    conn.create_bucket(Bucket=BUCKET)
    conn.put_object(Bucket=BUCKET, Key=KEY_RAW, Body=BODY_RAW)

    s3_object_lines = common._s3_object_lines  # pylint: disable=protected-access
    assert list(s3_object_lines(BUCKET, KEY_RAW)) == [
        "a",
        "b",
        "c",
    ]  # pylint: disable=protected-access


@mock_s3  # type: ignore
def test_s3_object_lines_compressed() -> None:
    conn = boto3.client("s3")
    conn.create_bucket(Bucket=BUCKET)
    conn.put_object(Bucket=BUCKET, Key=KEY_GZIP, Body=BODY_GZIP)

    s3_object_lines = common._s3_object_lines  # pylint: disable=protected-access
    assert list(s3_object_lines(BUCKET, KEY_GZIP)) == [
        "a",
        "b",
        "c",
    ]  # pylint: disable=protected-access


def test_hash_es_doc() -> None:
    assert (
        common._hash_es_doc({"a": 1, "b": 2})
        == "XKu6a8Nd4Crs18uTxgt4KFw8QzjkZWJWg3hra2Tl7+I="
    )


def test_transform_lines() -> None:
    lines = ["a", "b", "c"]
    transform_fn = lambda i, _n: [{"_id": i}]
    transform_lines = common._transform_lines  # pylint: disable=protected-access
    assert list(transform_lines(lines, transform_fn)) == [{"_id": i} for i in lines]


def test_transform_lines_failure() -> None:
    lines = ["a", "b", "c"]
    transform_fn = lambda i, _n: i / _n
    transform_lines = common._transform_lines  # pylint: disable=protected-access
    with pytest.raises(TypeError):
        assert list(transform_lines(lines, transform_fn)) == [{"_id": i} for i in lines]


def test_transform_lines_id_gen() -> None:
    lines = ["a", "b", "c"]
    transform_fn: Callable[
        [str, int], Iterable[Dict[str, Union[str, bool, float]]]
    ] = lambda i, _n: [{}]
    transform_lines = common._transform_lines  # pylint: disable=protected-access
    for item in list(transform_lines(lines, transform_fn)):
        assert "_id" in item


def test_es_streaming_wrapper() -> None:
    iterator = (i for i in range(4))
    assert list(common._es_streaming_wrapper(iterator)) == [0, 1, 2, 3]


def test_buffering_iterator() -> None:
    buffer: Dict[str, str] = {}
    assert list(common.buffering_iterator([{}, {"_id": "abc"}], buffer)) == [
        {},
        {"_id": "abc"},
    ]
