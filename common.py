import gzip
import traceback
from typing import Iterable, Callable, Dict, Iterator, Union, TypeVar
import logging

import boto3  # type: ignore
import elasticsearch  # type: ignore
import elasticsearch.helpers  # type: ignore
import urllib3.exceptions  # type: ignore
from aws_xray_sdk.core import xray_recorder  # type: ignore

_ES_STREAM_BULK_OPTS = {
    "max_chunk_bytes": 90 * 1024 * 1024,  # 90mbyte
    "chunk_size": 1_000,
    "max_retries": 3,
    "initial_backoff": 1,
    "max_backoff": 10,
    "raise_on_error": False,  # Don't raise exception if we fail to load data, just return error response
    "raise_on_exception": False,  # Don't re-raise exceptions if the call to es.bulk fails, just return error response
}

EsDocument = Dict[str, Union[str, int, bool, float]]
TransformFn = Callable[[str, int], Iterable[EsDocument]]
T = TypeVar("T")

logger = logging.getLogger()


@xray_recorder.capture("_s3_object_lines")  # type: ignore
def _s3_object_lines(bucket: str, key: str) -> Iterable[str]:
    """Return lines from an S3 object in a streaming manner."""
    obj = boto3.resource("s3").Object(bucket_name=bucket, key=key)
    response = obj.get()
    logger.debug("Streaming %s bytes from S3", response["ContentLength"])
    if key.endswith(".gz"):
        stream = gzip.GzipFile(fileobj=response["Body"])
    else:
        stream = response["Body"]
    for line in stream:
        yield line.decode()
    logger.debug("Finished streaming from S3")


@xray_recorder.capture("_transform_lines")  # type: ignore
def _transform_lines(
    lines: Iterable[str], transform_fn: TransformFn
) -> Iterable[EsDocument]:
    n = 0
    for line in lines:
        try:
            documents = transform_fn(line, n)
        except Exception:
            logger.exception("Failed to transform line %s (%r)", n, line)
            raise
        n += 1
        for doc in documents:
            yield doc


def _es_streaming_wrapper(streamer: Iterator[T]) -> Iterator[T]:
    """Swallow some errors from elasticsearch.helpers.streaming_bulk.

    Sometimes this returns errors like:
    ReadTimeoutError: Read timeout on endpoint URL: "None"

    We don't want to stop the whole process if this happens, we should try to
    continue so only the minimum amount of data is lost."""
    while True:
        try:
            yield next(streamer)
        except StopIteration:
            # Generators don't raise StopException, they just return
            # https://stackoverflow.com/a/51701040
            return
        except urllib3.exceptions.ReadTimeoutError as e:
            logger.warning(
                "Error from Elasticsearch: %s. Will continue processing. Traceback follows:",
                e,
            )
            traceback.print_exc()


@xray_recorder.capture("_stream_to_es")  # type: ignore
def _stream_to_es(
    es: elasticsearch.Elasticsearch,
    documents: Iterable[EsDocument],
    log_interval: int = 10_000,
) -> None:
    elastic_stream = _es_streaming_wrapper(
        elasticsearch.helpers.streaming_bulk(es, documents, **_ES_STREAM_BULK_OPTS)
    )
    count = 0
    for resp in elastic_stream:
        count += 1
        if count % log_interval == 0:
            logger.debug("Sent %s documents to Elasticsearch", count)
        if resp[0]:
            count += 1
        else:
            logger.warning("Error from Elasticsearch, continuing: %r", resp)
    logger.info("Sent %s total documents to Elasticsearch", count)


def s3_to_es(
    bucket: str,
    key: str,
    transform_fn: TransformFn,
    es_client: elasticsearch.Elasticsearch,
) -> None:
    """
    Index lines in an S3 file into Elasticsearch.

    Args:
        bucket: S3 bucket
        key: S3 key
        transform_fn: Function that converts a line from the S3 file into an ES
            document. The ES document must include `_index` and (if ES<7)
            `_type` fields. If a line should not be indexed, return None.
            `line_no` is 0-indexed.
            Function signature: (line: str, line_no: int) -> Iterable[EsDocument]
        elasticsearch: The ES connection object.

    Returns:
        None if successful. Raises exception if something critical went wrong.
        Errors on submitting some data to ES are printed but otherwise ignored.
    """
    docs = _transform_lines(_s3_object_lines(bucket, key), transform_fn)

    _stream_to_es(es_client, docs)
