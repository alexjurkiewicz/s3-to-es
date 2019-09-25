import gzip
import traceback
from typing import Iterable, Callable, Dict, Iterator, Union, TypeVar
import logging
import hashlib
import json
import base64

import pylru  # type: ignore
import boto3  # type: ignore
import elasticsearch  # type: ignore
import elasticsearch.helpers  # type: ignore
import urllib3.exceptions  # type: ignore

_ES_STREAM_BULK_OPTS = {
    "max_chunk_bytes": 90 * 1024 * 1024,  # 90mbyte
    "chunk_size": 10_000,
    "max_retries": 3,
    "initial_backoff": 1,
    "max_backoff": 10,
    "raise_on_error": False,  # Don't raise exception if we fail to load data, just return error response
    "raise_on_exception": False,  # Don't re-raise exceptions if the call to es.bulk fails, just return error response
}

EsDocument = Dict[str, Union[str, int, bool, float]]
TransformFn = Callable[[str, int], Iterable[EsDocument]]
T = TypeVar("T")  # generic type

logger = logging.getLogger()


def _s3_object_lines(bucket: str, key: str) -> Iterable[str]:
    """Return lines from an S3 object in a streaming manner."""
    obj = boto3.resource("s3").Object(bucket_name=bucket, key=key)
    response = obj.get()
    logger.debug("Streaming %s bytes from S3", response["ContentLength"])
    if key.endswith(".gz"):
        stream = gzip.GzipFile(fileobj=response["Body"])
    else:
        stream = response["Body"]
    for line in stream.iter_lines():
        yield line.decode()
    logger.debug("Finished streaming from S3")


def _hash_es_doc(doc: EsDocument) -> str:
    """
    Generate an _id value for an EsDocument.

    Use a stable representation of the doc so we don't have consistency issues
    when processing the same document over multiple Lambda invoations.
    """
    # We limit the digest size to make sure the ID fits in 512 bytes
    return base64.b64encode(
        hashlib.blake2b(
            json.dumps(doc, sort_keys=True).encode(), digest_size=32
        ).digest()
    ).decode()


def _transform_lines(
    lines: Iterable[str], transform_fn: TransformFn
) -> Iterable[EsDocument]:
    """Transform log file lines into Elasticsearch Documents, one at a time."""
    for n, line in enumerate(lines):
        try:
            documents = transform_fn(line, n)
        except Exception:
            logger.exception("Failed to transform line %s (%r)", n, line)
            raise
        for doc in documents:
            if "_id" not in doc:
                doc["_id"] = _hash_es_doc(doc)
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


def buffering_iterator(
    iterable: Iterable[EsDocument], buffer: pylru.lrucache
) -> Iterable[EsDocument]:
    for item in iterable:
        _id = item.get("_id")
        if _id:
            buffer[_id] = item
        yield item


def _stream_to_es(
    es: elasticsearch.Elasticsearch, documents: Iterable[EsDocument]
) -> None:
    # We buffer items as they are sent through to ES so that we can show them
    # in case ES returns an error. This requires the _id to be pre-set.
    buffer = pylru.lrucache(_ES_STREAM_BULK_OPTS["chunk_size"] * 2)

    documents_bufferer = buffering_iterator(documents, buffer)

    elastic_stream = _es_streaming_wrapper(
        elasticsearch.helpers.streaming_bulk(
            es, documents_bufferer, **_ES_STREAM_BULK_OPTS
        )
    )
    # Each document causes an iteration of this loop, even though docs are sent
    # in batches.
    count = 0  # if enumerate() gets zero items, it won't set this
    # Resp is a tuple: (success: bool, es_response: dict)
    for count, resp in enumerate(elastic_stream):
        if not resp[0]:
            # The error might not reference a document
            doc_id = resp[1].get("index", {}).get("_id")
            if doc_id:
                if doc_id in buffer:
                    original_doc = buffer[doc_id]
                    logger.warning(
                        "Error from Elasticsearch, continuing: %r (original document: %r)",
                        resp,
                        original_doc,
                    )
                else:
                    logger.warning(
                        "Error from Elasticsearch, continuing: %r (couldn't find doc in buffer of %s items)",
                        resp,
                        len(buffer),
                    )
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
