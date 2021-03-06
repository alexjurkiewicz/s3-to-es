import re
from typing import Iterable
import urllib.parse

import common


def check_filename(filename: str) -> bool:
    return bool(
        re.match(r".*[A-Z0-9]+\.\d{4}-\d{2}-\d{2}-\d{2}\.[0-9a-f]+\.gz$", filename)
    )


def decode(s: str) -> str:
    """URL decoding from RFC 1738 with a twist!

    Cloudfront double-encodes a couple of characters, so handle them specially.
    """
    return urllib.parse.unquote(
        s.replace("%2522", '"').replace("%255C", "\\").replace("%2520", " ")
    )


def transform(line: str, line_no: int) -> Iterable[common.EsDocument]:
    if line_no in (0, 1):
        return []
    data = line.strip().split("\t")
    doc: common.EsDocument = {
        # Required by Elasticsearch
        "_index": "cloudfront-%s" % data[0],
        "_type": "doc",  # Remove for ES > 7
        "ecs.version": "1.1.0",
        # Actual data
        "@timestamp": "%sT%s.000Z" % (data[0], data[1]),
        "aws.cloudfront.edge_location": data[2],
        "http.response.total.bytes": int(data[3]),
        "client.ip": data[4],
        "client.protocol": "ipv6" if ":" in data[4] else "ipv4",
        "http.request.method": data[5],
        "aws.cloudfront.distribution_id": data[6],
        "url.path": data[7],
        "http.response.status_code": int(data[8]),
        "http.request.referrer": data[9],
        "user_agent.original": decode(data[10]),
        "url.query": data[11],
        # "url.cookie": data[12], # disabled
        "aws.cloudfront.result_type_edge": data[13],
        "aws.cloudfront.request_id": data[14],
        "http.request.host": data[15],
        "http.protocol": data[16],
        "http.request.total.bytes": int(data[17]) if data[17] != "-" else "-",
        "event.duration": float(data[18]) * 1_000_000_000,  # s to ns
        "http.request.x-forwarded-for": data[19],
        "http.ssl.protocol": data[20],
        "http.ssl.cipher": data[21],
        "aws.cloudfront.result_type_final": data[22],
        "http.version": data[23],
        "aws.cloudfront.fle_status": data[24],
        "aws.cloudfront.fle_fields": data[25],
    }
    # New fields added 2019-12-12
    if len(data) > 26:
        doc["client.port"] = int(data[26])
        doc["aws.cloudfront.time_to_first_byte"] = float(data[27])
        doc["aws.cloudfront.result_type_detailed"] = data[28]
        doc["http.response.content-type"] = data[29]
        doc["http.response.content-length"] = int(data[30])
        doc["http.response.content-range.start"] = int(data[31]) if data[31] != "-" else "-"
        doc["http.response.content-range.end"] = int(data[32]) if data[32] != "-" else "-"
    if len(data) > 33:
        doc["aws.cloudfront.unhandled_fields"] = repr(data[33:])

    # Strip all fields with "-"
    yield {k: v for k, v in doc.items() if v != "-"}
