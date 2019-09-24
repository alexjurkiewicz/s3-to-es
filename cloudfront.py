from typing import Iterable

import common


def transform(line: str, line_no: int) -> Iterable[common.EsDocument]:
    if line_no in (0, 1):
        return []
    data = line.strip().split("\t")
    yield {
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
        "user_agent.original": data[10],
        "url.query": data[11],
        # "url.cookie": data[12], # disabled
        "aws.cloudfront.result_type_edge": data[13],
        "aws.cloudfront.request_id": data[14],
        "http.request.host": data[15],
        "http.protocol": data[16],
        "http.request.total.bytes": int(data[17]) if data[17] != "-" else 0,
        "event.duration": float(data[18]) * 1_000_000,  # nanoseconds
        "http.request.x-forwarded-for": data[19],
        "http.ssl.protocol": data[20],
        "http.ssl.cipher": data[21],
        "aws.cloudfront.result_type_final": data[22],
        "http.version": data[23],
        "aws.cloudfront.fle_status": data[24],
        "aws.cloudfront.fle_fields": data[25],
    }
