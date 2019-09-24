import urllib.parse
import re
from typing import Iterable

import common


def check_filename(filename: str) -> bool:
    return bool(
        re.match(
            r".*AWSLogs/.*/elasticloadbalancing/.*\d+_elasticloadbalancing_.*.log.gz$",
            filename,
        )
    )


def transform(
    line: str, _line_no: int
) -> Iterable[
    common.EsDocument
]:  # pylint: disable=too-many-locals,too-many-branches,too-many-statements
    """
    Parse ALB access log line into Elasticsearch document.

    Note that almost every field in the access log can be "-" to indicate N/A.

    Attempts to be compatible with Elasticsearch Common Schema (ECS) 1.0, but
    some of the fields aren't explicitly specified in the ECS so we guess.
    """

    if line.startswith("Enable AccessLog for ELB: "):
        return []

    doc: common.EsDocument = {}

    # Required by Elasticsearch
    doc["_type"] = "doc"  # Remove for ES > 7
    doc["ecs.version"] = "1.0.1"

    doc["http.type"], line = line.split(" ", 1)

    timestamp, line = line.split(" ", 1)
    doc["@timestamp"] = timestamp
    doc["_index"] = "alb-%s" % timestamp.split("T")[0]

    doc["aws.lb.resource_id"], line = line.split(" ", 1)

    client, line = line.split(" ", 1)
    client_ip, client_port = client.split(":")
    doc["client.ip"], doc["client.port"] = client_ip, int(client_port)
    doc["client.protocol"] = "ipv6" if ":" in client_ip else "ipv4"

    server, line = line.split(" ", 1)
    if server != "-":
        server_ip, server_port = server.split(":")
        doc["server.ip"], doc["server.port"] = server_ip, int(server_port)

    request_duration, line = line.split(" ", 1)
    if request_duration != "-1":
        doc["event.duration"] = int(float(request_duration) * 1_000_000_000)  # s to ns
    target_duration, line = line.split(" ", 1)
    if target_duration != "-1":
        doc["aws.lb.target_processing_time"] = int(
            float(target_duration) * 1_000_000_000
        )  # s to ns
    client_duration, line = line.split(" ", 1)
    if client_duration != "-1":
        doc["aws.lb.response_processing_time"] = int(
            float(client_duration) * 1_000_000_000
        )  # s to ns
    lb_status_code, line = line.split(" ", 1)
    doc["http.response.status_code"] = int(lb_status_code)
    target_status_code, line = line.split(" ", 1)
    if target_status_code != "-":
        doc["aws.lb.backend_status_code"] = int(target_status_code)
    request_bytes, line = line.split(" ", 1)
    doc["http.request.total.bytes"] = int(request_bytes)
    response_bytes, line = line.split(" ", 1)
    doc["http.response.total.bytes"] = int(response_bytes)

    _, request, line = line.split('"', 2)
    line = line[1:]  # Remove space prefix
    doc["http.request.method"], request = request.split(" ", 1)
    url_full, request = request.split(" ", 1)
    doc["url.full"] = url_full
    url_parsed = urllib.parse.urlparse(url_full)
    if url_parsed.netloc:
        doc["url.domain"] = url_parsed.netloc
    if url_parsed.path:
        doc["url.path"] = url_parsed.path
    if url_parsed.query:
        doc["url.query"] = url_parsed.query
    if url_parsed.fragment:
        doc["url.fragment"] = url_parsed.fragment
    try:
        doc["http.version"] = request.split("/")[1]
    except IndexError:  # If the version string was unparseable junk
        pass

    _, user_agent, line = line.split('"', 2)
    line = line[1:]  # Remove space prefix
    doc["user_agent.original"] = user_agent

    ssl_cipher, line = line.split(" ", 1)
    if ssl_cipher != "-":
        doc["http.ssl.cipher"] = ssl_cipher
    ssl_protocol, line = line.split(" ", 1)
    if ssl_protocol != "-":
        doc["http.ssl.protocol"] = ssl_protocol

    target_group_arn, line = line.split(" ", 1)
    if target_group_arn != "-":
        doc["aws.lb.target_group_arn"] = target_group_arn

    _, trace_id, line = line.split('"', 2)
    line = line[1:]  # Remove space prefix
    if trace_id != "-":
        doc["http.request.header.x-amzn-trace-id"] = trace_id

    _, sni_domain, line = line.split('"', 2)
    line = line[1:]  # Remove space prefix
    if sni_domain != "-":
        doc["http.ssl.sni_host"] = sni_domain

    _, ssl_cert_arn, line = line.split('"', 2)
    line = line[1:]  # Remove space prefix
    if ssl_cert_arn != "-":
        doc["aws.lb.certificate_arn"] = ssl_cert_arn

    matched_rule, line = line.split(" ", 1)
    if matched_rule != "-":
        doc["aws.lb.matched_rule"] = int(matched_rule)

    doc["event.start"], line = line.split(" ", 1)

    _, actions, line = line.split('"', 2)
    line = line[1:]  # Remove space prefix
    if actions != "-":
        for action in actions.split(","):
            doc["aws.lb.action.%s" % action] = True

    _, redirect_url, line = line.split('"', 2)
    line = line[1:]  # Remove space prefix
    if redirect_url != "-":
        doc["http.response.header.location"] = redirect_url

    _, error_reason, line = line.split('"', 2)
    line = line[1:]  # Remove space prefix
    if error_reason != "-":
        doc["error.code"] = error_reason

    if line:
        doc["aws.lb.unhandled_fields"] = line

    yield doc
