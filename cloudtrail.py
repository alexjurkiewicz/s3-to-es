from typing import Iterable, Optional
import json
import re

import flatten_dict  # type: ignore

import common

PREDEFINED_MAPPINGS = {
    "awsRegion": "cloud.account.region",
    "eventID": "event.id",
    "eventName": "event.action",
    "eventSource": "event.module",
    "eventTime": "@timestamp",
    "eventType": "event.dataset",
    # These fields can be either a string or an array of strings, which confuses
    # ES. Convert the string form to an array of one string
    "request_parameters.policy": "aws.cloudtrail.request_parameters.policy.0",
}
STRING_FIELDS = set(["aws.cloudtrail.response_elements.version"])


def elasticsearch_reducer(k1: Optional[str], k2: str) -> str:
    """Combine items with '.' as per Elastic Common Schema convention."""
    if k1 is None:  # pylint: disable=no-else-return
        return str(k2)
    else:
        return str(k1) + "." + str(k2)


REGEX_ONE = re.compile("(.)([A-Z][a-z]+)")
REGEX_TWO = re.compile("([a-z0-9])([A-Z])")


def convert_cloudtrail_key(key: str) -> str:
    """
    Convert eg 'accessKeyId' -> 'access_key_id'.

    This is more complex than it looks because we have to watch out for acronyms
    in things like `sourceIPAddress` or `ARN`.
    """
    s1 = re.sub(REGEX_ONE, r"\1_\2", key)
    return re.sub(REGEX_TWO, r"\1_\2", s1).lower()


def check_filename(filename: str) -> bool:
    return bool(re.match(r".*CloudTrail/.*/\d+_CloudTrail_.*.json.gz$", filename))


def transform(line: str, _line_no: int) -> Iterable[common.EsDocument]:
    try:
        data = json.loads(line)
    except:  # pylint: disable=bare-except
        return

    for record in data["Records"]:
        doc: common.EsDocument = {}
        # Flatten the dictionary
        record = flatten_dict.flatten(
            record, reducer=elasticsearch_reducer, enumerate_types=(list,)
        )
        for key, value in record.items():
            if value is None:
                continue
            if key in PREDEFINED_MAPPINGS:
                new_key = PREDEFINED_MAPPINGS[key]
                doc[new_key] = value
            else:
                new_key = "aws.cloudtrail." + convert_cloudtrail_key(key)
                # Some fields come through with differing types, we need to
                # explicitly list these and convert them as apropriate.
                if new_key in STRING_FIELDS:
                    value = str(value)
                doc[new_key] = value

        doc["_type"] = "doc"  # Can be removed with ES > 7
        doc["_index"] = "cloudtrail-" + record["eventTime"].split("T")[0]
        doc["event.provider"] = "cloudtrail"
        yield doc
