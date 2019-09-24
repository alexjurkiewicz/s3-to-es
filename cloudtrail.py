from typing import Iterable
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
}

DOC_NAME_REGEX = re.compile("[a-zA-Z][^A-Z]*")


def elasticsearch_reducer(k1: str, k2: str) -> str:
    """Combine items with '.' as per Elastic Common Schema convention."""
    if k1 is None:  # pylint: disable=no-else-return
        return str(k2)
    else:
        return str(k1) + "." + str(k2)


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
            if key in PREDEFINED_MAPPINGS:
                new_key = PREDEFINED_MAPPINGS[key]
                doc[new_key] = value
            else:
                # Convert eg 'accessKeyId' -> 'access_key_id'
                new_key = "_".join(i.lower() for i in re.findall(DOC_NAME_REGEX, key))
                new_key = new_key.replace("i_d", "id")  # little fixup hack
                doc["aws.cloudtrail." + new_key] = value

        doc["_type"] = "doc"  # Can be removed with ES > 7
        doc["_index"] = "cloudtrail-" + record["eventTime"].split("T")[0]
        yield doc
