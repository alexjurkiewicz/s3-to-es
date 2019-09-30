import json

import cloudtrail

EXAMPLE = """{"Records":[{
    "eventVersion": "1.05",
    "userIdentity": {
        "type": "IAMUser",
        "principalId": "AIDAJDPLRKLG7UEXAMPLE",
        "arn": "arn:aws:iam::123456789012:user/Mary_Major",
        "accountId": "123456789012",
        "accessKeyId": "AKIAIOSFODNN7EXAMPLE",
        "userName": "Mary_Major",
        "sessionContext": {
            "sessionIssuer": {},
            "webIdFederationData": {},
            "attributes": {
                "mfaAuthenticated": "false",
                "creationDate": "2019-06-18T22:28:31Z"
            }
        },
        "invokedBy": "signin.amazonaws.com"
    },
    "eventTime": "2019-06-19T00:18:31Z",
    "eventSource": "cloudtrail.amazonaws.com",
    "eventName": "StartLogging",
    "awsRegion": "us-east-2",
    "sourceIPAddress": "203.0.113.64",
    "userAgent": "signin.amazonaws.com",
    "requestParameters": {
        "name": "arn:aws:cloudtrail:us-east-2:123456789012:trail/My-First-Trail"
    },
    "responseElements": null,
    "requestID": "ddf5140f-EXAMPLE",
    "eventID": "7116c6a1-EXAMPLE",
    "readOnly": false,
    "eventType": "AwsApiCall",
    "recipientAccountId": "123456789012"
}]}\n"""
EXPECTED = {
    "aws.cloudtrail.event_version": "1.05",
    "aws.cloudtrail.user_identity.type": "IAMUser",
    "aws.cloudtrail.user_identity.principal_id": "AIDAJDPLRKLG7UEXAMPLE",
    "aws.cloudtrail.user_identity.arn": "arn:aws:iam::123456789012:user/Mary_Major",
    "aws.cloudtrail.user_identity.account_id": "123456789012",
    "aws.cloudtrail.user_identity.access_key_id": "AKIAIOSFODNN7EXAMPLE",
    "aws.cloudtrail.user_identity.user_name": "Mary_Major",
    "aws.cloudtrail.user_identity.session_context.attributes.mfa_authenticated": "false",
    "aws.cloudtrail.user_identity.session_context.attributes.creation_date": "2019-06-18T22:28:31Z",
    "aws.cloudtrail.user_identity.invoked_by": "signin.amazonaws.com",
    "@timestamp": "2019-06-19T00:18:31Z",
    "event.module": "cloudtrail.amazonaws.com",
    "event.action": "StartLogging",
    "cloud.account.region": "us-east-2",
    "aws.cloudtrail.source_ip_address": "203.0.113.64",
    "aws.cloudtrail.user_agent": "signin.amazonaws.com",
    "aws.cloudtrail.request_parameters.name": "arn:aws:cloudtrail:us-east-2:123456789012:trail/My-First-Trail",
    "aws.cloudtrail.request_id": "ddf5140f-EXAMPLE",
    "event.id": "7116c6a1-EXAMPLE",
    "aws.cloudtrail.read_only": False,
    "event.dataset": "AwsApiCall",
    "event.provider": "cloudtrail",
    "aws.cloudtrail.recipient_account_id": "123456789012",
    "_type": "doc",
    "_index": "cloudtrail-2019-06-19",
}


def test_key_convert() -> None:
    assert cloudtrail.convert_cloudtrail_key("awsRegion") == "aws_region"
    assert cloudtrail.convert_cloudtrail_key("source.awsRegion") == "source.aws_region"


def test_transform() -> None:
    response = list(cloudtrail.transform(EXAMPLE, 0))
    assert len(response) == 1
    assert response[0] == EXPECTED


def test_transform_fail() -> None:
    response = list(cloudtrail.transform("asd", 0))
    assert len(response) == 0


def test_transform_stringify() -> None:
    obj = {
        "Records": [
            {"eventTime": "2019-09-09T00:00:01Z", "responseElements": {"version": 3}}
        ]
    }
    response = list(cloudtrail.transform(json.dumps(obj), 0))
    assert len(response) == 1
    print(response[0])
    assert response[0]["aws.cloudtrail.response_elements.version"] == "3"


def test_s3_key_names() -> None:
    assert not cloudtrail.check_filename("foo")
    # Normal
    assert cloudtrail.check_filename(
        "AWSLogs/0123/CloudTrail/region/2019/09/09/123456789012_CloudTrail_us-west-1_20140620T1255ZHdkvFTXOA3Vnhbc.json.gz"
    )
    assert cloudtrail.check_filename(
        "prefix/AWSLogs/0123/CloudTrail/region/2019/09/09/123456789012_CloudTrail_us-west-1_20140620T1255ZHdkvFTXOA3Vnhbc.json.gz"
    )
    # AWS Org
    assert cloudtrail.check_filename(
        "AWSLogs/0123/0123/CloudTrail/region/2019/09/09/123456789012_CloudTrail_us-west-1_20140620T1255ZHdkvFTXOA3Vnhbc.json.gz"
    )
    assert cloudtrail.check_filename(
        "prefix/AWSLogs/0123/0123/CloudTrail/region/2019/09/09/123456789012_CloudTrail_us-west-1_20140620T1255ZHdkvFTXOA3Vnhbc.json.gz"
    )
    # Digest files
    assert not cloudtrail.check_filename(
        "AWSLogs/0123/CloudTrail-Digest/region/digest-end-year/digest-end-month/digest-end-date/aws-account-id_CloudTrail-Digest_region_trail-name_region_digest_end_timestamp.json.gz"
    )
    assert not cloudtrail.check_filename(
        "prefix/AWSLogs/0123/CloudTrail-Digest/region/digest-end-year/digest-end-month/digest-end-date/aws-account-id_CloudTrail-Digest_region_trail-name_region_digest_end_timestamp.json.gz"
    )
    assert not cloudtrail.check_filename(
        "AWSLogs/0123/0123/CloudTrail-Digest/region/digest-end-year/digest-end-month/digest-end-date/aws-account-id_CloudTrail-Digest_region_trail-name_region_digest_end_timestamp.json.gz"
    )
    assert not cloudtrail.check_filename(
        "prefix/AWSLogs/0123/0123/CloudTrail-Digest/region/digest-end-year/digest-end-month/digest-end-date/aws-account-id_CloudTrail-Digest_region_trail-name_region_digest_end_timestamp.json.gz"
    )
