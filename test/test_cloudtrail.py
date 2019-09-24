import re

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
    "aws.cloudtrail.source_i_p_address": "203.0.113.64",
    "aws.cloudtrail.user_agent": "signin.amazonaws.com",
    "aws.cloudtrail.request_parameters.name": "arn:aws:cloudtrail:us-east-2:123456789012:trail/My-First-Trail",
    "aws.cloudtrail.response_elements": None,
    "aws.cloudtrail.request_id": "ddf5140f-EXAMPLE",
    "event.id": "7116c6a1-EXAMPLE",
    "aws.cloudtrail.read_only": False,
    "event.dataset": "AwsApiCall",
    "aws.cloudtrail.recipient_account_id": "123456789012",
    "_type": "doc",
    "_index": "cloudtrail-2019-06-19",
}


def test_basic():
    response = list(cloudtrail.transform(EXAMPLE, 0))
    assert len(response) == 1
    assert response[0] == EXPECTED
