import re

import cloudfront

# Lines 1 & 2 are header
# # Lines 3 & 4 don't include past field 26 (fle-encrypted-fields)
# Line 5 includes 7 additional fields as per https://aws.amazon.com/about-aws/whats-new/2019/12/cloudfront-detailed-logs/
# Line 6 includes extra "unknown field data"
EXAMPLE = """#Version: 1.0
#Fields: date time x-edge-location sc-bytes c-ip cs-method cs(Host) cs-uri-stem sc-status cs(Referer) cs(User-Agent) cs-uri-query cs(Cookie) x-edge-result-type x-edge-request-id x-host-header cs-protocol cs-bytes time-taken x-forwarded-for ssl-protocol ssl-cipher x-edge-response-result-type cs-protocol-version fle-status fle-encrypted-fields c-port time-to-first-byte x-edge-detailed-result-type sc-content-type sc-content-len sc-range-start sc-range-end
2014-05-23\t01:13:11\tFRA2\t182\t192.0.2.10\tGET\td111111abcdef8.cloudfront.net\t/view/my/file.html\t200\twww.displaymyfiles.com\tMozilla/4.0%20(compatible;%20MSIE%205.0b1;%20Mac_PowerPC)\t-\tzip=98101\tRefreshHit\tMRVMF7KydIvxMWfJIglgwHQwZsbG2IhRJ07sn9AkKUFSHS9EXAMPLE==\td111111abcdef8.cloudfront.net\thttp\t-\t0.001\t-\t-\t-\tRefreshHit\tHTTP/1.1\tProcessed\t1
2014-05-23\t01:13:11\tFRA2\t182\t192.0.2.202\tGET\td111111abcdef8.cloudfront.net\t/soundtrack/happy.mp3\t304\twww.unknownsingers.com\tMozilla/4.0%20(compatible;%20MSIE%207.0;%20Windows%20NT%205.1)\ta=b&c=d\tzip=50158\tHit\txGN7KWpVEmB9Dp7ctcVFQC4E-nrcOcEKS3QyAez--06dV7TEXAMPLE==\td111111abcdef8.cloudfront.net\thttp\t-\t0.002\t-\t-\t-\tHit\tHTTP/1.1\t-\t-
2014-05-23\t01:13:11\tFRA2\t182\t192.0.2.100\tGET\td111111abcdef8.cloudfront.net\t/index.html\t200\t-\tMozilla/5.0%2520(Windows%2520NT)\t-\t-\tHit\tSOX4xwn4XV6Q4rgb7XiVGOHms_BGlTAC4KyHmureZmBNrjGdRLiNIQ==\td111111abcdef8.cloudfront.net\thttps\t23\t0.001\t-\tTLSv1.2\tECDHE-RSA-AES128-GCM-SHA256\tHit\tHTTP/2.0\t-\t-\t11040\t0.001\tHit\ttext/html\t78\t-\t-
2014-05-23\t01:13:11\tFRA2\t182\t192.0.2.100\tGET\td111111abcdef8.cloudfront.net\t/index.html\t200\t-\tMozilla/5.0%2520(Windows%2520NT)\t-\t-\tHit\tSOX4xwn4XV6Q4rgb7XiVGOHms_BGlTAC4KyHmureZmBNrjGdRLiNIQ==\td111111abcdef8.cloudfront.net\thttps\t23\t0.001\t-\tTLSv1.2\tECDHE-RSA-AES128-GCM-SHA256\tHit\tHTTP/2.0\t-\t-\t11040\t0.001\tHit\ttext/html\t78\t-\t-\tfoo\tbar\tbaz"""
NUM_HEADER_LINES = 2
# These fields should be present and set to the given value
EXPECTED_ITEMS = {
    "_type": "doc",  # test static field
    "@timestamp": "2014-05-23T01:13:11.000Z",  # test combining fields
    "aws.cloudfront.edge_location": "FRA2",  # test parsing field
    "http.response.total.bytes": 182,  # test converting to int
}
# These fields should be present and set to the given value OR absent
MAYBE_EXPECTED_ITEMS = {
    "aws.cloudfront.result_type_detailed": "Hit",  # Extra field added 2019-12
    "aws.cloudfront.time_to_first_byte": 0.001,  # Extra field added 2019-12 with float conversion
    "aws.cloudfront.unhandled_fields": "['foo', 'bar', 'baz']",
}
# These fields should not be present
EXPECTED_NON_ITEMS = {
    "http.response.content-range.start",  # Values of '-' should be ignored
}


def test_filenames() -> None:
    assert not cloudfront.check_filename("foo")
    assert cloudfront.check_filename("DIST012346.2019-09-30-01.abcdef.gz")
    assert cloudfront.check_filename("prefix/DIST012346.2019-09-30-01.abcdef.gz")


def test_basic() -> None:
    num_docs = 0
    for line_no, line in enumerate(EXAMPLE.splitlines()):
        response = list(cloudfront.transform(line, line_no))
        print("Line: %s Response: %s" % (repr(line), repr(response)))
        if line_no < NUM_HEADER_LINES:
            assert len(response) == 0
            continue
        # We assert on line_no to improve pytest failure output
        num_docs += 1
        assert line_no and len(response) == 1
        doc = response[0]
        assert "_index" in doc
        assert "_type" in doc
        assert "@timestamp" in doc
        # Check the value of a few keys
        for key in EXPECTED_ITEMS:
            # We assert on key to improve pytest failure output
            assert key and doc[key] == EXPECTED_ITEMS[key]
        for key in MAYBE_EXPECTED_ITEMS:
            if key in doc:
                # We assert on key to improve pytest failure output
                assert key and doc[key] == MAYBE_EXPECTED_ITEMS[key]
        for key in EXPECTED_NON_ITEMS:
            # We assert on key to improve pytest failure output
            assert key and key not in doc
        # Check key names are all correct
        for key, value in doc.items():
            assert isinstance(value, (str, int, float, bool))
            assert key.islower()
            # Limited punctuation is allowed
            assert re.sub("[._@-]", "", key).isalnum()
            assert value != "-"
    assert num_docs == (len(EXAMPLE.split('\n')) - NUM_HEADER_LINES)
