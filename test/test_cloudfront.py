import re

import cloudfront

EXAMPLE = """#Version:\t1.0\n#Fields:\tdate\ttime\tx-edge-location\tsc-bytes\tc-ip\tcs-method\tcs(Host)\tcs-uri-stem\tsc-status\tcs(Referer)\tcs(User-Agent)\tcs-uri-query\tcs(Cookie)\tx-edge-result-type\tx-edge-request-id\tx-host-header\tcs-protocol\tcs-bytes\ttime-taken\tx-forwarded-for\tssl-protocol\tssl-cipher\tx-edge-response-result-type\tcs-protocol-version\tfle-status\tfle-encrypted-fields\n2014-05-23\t01:13:11\tFRA2\t182\t192.0.2.10\tGET\td111111abcdef8.cloudfront.net\t/view/my/file.html\t200\twww.displaymyfiles.com\tMozilla/4.0%20(compatible;%20MSIE%205.0b1;%20Mac_PowerPC)\t-\tzip=98101\tRefreshHit\tMRVMF7KydIvxMWfJIglgwHQwZsbG2IhRJ07sn9AkKUFSHS9EXAMPLE==\td111111abcdef8.cloudfront.net\thttp\t-\t0.001\t-\t-\t-\tRefreshHit\tHTTP/1.1\tProcessed\t1\n2014-05-23\t01:13:12\tLAX1\t2390282\t192.0.2.202\tGET\td111111abcdef8.cloudfront.net\t/soundtrack/happy.mp3\t304\twww.unknownsingers.com\tMozilla/4.0%20(compatible;%20MSIE%207.0;%20Windows%20NT%205.1)\ta=b&c=d\tzip=50158\tHit\txGN7KWpVEmB9Dp7ctcVFQC4E-nrcOcEKS3QyAez--06dV7TEXAMPLE==\td111111abcdef8.cloudfront.net\thttp\t-\t0.002\t-\t-\t-\tHit\tHTTP/1.1\t-\t-\n"""
EXPECTED_ITEMS = {
    "_type": "doc",  # test static field
    "@timestamp": "2014-05-23T01:13:11.000Z",  # test combining fields
    "aws.cloudfront.edge_location": "FRA2",  # test parsing field
    "http.response.total.bytes": 182,  # test converting to int
}


def test_filenames() -> None:
    assert not cloudfront.check_filename("foo")
    assert cloudfront.check_filename("DIST012346.2019-09-30-01.abcdef.gz")
    assert cloudfront.check_filename("prefix/DIST012346.2019-09-30-01.abcdef.gz")


def test_basic() -> None:
    docs = 0
    for line_no, line in enumerate(EXAMPLE.splitlines()):
        docs += 1
        response = list(cloudfront.transform(line, line_no))
        if line_no in (0, 1):
            assert len(response) == 0
            continue
        assert len(response) == 1
        doc = response[0]
        assert "_index" in doc
        assert "_type" in doc
        assert "@timestamp" in doc
        if line_no == 2:
            # Check the value of a few keys
            for key in EXPECTED_ITEMS:
                assert doc[key] == EXPECTED_ITEMS[key]
            # Check key names are all correct
            for key, value in doc.items():
                assert isinstance(value, (str, int, float, bool))
                assert key.islower()
                # Limited punctuation is allowed
                assert re.sub("[._@-]", "", key).isalnum()
                assert value != "-"
    assert docs == 4  # 4 items in EXAMPLE
