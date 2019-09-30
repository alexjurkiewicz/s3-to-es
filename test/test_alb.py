import re

import alb

EXAMPLE_HTTP = '''http 2018-07-02T22:23:00.186641Z app/my-loadbalancer/50dc6c495c0c9188 192.168.131.39:2817 10.0.0.1:80 0.000 0.001 0.000 200 200 34 366 "GET http://www.example.com:80/?foo=bar#loc HTTP/1.1" "curl/7.46.0" ECDHE-RSA-AES128-GCM-SHA256 TLSv1.2 arn:aws:elasticloadbalancing:us-east-2:123456789012:targetgroup/my-targets/73e2d6bc24d8a067 "Root=1-58337262-36d228ad5d99923122bbe354" "www.example.com" "arn:aws:acm:us-east-2:123456789012:certificate/12345678-1234-1234-1234-123456789012" 0 2018-07-02T22:22:48.364000Z "forward" "https://redirect.location" "AuthInvalidCookie"'''
EXAMPLE_HTTP_ITEMS = {
    "_type": "doc",
    "http.type": "http",
    "client.ip": "192.168.131.39",  # test splitting client field
    "client.port": 2817,  # test converting to int
    "http.request.method": "GET",  # test splitting request field
    "url.full": "http://www.example.com:80/?foo=bar#loc",  # test splitting request field
    "url.query": "foo=bar",  # test parsing URL field
    "url.fragment": "loc",
}

EXAMPLE_HTTP_BAD_HTTP_VER = '''http 2018-07-02T22:23:00.186641Z app/my-loadbalancer/50dc6c495c0c9188 192.168.131.39:2817 10.0.0.1:80 0.000 0.001 0.000 200 200 34 366 "GET http://www.example.com:80/?foo=bar#loc badver" "curl/7.46.0" ECDHE-RSA-AES128-GCM-SHA256 TLSv1.2 arn:aws:elasticloadbalancing:us-east-2:123456789012:targetgroup/my-targets/73e2d6bc24d8a067 "Root=1-58337262-36d228ad5d99923122bbe354" "www.example.com" "arn:aws:acm:us-east-2:123456789012:certificate/12345678-1234-1234-1234-123456789012" 0 2018-07-02T22:22:48.364000Z "forward" "https://redirect.location" "AuthInvalidCookie"'''

EXAMPLE_HTTP_EXTRA_FIELDS = """http 2018-07-02T22:23:00.186641Z app/my-loadbalancer/50dc6c495c0c9188 192.168.131.39:2817 10.0.0.1:80 0.000 0.001 0.000 200 200 34 366 "GET http://www.example.com:80/?foo=bar#loc HTTP/1.1" "curl/7.46.0" ECDHE-RSA-AES128-GCM-SHA256 TLSv1.2 arn:aws:elasticloadbalancing:us-east-2:123456789012:targetgroup/my-targets/73e2d6bc24d8a067 "Root=1-58337262-36d228ad5d99923122bbe354" "www.example.com" "arn:aws:acm:us-east-2:123456789012:certificate/12345678-1234-1234-1234-123456789012" 0 2018-07-02T22:22:48.364000Z "forward" "https://redirect.location" "AuthInvalidCookie" foo bar"""


def test_filenames() -> None:
    assert not alb.check_filename("foo")
    assert alb.check_filename(
        "AWSLogs/0123/elasticloadbalancing/region/yyyy/mm/dd/0123_elasticloadbalancing_region_load-balancer-id_end-time_ip-address_random-string.log.gz"
    )
    assert alb.check_filename(
        "prefix/AWSLogs/0123/elasticloadbalancing/region/yyyy/mm/dd/0123_elasticloadbalancing_region_load-balancer-id_end-time_ip-address_random-string.log.gz"
    )
    assert not alb.check_filename("AWSLogs/123456789012/ELBAccessLogTestFile")


def test_transform() -> None:
    response = list(alb.transform(EXAMPLE_HTTP, 0))
    assert len(response) == 1
    doc = response[0]
    assert "_index" in doc
    assert "_type" in doc
    assert "@timestamp" in doc
    # Check the value of a few keys
    for key in EXAMPLE_HTTP_ITEMS:
        assert doc[key] == EXAMPLE_HTTP_ITEMS[key]
    # Check key names are all correct
    for key, value in doc.items():
        assert isinstance(value, (str, int, float, bool))
        assert key.islower()
        # Limited punctuation is allowed
        assert re.sub("[._@-]", "", key).isalnum()


def test_enable_line() -> None:
    response = list(alb.transform("Enable AccessLog for ELB: elb/123", 0))
    assert len(response) == 0


def test_bad_httpver() -> None:
    response = list(alb.transform(EXAMPLE_HTTP_BAD_HTTP_VER, 0))
    assert len(response) == 1
    doc = response[0]
    assert "http.version" not in doc


def test_extra_fields() -> None:
    response = list(alb.transform(EXAMPLE_HTTP_EXTRA_FIELDS, 0))
    assert len(response) == 1
    assert response[0]["aws.lb.unhandled_fields"] == "foo bar"
