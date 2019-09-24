# S3 to ES

[![Build Status](https://travis-ci.com/alexjurkiewicz/s3-to-es.svg?branch=master)](https://travis-ci.com/alexjurkiewicz/s3-to-es)

A Serverless Framework application to help ingest the following S3 logs into Elasticsearch Service:

* ALB access logs
* CloudFront access logs
* CloudTrail logs

It's easy to add other formats as well.

```plain
S3 Bucket ---(object creation notifications)---> Lambda ---(HTTPS POST)---> Elasticsearch Service
```

This repository implements the Lambda functions in the above diagram. You have to supply your own:

* S3 Buckets
  * S3 event mappings
* Elasticsearch Service domain
  * (Using a non-AWS ES cluster will work, but you'll have to modify the Lambda's authentication process.)

## Usage

1. Clone this repository
2. `npm install`
3. Create `serverless-variables.yml`
4. Deploy the stack: `npx serverless deploy --region us-west-2`
5. Add event triggers for your S3 buckets to trigger the correct Lambda function. The function ARNs are available in SSM Parameter Store under the `/s3-to-es/handlers/` prefix.

## Development

General logic is:

1. Get a line iterator of the source file
2. Convert every line into an ES document (Python dict) with a source-type-specific transform function
3. Upload the ES documents in batches to the ES cluster

The lambda handler lives in `handler.py` and figures out the log type it supports at startup. The transformation function is from `alb.py`/`cloudfront.py`/etc, and the heavy lifting is performed by all the code in `common.py`.

### Adding support for a new log format

1. Write a Python function that transforms the log file into ES documents one line at a time.
    1. See `cloudfront.py` for an example.
    2. Fields `_index`, `_type` are required.
    3. Use the [Elastic Common Schema](https://www.elastic.co/guide/en/ecs/current/ecs-reference.html) to detemine appropriate field names.
2. Import this function into `handler.py` and add a mapping based on `LOG_TYPE` environment variable.
3. Add the following to `serverless.yml`:
    1. New entry under `functions`
    2. New `AWS::Lambda::Permission` resource. Use `AlbHandlerFunctionPolicy` as a template.
    3. New `AWS::SSM::Parameter` resource. Use `AlbHandlerParameter` as a template.
4. Deploy and test your code.
