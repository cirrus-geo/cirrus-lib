import os

import moto
import boto3
import pytest

from boto3utils import s3


if not 'AWS_DEFAULT_REGION' in os.environ:
    os.environ['AWS_DEFAULT_REGION'] = 'us-east-1'


@pytest.fixture
def aws_credentials():
    """Mocked AWS Credentials for moto."""
    os.environ['AWS_ACCESS_KEY_ID'] = 'testing'
    os.environ['AWS_SECRET_ACCESS_KEY'] = 'testing'
    os.environ['AWS_SECURITY_TOKEN'] = 'testing'
    os.environ['AWS_SESSION_TOKEN'] = 'testing'
    os.environ['AWS_DEFAULT_REGION'] = 'us-east-1'
    os.environ['AWS_REGION'] = 'us-east-1'


@pytest.fixture
def boto3utils_s3(aws_credentials):
    with moto.mock_s3():
        yield s3(boto3.session.Session(region_name='us-east-1'))


@pytest.fixture
def sqs(aws_credentials):
    with moto.mock_sqs():
        yield boto3.client('sqs', region_name='us-east-1')


@pytest.fixture
def dynamo(aws_credentials):
    with moto.mock_dynamodb():
        yield boto3.client('dynamodb', region_name='us-east-1')
