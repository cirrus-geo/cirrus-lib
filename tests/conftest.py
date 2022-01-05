import os


if not 'AWS_DEFAULT_REGION' in os.environ:
    os.environ['AWS_DEFAULT_REGION'] = 'us-west-2'
