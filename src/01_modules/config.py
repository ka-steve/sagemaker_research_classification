# import os
# import sys
# sys.path.append(os.path.abspath(os.getcwd()))
# from utils import version_test

_VERSION = '0.1'

AWS_REGION = "eu-west-2" # London

DEFAULT_S3_BUCKET_NAME = "sagemaker-research-methodology-extraction"
DEFAULT_S3_BUCKET_ROOT = f's3://{DEFAULT_S3_BUCKET_NAME}'

S3_DATA_PREFIX = '01_data'
S3_RAW_DATA_PREFIX = f'{S3_DATA_PREFIX}/01_raw'
S3_PROCESSED_DATA_PREFIX = f'{S3_DATA_PREFIX}/02_processed'

DEFAULT_PROCESSING_FILEPATH_PREFIX = '/opt/ml/processing'

SEMANTICSCHOLAR_API_BASE_URL = "https://api.semanticscholar.org/datasets/v1/release"
SEMANTICSCHOLAR_SECRET_NAME = 'semanticscholar_api_key'
SEMANTICSCHOLAR_SECRET_KEY = 'x-api-key'

OPENALEX_SOURCE_S3_BUCKET_NAME = 'openalex'
OPENALEX_S3_RAW_DATA_PREFIX = f'{S3_RAW_DATA_PREFIX}/openalex'

print(f'config.py loaded: v{_VERSION}')