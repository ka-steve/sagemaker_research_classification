import json
import boto3
from botocore.exceptions import ClientError


def version_test()->str:
    # Helper util to confirm that the last version of the helper_utils.py is loaded
    version = '0.2.12'
    # print(version)
    return version


def get_secret(region_name:str, secret_name:str)->dict:
    # Source of the function:
    # https://eu-west-2.console.aws.amazon.com/secretsmanager/secret?name=semanticscholar_api_key#secret-details-sample-code-section
    # Create a Secrets Manager client
    session = boto3.session.Session()
    client = session.client(
        service_name='secretsmanager',
        region_name=region_name
    )

    try:
        get_secret_value_response = client.get_secret_value(
            SecretId=secret_name
        )
    except ClientError as e:
        # For a list of exceptions thrown, see
        # https://docs.aws.amazon.com/secretsmanager/latest/apireference/API_GetSecretValue.html
        raise e

    secret = json.loads(get_secret_value_response['SecretString'])
    return secret


def ensure_path(path:str)->None:
    from pathlib import Path
    Path(path).mkdir(parents=True, exist_ok=True)


# If loading this module after kernel start with the usual way:
# # import os
# # import sys
# # from importlib import reload
# # sys.path.append(os.path.abspath(os.path.join(os.getcwd(), '..', '01_modules')))
# # import helper_utils
# # reload(helper_utils)
# # from helper_utils import get_secret
# there should be two printed version numbers printed in the loader notebook, 
# while on subsequent cell reruns, there should be only one version printed
print(f'utils.py loaded: v{version_test()}')