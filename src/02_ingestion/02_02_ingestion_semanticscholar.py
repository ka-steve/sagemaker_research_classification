# ---
# jupyter:
#   jupytext:
#     formats: ipynb,py:percent
#     text_representation:
#       extension: .py
#       format_name: percent
#       format_version: '1.3'
#       jupytext_version: 1.17.2
#   kernelspec:
#     display_name: conda_pytorch_p310
#     language: python
#     name: conda_pytorch_p310
# ---

# %%
import requests
import boto3
import json
import os
import sys
import argparse
import importlib
import subprocess
from IPython.display import display

# Adding ../01_modules or ./01_modules to the system path so that we can load modules from 
# there as well
modules_path_in_notebook = os.path.abspath(os.path.join(os.getcwd(), '..', '01_modules'))
modules_path_in_processing_script = os.path.abspath(os.path.join(os.getcwd(), '01_modules'))
if os.path.exists(modules_path_in_notebook):
    sys.path.append(modules_path_in_notebook)
if os.path.exists(modules_path_in_processing_script):
    sys.path.append(modules_path_in_processing_script)

result = subprocess.run(["pwd"], capture_output=True, text=True)
print(result.stdout)
result = subprocess.run(["ls", "-l"], capture_output=True, text=True)
print(result.stdout)
# # Jupyter only reads a local module the first time after 
# # kernel start. Re-running a cell with 
# # "from mymodulename import *" would not change
# # anything, even if the imported module has since changed.
# # As a workaround, we need to directly load the module, 
# # use importlib.reload to reload it and then import * 
import utils
_ = importlib.reload(utils)
import config
_ = importlib.reload(config) 

# %%
semanticscholar_secret = utils.get_secret(config.AWS_REGION, config.SEMANTICSCHOLAR_SECRET_NAME)
SEMANTICSCHOLAR_API_KEY = semanticscholar_secret[config.SEMANTICSCHOLAR_SECRET_KEY]
print(f'Keys stored in Secrets Managers for the secret "{config.SEMANTICSCHOLAR_SECRET_NAME}":', list(semanticscholar_secret.keys()))


# %%
def semanticscholar_get_release_ids():
    """Fetching the list of dataset release IDs."""
    response = requests.get(config.SEMANTICSCHOLAR_API_BASE_URL)
    response.raise_for_status()
    res = response.json()
    print(res)
    return res


def semanticscholar_get_latest_metadata(release_id):
    """Fetch the metadata for the latest dataset release."""
    url = f'{config.SEMANTICSCHOLAR_API_BASE_URL}/{release_id}'
    response = requests.get(url)
    response.raise_for_status()
    res_json = response.json()
    res = []
    for dataset in res_json['datasets']:
        if dataset['name'] in ['papers', 's2orc']:
            res.append(dataset)
            display(dataset)
    return res


def get_releases_and_metadata():
    release_ids = semanticscholar_get_release_ids()
    latest_release_id = release_ids[-1]

    semanticscholar_get_latest_metadata(latest_release_id)
    return latest_release_id


SEMANTICSCHOLAR_LATEST_RELEASE_ID = get_releases_and_metadata()
SEMANTICSCHOLAR_LATEST_RELEASE_ID

# %%
parser = argparse.ArgumentParser()
parser.add_argument("--runtype", type=str, default='dev')
parser.add_argument("--test-argument-key-01", type=str, default='test-argument-default-value-01')
parser.add_argument("--test-argument-key-02", type=str, default='test-argument-default-value-02')
args, _ = parser.parse_known_args()
RUNTYPE = args.runtype

print (args)

if RUNTYPE == 'dev':
    PROCESSING_FILEPATH_PREFIX = '_dev_processing'
elif RUNTYPE == 'prod':
    PROCESSING_FILEPATH_PREFIX = config.DEFAULT_PROCESSING_FILEPATH_PREFIX
else:
    raise ValueError('Argument --runtype should be either "dev" or "prod" (without quotes).')

PROCESSING_FILEPATH_INPUT = f'{PROCESSING_FILEPATH_PREFIX}/input/data/'
PROCESSING_FILEPATH_OUTPUT = f'{PROCESSING_FILEPATH_PREFIX}/output/results/'

utils.ensure_path(PROCESSING_FILEPATH_INPUT)
utils.ensure_path(PROCESSING_FILEPATH_OUTPUT)

with open(os.path.join(PROCESSING_FILEPATH_INPUT, 'test.txt'), "r", encoding='utf-8') as test_file:
    test_content = test_file.read()

with open(os.path.join(PROCESSING_FILEPATH_OUTPUT, 'results.txt'), "w", encoding='utf-8') as results_file:
    results_file.write('\n'.join([
        f'semanticscholar_secret_keys[0]: {list(semanticscholar_secret.keys())[0]}',
        f'SEMANTICSCHOLAR_LATEST_RELEASE_ID: {SEMANTICSCHOLAR_LATEST_RELEASE_ID}',
        f'test_content: {test_content}',
        f'args.test_argument_key_01: {args.test_argument_key_01}',
        f'args.test_argument_key_02: {args.test_argument_key_02}'
    ]))


