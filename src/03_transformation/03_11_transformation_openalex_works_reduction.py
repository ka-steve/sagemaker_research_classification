import requests
import os
import sys
import argparse
import pathlib
import boto3
import time
import smart_open
import json
from botocore.exceptions import ClientError
from datetime import datetime, timezone
from tqdm import tqdm
from typing import List, Union, Optional


# Adding ../01_modules or ./01_modules to the system path so that we can load modules from 
# there as well
if '__file__' in globals():
    script_dir = pathlib.Path(__file__).parent.resolve()
else:
    script_dir = pathlib.Path().absolute()
modules_path_in_dev = os.path.abspath(os.path.join(script_dir, '..', '01_modules'))
modules_path_in_prod = os.path.abspath(os.path.join(script_dir, '01_modules'))
if os.path.exists(modules_path_in_dev):
    sys.path.append(modules_path_in_dev)
if os.path.exists(modules_path_in_prod):
    sys.path.append(modules_path_in_prod)

import utils
import config

time_logger = utils.TimeLogger()


parser = argparse.ArgumentParser()
parser.add_argument('--runtype', type=str, default='dev')
parser.add_argument('--file-min-limit', type=int, default=0) # inclusive
parser.add_argument('--file-max-limit', type=int, default=100) # exclusive
args, _ = parser.parse_known_args()
RUNTYPE = args.runtype

print(args.file_min_limit, args.file_max_limit)
print (args)

if RUNTYPE == 'dev':
    # PROCESSING_FILEPATH_PREFIX = '_dev_processing'
    # PROCESSING_FILEPATH_INPUT = os.path.join(script_dir, f'{PROCESSING_FILEPATH_PREFIX}/input/data/')
    # PROCESSING_FILEPATH_OUTPUT = os.path.join(script_dir, f'{PROCESSING_FILEPATH_PREFIX}/output/results/')
    USE_TQDM = True
elif RUNTYPE == 'prod':
    # PROCESSING_FILEPATH_PREFIX = config.DEFAULT_PROCESSING_FILEPATH_PREFIX
    # PROCESSING_FILEPATH_INPUT = f'{PROCESSING_FILEPATH_PREFIX}/input/data/'
    # PROCESSING_FILEPATH_OUTPUT = f'{PROCESSING_FILEPATH_PREFIX}/output/results/'
    USE_TQDM = False
else:
    raise ValueError('Argument --runtype should be either "dev" or "prod" (without quotes).')

time_logger.log('Processed arguments')


file_counter = 0
file_min_limit = args.file_min_limit
file_max_limit = args.file_max_limit
total_record_counter = 0
s3_client = boto3.client('s3')
files = s3_client.list_objects(Bucket=config.DEFAULT_S3_BUCKET_NAME, Prefix='01_data/01_raw/openalex/data/works_unpartitioned/', Delimiter='/', MaxKeys=file_max_limit)

print("len(files['Contents'])", len(files['Contents']))

openalex_works_source_prefix = f'{config.OPENALEX_S3_RAW_DATA_PREFIX}/data/works_unpartitioned/'
openalex_works_source_path = f's3://{config.DEFAULT_S3_BUCKET_NAME}/{openalex_works_source_prefix}'
openalex_works_target_prefix = f'{config.OPENALEX_S3_RAW_DATA_PREFIX}/data/works_reduced/'
openalex_works_target_path = f's3://{config.DEFAULT_S3_BUCKET_NAME}/{openalex_works_target_prefix}'


def reduce_line(source_filepath:str, line_counter:int, line:str)->str:
    try:
        line_dict = json.loads(line)
    except Exception as e:
        print(f'ERROR: Could not parse JSON from [{source_filepath}]:{line_counter}: {line}\n')
        print(e)
        return ''
    else:
        reduced_line = {}
        if 'id' in line_dict:
            reduced_line['id_openalex_long'] = line_dict['id'] # https://openalex.org/W2079861989
            try:
                reduced_line['id_openalex_short'] = line_dict['id'].split('/')[-1][1:] # 2079861989
            except:
                # print(f'Warning: could not split {key} in [{source_filepath}]:{line_counter}: {line}')
                pass
        else:
            print(f'ERROR: Line does not contain ID [{source_filepath}]:{line_counter}: {line}\n')
            return ''
            
        key = 'doi'
        if key in line_dict:
            reduced_line['id_doi_long'] = line_dict[key]
            try:
                reduced_line['id_doi_short'] = None if line_dict[key] is None else '/'.join(line_dict[key].split('/')[3:])
            except:
                # print(f'Warning: could not split {key} in [{source_filepath}]:{line_counter}: {line}')
                pass
            
        key = 'display_name'
        if key in line_dict:
            reduced_line[key] = line_dict[key]
            
        key = 'title'
        if key in line_dict:
            reduced_line[key] = line_dict[key]
            
        key = 'language'
        if key in line_dict:
            reduced_line[key] = line_dict[key]
            
        key = 'publication_year'
        if key in line_dict:
            reduced_line[key] = line_dict[key]
            
        key = 'ids'
        if key in line_dict:
            subkey = 'pmid'
            if (line_dict[key] is not None) and (subkey in line_dict[key]):
                reduced_line[f'id_{subkey}_long'] = line_dict[key][subkey]
                try:
                    reduced_line[f'id_{subkey}_short'] = None if line_dict[key][subkey] is None else '/'.join(line_dict[key][subkey].split('/')[3:])
                except:
                    # print(f'Warning: could not split {key}_{subkey} in [{source_filepath}]:{line_counter}: {line}')
                    pass
            subkey = 'mag'
            if (line_dict[key] is not None) and (subkey in line_dict[key]):
                reduced_line[f'id_{subkey}'] = line_dict[key][subkey]
        
        key = 'type'
        if key in line_dict:
            reduced_line['item_type'] = line_dict[key]

        key = 'primary_topic'
        if key in line_dict:
            subkey = 'id'
            if (line_dict[key] is not None) and (subkey in line_dict[key]):
                reduced_line[f'{key}_long_id'] = line_dict[key][subkey] # https://openalex.org/T10062
                try:
                    reduced_line[f'{key}_short_id'] = None if line_dict[key][subkey] is None else line_dict[key][subkey].split('/')[-1] # T10062
                except:
                    # print(f'Warning: could not split {key}_{subkey} in [{source_filepath}]:{line_counter}: {line}')
                    pass
            subkey = 'display_name'
            if (line_dict[key] is not None) and (subkey in line_dict[key]):
                reduced_line[f'{key}_{subkey}'] = line_dict[key][subkey]
            subkey = 'score'
            if (line_dict[key] is not None) and (subkey in line_dict[key]):
                reduced_line[f'{key}_{subkey}'] = line_dict[key][subkey]
            subkey = 'subfield'
            if (line_dict[key] is not None) and (subkey in line_dict[key]):
                subsubkey = 'id'
                if (line_dict[key][subkey] is not None) and (subsubkey in line_dict[key][subkey]):
                    reduced_line[f'{key}_{subkey}_long_id'] = line_dict[key][subkey][subsubkey] # https://openalex.org/subfields/1306
                    try:
                        reduced_line[f'{key}_{subkey}_short_id'] = None if line_dict[key][subkey][subsubkey] is None else line_dict[key][subkey][subsubkey].split('/')[-1] # 1306
                    except:
                        # print(f'Warning: could not split {key}_{subkey}_{subsubkey} in [{source_filepath}]:{line_counter}: {line}')
                        pass
                subsubkey = 'display_name'
                if (line_dict[key][subkey] is not None) and (subsubkey in line_dict[key][subkey]):
                    reduced_line[f'{key}_{subkey}_{subsubkey}'] = line_dict[key][subkey][subsubkey]
            subkey = 'field'
            if (line_dict[key] is not None) and (subkey in line_dict[key]):
                subsubkey = 'id'
                if (line_dict[key][subkey] is not None) and (subsubkey in line_dict[key][subkey]):
                    reduced_line[f'{key}_{subkey}_long_id'] = line_dict[key][subkey][subsubkey] # https://openalex.org/subfields/1306
                    try:
                        reduced_line[f'{key}_{subkey}_short_id'] = None if line_dict[key][subkey][subsubkey] is None else line_dict[key][subkey][subsubkey].split('/')[-1] # 1306
                    except:
                        # print(f'Warning: could not split {key}_{subkey}_{subsubkey} in [{source_filepath}]:{line_counter}: {line}')
                        pass
                subsubkey = 'display_name'
                if (line_dict[key][subkey] is not None) and (subsubkey in line_dict[key][subkey]):
                    reduced_line[f'{key}_{subkey}_{subsubkey}'] = line_dict[key][subkey][subsubkey]
            subkey = 'domain'
            if (line_dict[key] is not None) and (subkey in line_dict[key]):
                subsubkey = 'id'
                if (line_dict[key][subkey] is not None) and (subsubkey in line_dict[key][subkey]):
                    reduced_line[f'{key}_{subkey}_long_id'] = line_dict[key][subkey][subsubkey] # https://openalex.org/subfields/1306
                    try:
                        reduced_line[f'{key}_{subkey}_short_id'] = None if line_dict[key][subkey][subsubkey] is None else line_dict[key][subkey][subsubkey].split('/')[-1] # 1306
                    except:
                        # print(f'Warning: could not split {key}_{subkey}_{subsubkey} in [{source_filepath}]:{line_counter}: {line}')
                        pass
                subsubkey = 'display_name'
                if (line_dict[key][subkey] is not None) and (subsubkey in line_dict[key][subkey]):
                    reduced_line[f'{key}_{subkey}_{subsubkey}'] = line_dict[key][subkey][subsubkey]
    return json.dumps(reduced_line, default=str)

# max_line_length_index = None
# max_line_length_value = 0
# max_line_length_line = ''

for file_ref in files['Contents']:
    if (file_counter >= file_min_limit) and (file_counter < file_max_limit):
        line_counter = 0
        source_filepath = file_ref['Key']
        target_filepath = source_filepath.replace('works_unpartitioned', 'works_reduced')
        source_filename = source_filepath.split('/')[-1]
        target_file_location = f's3://{config.DEFAULT_S3_BUCKET_NAME}/{target_filepath}'
        with smart_open.open(f's3://{config.DEFAULT_S3_BUCKET_NAME}/{source_filepath}') as file_source:
            with smart_open.open(target_file_location, 'w') as file_target:
                for line in file_source:
                    # if max_line_length_value < len(line):
                    #     max_line_length_index = line_counter
                    #     max_line_length_value = len(line)
                    #     max_line_length_line = line
                    reduced_line = reduce_line(source_filepath, line_counter, line)
                    if reduced_line != '':
                        file_target.write(reduced_line+'\n')
                        pass
    
                    # print(f'{source_filename}:{line_counter:06}|')
                    # print(reduced_line)
                    # print('\n\n')
                    line_counter += 1
                    total_record_counter += 1
        print(f'done: {source_filename}, {line_counter}/{total_record_counter}')
        # print(max_line_length_index, max_line_length_value, max_line_length_line)
    file_counter += 1







time_logger.log('Openalex works dataset reduced')
time_logger.log('DONE')