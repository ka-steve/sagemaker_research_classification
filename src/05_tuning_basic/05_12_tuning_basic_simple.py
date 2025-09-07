import argparse
import boto3
import evaluate
import importlib
import json
import os
import pathlib
import requests
import sys
import torch
import transformers
import wandb

import awswrangler as wr
import numpy as np

from botocore.exceptions import ClientError
from datasets import load_dataset, DatasetDict, Dataset
from datetime import datetime, timezone
from sagemaker.huggingface.processing import HuggingFaceProcessor
from sagemaker.sklearn.processing import SKLearnProcessor
from sagemaker.processing import FrameworkProcessor
from sagemaker.sklearn.estimator import SKLearn
from sagemaker.workflow.steps import ProcessingStep
from sagemaker.workflow.pipeline_context import PipelineSession
from sagemaker.processing import ProcessingInput, ProcessingOutput
from sagemaker.session import get_execution_role
from sklearn.metrics import accuracy_score, precision_recall_fscore_support
from tqdm import tqdm
from transformers import (
    AutoTokenizer,
    AutoConfig, 
    AutoModelForSequenceClassification,
    DataCollatorWithPadding,
    PrinterCallback,
    TrainingArguments,
    Trainer
)
from typing import List, Union, Optional

##########################################################################################

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

print('transformers.__version__', transformers.__version__)

##########################################################################################

time_logger = utils.TimeLogger()
wandb_api_key = utils.get_secret(region_name=config.AWS_REGION, secret_name='WeightsAndBiases')['api_key']
wandb.login(key=wandb_api_key)
boto3.setup_default_session(region_name=config.AWS_REGION)


##########################################################################################

parser = argparse.ArgumentParser()
parser.add_argument('--runtype', type=str, default='dev')
parser.add_argument('--instance_type', type=str)

parser.add_argument('--model_name', type=str, default='distilbert-base-uncased')
parser.add_argument('--hf_dataset_suffix', type=str, default='_Title_SubfieldIndex')
parser.add_argument('--label_type', type=str, default='subfield')
parser.add_argument('--text_key', type=str, default='title')
parser.add_argument('--text_key_rename_to', type=str, default='text')
parser.add_argument('--label_key_rename_to', type=str, default='label')
parser.add_argument('--sample', type=int, default=1)

parser.add_argument('--epochs', type=int, default=5)
parser.add_argument('--train_batch_size', type=int, default=32)
parser.add_argument('--eval_batch_size', type=int, default=64)
parser.add_argument('--warmup_steps', type=int, default=500)
parser.add_argument('--learning_rate', type=str, default=5e-5)

# Data, model, and output directories
SM_OUTPUT_DATA_DIR = os.environ['SM_OUTPUT_DATA_DIR'] if 'SM_OUTPUT_DATA_DIR' in os.environ else 'SM_OUTPUT_DATA_DIR'
parser.add_argument('--output_data_dir', type=str, default=SM_OUTPUT_DATA_DIR) # )
SM_MODEL_DIR = os.environ['SM_MODEL_DIR'] if 'SM_MODEL_DIR' in os.environ else 'SM_MODEL_DIR'
parser.add_argument('--model_dir', type=str, default=SM_MODEL_DIR) # )
SM_NUM_GPUS = os.environ['SM_NUM_GPUS'] if 'SM_NUM_GPUS' in os.environ else torch.cuda.device_count()
parser.add_argument('--n_gpus', type=str, default=SM_NUM_GPUS) # 

args, _ = parser.parse_known_args()
LABEL_KEY = f'{args.label_type}_index'
SAMPLE_SUFFIX = f'[:{args.sample}%]' if args.sample!=100 else ''
RUNTYPE = args.runtype

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


# set the wandb project where this run will be logged
os.environ['WANDB_PROJECT']='research_methodology_extraction'
# save your trained model checkpoint to wandb
os.environ['WANDB_LOG_MODEL']='checkpoint'
# turn off watch to log faster
os.environ['WANDB_WATCH']='false'


time_logger.log('Processed arguments')

##########################################################################################

dataset_train = load_dataset(
    'SteveAKopias/SemanticScholarCSFullTextWithOpenAlexTopics'+args.hf_dataset_suffix, 
    split=f'train{SAMPLE_SUFFIX}' # [:1%]
)
dataset_test = load_dataset(
    'SteveAKopias/SemanticScholarCSFullTextWithOpenAlexTopics'+args.hf_dataset_suffix, 
    split=f'test{SAMPLE_SUFFIX}' # [:1%]
)
dataset = DatasetDict({
    'train': dataset_train,
    'test': dataset_test
})
dataset = dataset.rename_column(args.text_key, args.text_key_rename_to)
dataset = dataset.rename_column(LABEL_KEY, args.label_key_rename_to)

time_logger.log('Dataset loaded')

##########################################################################################

label_df = wr.athena.read_sql_query(
f"""
SELECT 
    {args.label_type}_index AS index, 
    {args.label_type}_display_name AS display_name
FROM
    {args.label_type}s
""", '03_core'
)
index2label = dict(zip(label_df[f'index'].astype(int), label_df['display_name']))
label2index = dict(zip(label_df['display_name'], label_df['index'].astype(int)))

time_logger.log('Labels loaded')

##########################################################################################

model = AutoModelForSequenceClassification.from_pretrained(
    args.model_name,
    num_labels=label_df.shape[0],
    id2label=index2label,
    label2id=label2index
)

time_logger.log('Model initialized')

##########################################################################################

tokenizer = AutoTokenizer.from_pretrained(args.model_name, add_prefix_space=True)

def tokenize_function(example):
    text = example[args.text_key_rename_to]
    print(type(text), text[0:5])
    tokenizer.truncation_side = 'right'
    tokenized_inputs = tokenizer(
        text,
        return_tensors='np',
        truncation=True,
        padding=True,
        max_length=512
    )

    return tokenized_inputs


tokenized_dataset = dataset.map(tokenize_function, batched=True)
time_logger.log('Dataset tokenized')

##########################################################################################

data_collator = DataCollatorWithPadding(tokenizer=tokenizer)

def compute_metrics(pred):
    labels = pred.label_ids
    predictions = pred.predictions.argmax(-1)
    precision, recall, f1, _ = precision_recall_fscore_support(labels, predictions, average='micro') # TODO: weighted when not using 1% sample that skips some labels
    accuracy = accuracy_score(labels, predictions)
    return {
        'accuracy': accuracy, 
        'f1': f1,
        'precision': precision,
        'recall': recall
    }

now = datetime.now().strftime('%Y%m%d%H%M%S')
training_args = TrainingArguments(
    run_name=f'{args.model_name}-{args.hf_dataset_suffix}-{now}_sample-{args.sample}_epochs-{args.epochs}',
    output_dir=args.model_dir,
    num_train_epochs=args.epochs,
    per_device_train_batch_size=args.train_batch_size,
    per_device_eval_batch_size=args.eval_batch_size,
    warmup_steps=args.warmup_steps,
    learning_rate=float(args.learning_rate),

    logging_dir=f'{args.output_data_dir}/logs',
    report_to='wandb',
    # logging_steps=5,
    evaluation_strategy='epoch',
    # eval_strategy='steps',
    # eval_steps=20,
    # max_steps=100,
    # save_steps=100,
    disable_tqdm=True if RUNTYPE == 'prod' else False
)

trainer = Trainer(
    model=model,
    args=training_args,
    compute_metrics=compute_metrics,
    train_dataset=tokenized_dataset['train'],
    eval_dataset=tokenized_dataset['test'],
    tokenizer=tokenizer,
    data_collator=data_collator,
)

time_logger.log('Trainer set up')

##########################################################################################

trainer.train()
time_logger.log('Trained model')

##########################################################################################

tokenized_dataset_predict = tokenized_dataset['test'].select(range(0, 100))
raw_predictions, label_ids, _metrics = trainer.predict(tokenized_dataset_predict)
predictions = np.argmax(raw_predictions, axis=1)
for index, (text, label_id_truth, label_id_pred) in enumerate(zip(tokenized_dataset_predict['text'], tokenized_dataset_predict['label'], predictions)):
    print(f'#{index} | {text} | {index2label[label_id_truth]} ({label_id_truth}) | {index2label[label_id_pred]} ({label_id_pred})')

time_logger.log('Printed sample predictions')

##########################################################################################

wandb.finish()
time_logger.log('W&B closed')

##########################################################################################

time_logger.log('DONE')