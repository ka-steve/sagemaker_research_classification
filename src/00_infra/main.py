#!/usr/bin/env python
from constructs import Construct
from cdktf import App, TerraformStack, TerraformOutput
from imports.aws.provider import AwsProvider
from imports.aws import s3_bucket, sagemaker_notebook_instance, sagemaker_notebook_instance_lifecycle_configuration, iam_role, iam_role_policy_attachment, sagemaker_model

class SageMakerStack(TerraformStack):
    def __init__(self, scope: Construct, id: str):
        super().__init__(scope, id)

        AwsProvider(self, "AWS", region="us-east-1")

        # --- 1. Create S3 Bucket ---
        data_bucket = s3_bucket.S3Bucket(self, "DataBucket",
            bucket="steve-sagemaker-data-bucket"
        )

        # --- 2. Create IAM Role for SageMaker ---
        sagemaker_role = iam_role.IamRole(self, "SageMakerExecutionRole",
            name="SageMakerExecutionRole",
            assume_role_policy='''{
                "Version": "2012-10-17",
                "Statement": [
                    {
                        "Effect": "Allow",
                        "Principal": {
                            "Service": "sagemaker.amazonaws.com"
                        },
                        "Action": "sts:AssumeRole"
                    }
                ]
            }'''
        )

        # Attach basic policies
        iam_role_policy_attachment.IamRolePolicyAttachment(self, "S3Access",
            role=sagemaker_role.name,
            policy_arn="arn:aws:iam::aws:policy/AmazonS3FullAccess"
        )

        iam_role_policy_attachment.IamRolePolicyAttachment(self, "SageMakerFullAccess",
            role=sagemaker_role.name,
            policy_arn="arn:aws:iam::aws:policy/AmazonSageMakerFullAccess"
        )

        # --- 3. Define SageMaker Model ---
        # model_artifact_s3_path = "s3://steve-sagemaker-data-bucket/model/output/model.tar.gz"

        # sagemaker_model.SagemakerModel(self, "BertModel",
        #     name="bert-text-classification-model",
        #     execution_role_arn=sagemaker_role.arn,
        #     primary_container={
        #         "image": "763104351884.dkr.ecr.us-east-1.amazonaws.com/huggingface-pytorch-inference:1.13.1-transformers4.26.0-cpu-py39-ubuntu20.04",
        #         "model_data_url": model_artifact_s3_path
        #     }
        # )

        # # --- 4. SageMaker Training Job (BERT example) ---

        # sagemaker_training_job.SagemakerTrainingJob(self, "BertTrainingJob",
        #     name="bert-text-classification-training-job",
        #     role_arn=sagemaker_role.arn,
        #     algorithm_specification={
        #     "training_image": "763104351884.dkr.ecr.us-east-1.amazonaws.com/huggingface-pytorch-training:1.13.1-transformers4.26.0-cpu-py39-ubuntu20.04",
        #     "training_input_mode": "File"
        #     },
        #     input_data_config=[{
        #     "channel_name": "train",
        #     "data_source": {
        #         "s3_data_source": {
        #         "s3_data_type": "S3Prefix",
        #         "s3_uri": "s3://steve-sagemaker-data-bucket/data/train",
        #         "s3_data_distribution_type": "FullyReplicated"
        #         }
        #     },
        #     "content_type": "text/csv"
        #     }],
        #     output_data_config={
        #     "s3_output_path": "s3://steve-sagemaker-data-bucket/model/output"
        #     },
        #     resource_config={
        #     "instance_type": "ml.m5.large",
        #     "instance_count": 1,
        #     "volume_size_in_gb": 10
        #     },
        #     stopping_condition={
        #     "max_runtime_in_seconds": 3600
        #     },
        #     hyper_parameters={
        #     "epochs": "1",
        #     "per_device_train_batch_size": "8",
        #     "model_name_or_path": "bert-base-uncased",
        #     "output_dir": "/opt/ml/model"
        #     }
        # )

        # --- Outputs ---
        TerraformOutput(self, "BucketName", value=data_bucket.bucket)
        TerraformOutput(self, "RoleArn", value=sagemaker_role.arn)

app = App()
SageMakerStack(app, "sagemaker-cdktf")
app.synth()
