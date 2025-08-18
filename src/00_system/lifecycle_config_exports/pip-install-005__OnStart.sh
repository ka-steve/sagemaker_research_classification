#!/bin/bash

set -e

sudo -u ec2-user -i <<EOF

# PARAMETERS
ENVIRONMENT=pytorch_p310

# source /home/ec2-user/anaconda3/bin/activate "$ENVIRONMENT"
source /home/ec2-user/anaconda3/bin/activate "$ENVIRONMENT"

# pip install "jupytext" "sagemaker==2.250.0" "transformers==4.55.2" "datasets==4.0.0" "accelerate==1.10.0" 
# pip install /home/ec2-user/SageMaker/research_methodology_extraction
pip install -r /home/ec2-user/SageMaker/research_methodology_extraction/requirements.txt

# source /home/ec2-user/anaconda3/bin/deactivate
conda deactivate

EOF