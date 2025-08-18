#!/bin/bash

set -e

echo ":::::: start_notebook START ::::::::::::::::::::::::"

sudo -u ec2-user -i <<EOF

echo ":::::: start_notebook SUDO START ::::::::::::::::::::::::"

echo ":: source /home/ec2-user/anaconda3/bin/activate /home/ec2-user/anaconda3/envs/pytorch_p310"
source /home/ec2-user/anaconda3/bin/activate /home/ec2-user/anaconda3/envs/pytorch_p310

echo ":: pip install -r /home/ec2-user/SageMaker/research_methodology_extraction/requirements.txt"
pip install -r /home/ec2-user/SageMaker/research_methodology_extraction/requirements.txt

echo ":: conda deactivate"
conda deactivate

echo ":::::: start_notebook SUDO END ::::::::::::::::::::::::"

EOF

echo ":::::: start_notebook END ::::::::::::::::::::::::"