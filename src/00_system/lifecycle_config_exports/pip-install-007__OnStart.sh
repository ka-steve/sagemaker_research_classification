#!/bin/bash

set -e

echo "XXXXXX start_notebook START XXXXXXXXXXXXXXXXXX"

sudo -u ec2-user -i <<EOF

echo ":::::: start_notebook SUDO 1 START ::::::::::::::::::::::::"

echo ":: source /home/ec2-user/anaconda3/bin/activate /home/ec2-user/anaconda3/envs/pytorch_p310"
source /home/ec2-user/anaconda3/bin/activate /home/ec2-user/anaconda3/envs/pytorch_p310

echo ":: pip install -r /home/ec2-user/SageMaker/research_methodology_extraction/requirements.txt"
pip install -r /home/ec2-user/SageMaker/research_methodology_extraction/requirements.txt

echo ":: conda deactivate"
conda deactivate

echo ":::::: start_notebook SUDO 1 END ::::::::::::::::::::::::"

EOF

sudo -u ec2-user -i <<EOF

echo ":::::: start_notebook SUDO 2 START ::::::::::::::::::::::::"
echo ":: source /home/ec2-user/anaconda3/bin/activate /home/ec2-user/anaconda3/envs/JupyterSystemEnv"
source /home/ec2-user/anaconda3/bin/activate /home/ec2-user/anaconda3/envs/JupyterSystemEnv

echo ":: pip install jupytext --upgrade"
pip install jupytext --upgrade


echo ":: sudo systemctl restart jupyter-server"
sudo systemctl restart jupyter-server
echo ":::::: start_notebook SUDO 2 END ::::::::::::::::::::::::"

EOF

echo "XXXXXX start_notebook END XXXXXXXXXXXXXXXXXX"