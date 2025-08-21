# conda create -n python_311 python=3.11.13 -y
source activate python_311
# python -m pip install "numpy<2.3.0" ipykernel jupyterlab matplotlib
# python -m ipykernel install --user --name python_311 --display-name python_311
python -m pip install -r /home/sagemaker-user/research_methodology_extraction/requirements.txt
