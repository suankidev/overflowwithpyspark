pip list
pip search pyspark
pip list --outdated

#upgrade
pip install -U pyspark


#show package and version
pip freeze

#install all packages from text file
"""
file.txt --contains
Pymler==0.4
pyspark=3.3.1
"""
pip install -r file.txt



#-----------------virtualenv

pip install virtualenv

mkdir envrionment
cd $!
virtualenv project_env
#activate

source project_env/bin/activate

pip freez --local #only environ list

deactivate

#my current env for this project
conda activate C:\Users\sujee\anaconda3\envs\pyspark_learn_project


#we can specify version of python
virtualenv -p /usr/bin/python2.6 mypyenv


#conda env
conda create --name  your_environment_name

see list of conda environments:
conda env list

activate your environment:
conda activate your_environment_name


conda remove --name myenv  --all


#create env with yaml file

conda create -f pyspark_dev_env.yaml








------------------------------->
setting up script thats run when we activate out pyspark_dev_env
and run when deactivate

cd pyspark_Dev_env
mkdir -p etc/conda/deactivate.d
mkdir -p etc/conda/activate.d

deactivate.d --->  #!/usr/bin/env bash    
                   export db_url='usr@host:2222/dv'

activate.d    ---> unset db_url

-------------------------------->