## Guide to setting up a data pipeline on Airflow for DWH
1. Prerequisite:
- If your computer is running Windows, install Ubuntu to execute the code.
- If you’re on macOS or Linux, this is not necessary.
2. Check whether Python 3.8 or higher is installed by running ```cd ~/dwh_lab```
3. Open a terminal and run (one-time only) ```python3.8 -m venv venv```. 
4. Create a virtual environment ```source venv/bin/activate```.
5. Activate the virtual environment ```pip install requirements.txt```.
6. Once inside the virtual environment, install dependencies
``` 
    export AIRFLOW_HOME=~/dwh_lab
    export AIRFLOW__CORE__DAGS_FOLDER=$AIRFLOW_HOME/dags
    export AIRFLOW__DATABASE__SQL_ALCHEMY_CONN=sqlite:////root/dwh_lab/airflow.db
```
7. Run the following in your terminal to configure Airflow and start the scheduler ```airflow scheduler```.
8. In another terminal tab, after re-activating the virtual environment (step 4), create an Airflow admin user
```
airflow users create \
  --username admin \
  --password admin \
  --firstname admin \
  --lastname admin \
  --role Admin \
  --email your@email.com
```
9. Start the Airflow webserver ```airflow webserver```.
10. Open your browser [localhost](http://localhost:8080/) and login with ```admin/admin```.