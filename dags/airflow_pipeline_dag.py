from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.version import version
from datetime import datetime, timedelta
import snowflake.snowpark as snp
import json, getpass, uuid
from airflow.decorators import dag, task
from citibike_ml.ingest import incremental_elt
from citibike_ml.mlops_pipeline import deploy_pred_train_udf
from citibike_ml.mlops_pipeline import materialize_holiday_weather
from citibike_ml.mlops_pipeline import generate_feature_views
from citibike_ml.mlops_pipeline import train_predict_feature_views
from citibike_ml.model_eval import deploy_eval_udf
from citibike_ml.model_eval import evaluate_station_predictions
from snowpark_connection import snowpark_connect
import snowflake.snowpark.functions as F

# def snowpark_connect():
#     with open('/usr/local/airflow/include/creds.json') as f:
#         data = json.load(f)
#     connection_parameters = {
#     'account': data['account'],
#     'user': data['username'],
#     'password': data['password'], #getpass.getpass(),
#     'role': data['role'],
#     'warehouse': data['warehouse']}

#     session = snp.Session.builder.configs(connection_parameters).create()
#     return session

# def _snowpark_database_setup():
#     session = snowpark_connect()
#     project_db_name = 'CITIBIKEML_JF'
#     project_schema_name = 'DEMO'
#     project_db_schema = str(project_db_name)+'.'+str(project_schema_name)
#     top_n = 2
#     model_id = str(uuid.uuid1()).replace('-', '_')
#     download_base_url = 'https://s3.amazonaws.com/tripdata/'
#     load_table_name = str(project_db_schema)+'.'+'RAW_'
#     trips_table_name = str(project_db_schema)+'.'+'TRIPS'
#     holiday_table_name = str(project_db_schema)+'.'+'HOLIDAYS'
#     precip_table_name = str(project_db_schema)+'.'+'WEATHER'
#     model_stage_name = str(project_db_schema)+'.'+'model_stage'
#     clone_table_name = str(project_db_schema)+'.'+'CLONE_'+str(model_id)
#     feature_view_name = str(project_db_schema)+'.'+'STATION_<station_id>_VIEW_'+str(model_id)
#     pred_table_name = str(project_db_schema)+'.'+'PREDICTIONS_'+str(model_id)
#     eval_table_name = str(project_db_schema)+'.'+'EVAL_'+str(model_id)
#     load_stage_name = 'load_stage'
#     _ = session.sql('USE DATABASE ' + str(project_db_name)).collect()
#     _ = session.sql('USE SCHEMA ' + str(project_schema_name)).collect()
#     _ = session.sql('CREATE STAGE IF NOT EXISTS ' + str(model_stage_name)).collect()
#     #_ = session.sql('CREATE OR REPLACE TEMPORARY STAGE ' + str(load_stage_name)).collect()
#     _ = session.sql('CREATE OR REPLACE STAGE ' + str(load_stage_name)).collect()
#     _ = session.sql('CREATE OR REPLACE TABLE '+str(clone_table_name)+" CLONE "+str(trips_table_name)).collect()
#     _ = session.sql('CREATE TAG IF NOT EXISTS model_id_tag').collect()
#     _ = session.sql("ALTER TABLE "+str(clone_table_name)+" SET TAG model_id_tag = '"+str(model_id)+"'").collect()
#     _ = session.sql('DROP TABLE IF EXISTS '+pred_table_name).collect()
#     _ = session.sql('DROP TABLE IF EXISTS '+eval_table_name).collect()
#     session.close()

# def _incremental_elt_task():
#     session = snowpark_connect()
#     file_name_end2 = '202102-citibike-tripdata.csv.zip'
#     file_name_end1 = '201402-citibike-tripdata.zip'
#     files_to_download = [file_name_end1, file_name_end2]
    # trips_table_name = incremental_elt(session=session, 
    #                                 load_stage_name=load_stage_name, 
    #                                 files_to_download=files_to_download, 
    #                                 download_base_url=download_base_url, 
    #                                 load_table_name=load_table_name, 
    #                                 trips_table_name=trips_table_name)

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

local_airflow_path = '/usr/local/airflow/'

# def snowpark_connect():
#     with open('{}{}creds.json'.format(local_airflow_path,"include/")) as f:
#         data = json.load(f)
#     connection_parameters = {
#     'account': data['account'],
#     'user': data['username'],
#     'password': data['password'], #getpass.getpass(),
#     'role': data['role'],
#     'warehouse': data['warehouse'],
#     'database': data['database'],
#     'schema': data['schema']}
#     session = snp.Session.builder.configs(connection_parameters).create()
#     return session

# Using a DAG context manager, you don't have to specify the dag property of each task
@dag(default_args=default_args, schedule_interval=None, start_date=datetime(2022, 1, 24), catchup=False, tags=['test'])
def snowpark_citibike_ml_taskflow_2():
    """
    End to end Astronomer / Snowflake ML Demo
    """

    import uuid
    
    state_dict = {
    "download_base_url":"https://s3.amazonaws.com/tripdata/",
    "load_table_name":"RAW_",
    "trips_table_name":"TRIPS",
    "load_stage_name":"LOAD_STAGE",
    "model_stage_name":"MODEL_STAGE",
    "holiday_table_name":"HOLIDAYS",
    "precip_table_name":"WEATHER",
    "model_udf_name":"STATION_TRAIN_PREDICT_UDF",
    "model_id": str(uuid.uuid1()).replace('-', '_')
    }
    
    @task()
    def snowpark_database_setup(state_dict_snowpark_database_setup:dict): 
        session,_ = snowpark_connect(creds_file='/usr/local/airflow/include/creds.json')
        start_date, end_date = session.table(state_dict['trips_table_name']) \
                              .select(F.min('STARTTIME'), F.max('STARTTIME')).collect()[0][0:2]
        state_dict_snowpark_database_setup.update({"start_date":start_date.strftime("%m/%d/%Y-%H:%M:%S")})
        state_dict_snowpark_database_setup.update({"end_date":end_date.strftime("%m/%d/%Y-%H:%M:%S")})
        
        _ = session.sql('CREATE STAGE IF NOT EXISTS ' + str(state_dict_snowpark_database_setup['model_stage_name'])).collect()
        _ = session.sql('CREATE STAGE IF NOT EXISTS ' + str(state_dict_snowpark_database_setup['load_stage_name'])).collect()
        
        session.close()

        return state_dict_snowpark_database_setup
    
    @task()
    def  incremental_elt_task(state_dict_incremental_elt_task: dict, files_to_download:list)-> dict:
        from ingest import incremental_elt
        session,_ = snowpark_connect(creds_file='/usr/local/airflow/include/creds.json')
        
        _ = incremental_elt(session=session, 
                            load_stage_name=state_dict_incremental_elt_task['load_stage_name'], 
                            files_to_download=files_to_download, 
                            download_base_url=state_dict_incremental_elt_task['download_base_url'], 
                            load_table_name=state_dict_incremental_elt_task['load_table_name'], 
                            trips_table_name=state_dict_incremental_elt_task['trips_table_name']
                            )
        
        session.close()
        return state_dict_incremental_elt_task
    
    @task()
    def deploy_model_udf_task(state_dict_deploy_model_udf_task:dict)-> dict:
        from mlops_pipeline import deploy_pred_train_udf
        
        session,_ = snowpark_connect(creds_file='/usr/local/airflow/include/creds.json')
        model_udf_name = deploy_pred_train_udf(session=session,
                                            #start_time=datetime.strptime(state_dict_deploy_model_udf_task["start_time"], '%m/%d/%Y-%H:%M:%S'),
                                               #function_name='station_train_predict_func', 
                                               model_stage_name=state_dict_deploy_model_udf_task['model_stage_name']
                                              )
                
        state_dict_deploy_model_udf_task.update({"model_udf_name":model_udf_name})

        session.close()
        return state_dict_deploy_model_udf_task

    @task()
    def materialize_holiday_task(state_dict_materialize_holiday_task: dict)-> dict:
        from mlops_pipeline import materialize_holiday_table
        
        session,_ = snowpark_connect(creds_file='/usr/local/airflow/include/creds.json')
        
        holiday_table_name = materialize_holiday_table(session=session,
                                                        start_date=datetime.strptime(state_dict_materialize_holiday_task["start_date"], '%m/%d/%Y-%H:%M:%S'),
                                                        end_date="not needed",
                                                       #trips_table_name=state_dict['trips_table_name'], 
                                                       holiday_table_name='holidays'
                                                      )
        
        state_dict_materialize_holiday_task.update({"holiday_table_name":holiday_table_name})

        session.close()
        return state_dict_materialize_holiday_task

    @task()
    def materialize_precip_task(state_dict_materialize_precip_task: dict)-> dict:
        from mlops_pipeline import materialize_precip_table

        session,_ = snowpark_connect(creds_file='/usr/local/airflow/include/creds.json')
        
        precip_table_name = materialize_precip_table(
            session=session,
            start_date=datetime.strptime(state_dict_materialize_precip_task["start_date"], '%m/%d/%Y-%H:%M:%S'),
            end_date="not needed",
            #trips_table_name=state_dict['trips_table_name'], 
            precip_table_name='weather'
        )
        
        state_dict_materialize_precip_task.update({"precip_table_name":precip_table_name})

        session.close()
        return state_dict_materialize_precip_task

    @task()
    def generate_feature_table_task(state_dict_generate_feature_table_task:dict, top_n:int,state_dict_materialize_holiday_task,state_dict_materialize_precip_task)-> dict: 
        from parallel_udf import generate_feature_table
        
        session,_ = snowpark_connect(creds_file='/usr/local/airflow/include/creds.json')
        
        clone_table_name = 'TRIPS_CLONE_'+state_dict_generate_feature_table_task["model_id"]
        state_dict_generate_feature_table_task.update({"clone_table_name":clone_table_name})
        
        _ = session.sql('CREATE OR REPLACE TABLE '+clone_table_name+" CLONE "+state_dict_generate_feature_table_task["trips_table_name"]).collect()
        _ = session.sql('CREATE TAG IF NOT EXISTS model_id_tag').collect()
        _ = session.sql("ALTER TABLE "+clone_table_name+" SET TAG model_id_tag = '"+state_dict_generate_feature_table_task["model_id"]+"'").collect()
        
        feature_table_name = generate_feature_table(session=session, 
                                                    clone_table_name=state_dict_generate_feature_table_task["clone_table_name"], 
                                                    feature_table_name='TRIPS_FEATURES_'+state_dict_generate_feature_table_task["model_id"], 
                                                    holiday_table_name=state_dict_generate_feature_table_task["holiday_table_name"],
                                                    precip_table_name=state_dict_generate_feature_table_task["precip_table_name"],
                                                    #target_column='COUNT', 
                                                    #top_n=top_n
                                                   )
        state_dict_generate_feature_table_task.update({"feature_table_name":feature_table_name})

        session.close()
        return state_dict_generate_feature_table_task
    
    @task()
    def bulk_train_predict_task(state_dict_generate_feature_table_task:dict)-> dict: 
        from parallel_udf import train_predict_feature_table
        
        session, compute_parameters = snowpark_connect(creds_file='/usr/local/airflow/include/creds.json')
        
        _ = session.sql('USE WAREHOUSE '+compute_parameters['fe_warehouse']).collect()

        pred_table_name = train_predict_feature_table(session=session,
 station_train_pred_udf_name=state_dict_generate_feature_table_task["model_udf_name"], 
 feature_table_name=state_dict_generate_feature_table_task["feature_table_name"],
pred_table_name='PRED_'+state_dict_generate_feature_table_task["model_id"]
                                                     )
        
        state_dict_generate_feature_table_task.update({"pred_table_name":pred_table_name})

        session.close()
        return state_dict_generate_feature_table_task

    @task()
    def deploy_eval_udf_task(state_dict_deploy_eval_udf_task:dict)-> dict:
        from model_eval import deploy_eval_udf
        
        session,_ = snowpark_connect(creds_file='/usr/local/airflow/include/creds.json')
        eval_model_udf_name = deploy_eval_udf(session=session, 
                                              model_stage_name=state_dict_deploy_eval_udf_task['model_stage_name']
                                              )
                
        state_dict_deploy_eval_udf_task.update({"eval_model_udf_name":eval_model_udf_name})

        session.close()
        return state_dict_deploy_eval_udf_task

    @task()
    def eval_station_preds_task(state_dict_bulk_train_predict_task:dict, state_dict_deploy_eval_udf_task:dict)-> dict:
        from model_eval import evaluate_station_predictions
        
        state_dict = state_dict_bulk_train_predict_task.copy()
        state_dict.update({"eval_model_udf_name":state_dict_deploy_eval_udf_task['eval_model_udf_name']})

        session,_ = snowpark_connect(creds_file='/usr/local/airflow/include/creds.json')
        eval_table_name = evaluate_station_predictions(session=session, 
                                                       pred_table_name=state_dict['pred_table_name'],
                                                       eval_model_udf_name=state_dict['eval_model_udf_name'],
                                                       eval_table_name='EVAL_'+state_dict["model_id"]
                                                       )
        state_dict.update({"eval_table_name":eval_table_name})

        session.close()
        return state_dict                                               
    
    #Task order
    state_dict_snowpark_database_setup = snowpark_database_setup(state_dict)

    #state_dict_incremental_elt_task = incremental_elt_task(state_dict_snowpark_database_setup, files_to_download)

    state_dict_materialize_holiday_task = materialize_holiday_task(state_dict_snowpark_database_setup)

    state_dict_materialize_precip_task = materialize_precip_task(state_dict_snowpark_database_setup)

    state_dict_deploy_model_udf_task = deploy_model_udf_task(state_dict_snowpark_database_setup)

    state_dict_deploy_eval_udf_task = deploy_eval_udf_task(state_dict_snowpark_database_setup)

    state_dict_generate_feature_table_task = generate_feature_table_task(state_dict_deploy_model_udf_task, top_n,state_dict_materialize_holiday_task,state_dict_materialize_precip_task)

    state_dict_bulk_train_predict_task = bulk_train_predict_task(state_dict_generate_feature_table_task)

    state_dict_eval_station_preds_task = eval_station_preds_task(state_dict_bulk_train_predict_task, state_dict_deploy_eval_udf_task)        
    
    #return state_dict
files_to_download = ['202003-citibike-tripdata.csv.zip']    
top_n = 5
run_tasks = snowpark_citibike_ml_taskflow_2()

