from airflow import DAG
import pendulum
from datetime import datetime,timedelta
from api.video_stats import get_playlist_id, get_video_ids, extract_video_data, save_to_json
from datawarehouse.dwh import staging_table, core_table
local_tz = pendulum.timezone("America/Mexico_City")

default_args ={
    "owner": "dataengineers",
    "depends_on_past": False,
    "email_on__failure": False,
    "email_on_retry": False,
    "email": "data@engineers.com",
    "max_active_runs": 1,
    "dagrun_timeout": timedelta(hours=1),
    "start_date": datetime(2026,2,19, tzinfo=local_tz),
}

with DAG(
    dag_id = 'update_db',
    default_args = default_args,
    description = 'DAG to process JSON file and insert data into both staging and core schema',
    schedule='0 15 * * *',
    catchup = False
) as dag:
    update_staging = staging_table()
    update_core = core_table()
    
    
    #define depends
    update_staging >> update_core