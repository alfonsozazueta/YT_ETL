from airflow import DAG
import pendulum
from datetime import datetime,timedelta
from api.video_stats import get_playlist_id, get_video_ids, extract_video_data, save_to_json

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
    dag_id = 'produce_json',
    default_args = default_args,
    description = 'DAG to produce json file with raw data',
    schedule='0 14 * * *',
    catchup = False
) as dag:
    
    playlist_id = get_playlist_id()
    video_ids = get_video_ids(playlist_id)
    extract_data = extract_video_data(video_ids)
    save_to_json_task = save_to_json(extract_data)
    
    #define depends
    playlist_id >> video_ids >> extract_data >> save_to_json_task