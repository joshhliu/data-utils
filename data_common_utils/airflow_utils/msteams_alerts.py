"""
A module for mwaa airflow to alert MS Teams.
"""
import requests

def success_callback(context,http_conn):
    payload = {
        "@type": "MessageCard",
        "@context": "http://schema.org/extensions",
        "themeColor": "0076D7",
        "summary": "DBT Success",
        "sections": [{
            "activityTitle": "DAG SUCCESS:",
            "activitySubtitle": context.get('task_instance').dag_id + " @ " + context.get('task_instance').run_id,
            "facts": [
              {
                "name": "Task Id",
                "value": context.get('task_instance').task_id,
                  }
                ],
            "activityImage": "https://adaptivecards.io/content/cats/1.png",
            }]
        }
    headers = {"content-type": "application/json"}
    requests.post(http_conn, json=payload, headers=headers)

def failure_callback(context,http_conn):
    payload = {
        "@type": "MessageCard",
        "@context": "http://schema.org/extensions",
        "themeColor": "0076D7",
        "summary": "DBT Failed",
        "sections": [{
            "activityTitle": "DAG FAILED:",
            "activitySubtitle": context.get('task_instance').dag_id + " @ " + context.get('task_instance').run_id,
            "facts": [
              {
                "name": "Task Id",
                "value": context.get('task_instance').task_id,
                  }
                ],
            "activityImage": "https://adaptivecards.io/content/cats/3.png",
            }]
        }
    headers = {"content-type": "application/json"}
    requests.post(http_conn, json=payload, headers=headers)


"""
Usage:

from data_common_utils.airflow_utils import msteams_alerts

start = DummyOperator(task_id = 'start',
        dag = dag,
        on_success_callback=lambda context: msteams_alerts.success_callback(context, http_conn),
        on_failure_callback=lambda context: msteams_alerts.failure_callback(context, http_conn),
        )
"""