"""Generic Airflow Task to Post Task Success on Slack."""
from airflow.hooks.base_hook import BaseHook
from airflow.contrib.operators.slack_webhook_operator import SlackWebhookOperator

def task_generic_slackpostsuccess(dag, message='Oh, happy day!'):
    """Task Method to Post Successful DAG or Task Completion on Slack."""
    conn = BaseHook.get_connection('AIRFLOW_CONN_SLACK_WEBHOOK')

    return SlackWebhookOperator(
        task_id="slackpostonsuccess",
        http_conn_id='AIRFLOW_CONN_SLACK_WEBHOOK',
        webhook_token=conn.password,
        message=':partygritty: ' + message,
        username='airflow',
        trigger_rule="all_success",
        dag=dag)
