"""Generic Airflow Tasks Functions, Abstracted for Reuse."""
from airflow.hooks.base_hook import BaseHook
from airflow.contrib.operators.slack_webhook_operator import SlackWebhookOperator

def slackpostonfail(context, message=None):
    """Task Method to Post Failed DAG or Task Completion on Slack."""
    conn = BaseHook.get_connection('AIRFLOW_CONN_SLACK_WEBHOOK')
    ti = context.get('task_instance')
    log_url = ti.log_url
    task_id = ti.task_id
    dag_id = ti.dag_id
    task_date = context.get('execution_date')
    if not message:
        message = "Task failed: {} {} {} {}".format(dag_id, task_id, task_date, log_url)

    slack_post = SlackWebhookOperator(
        task_id='slackpostonfail',
        http_conn_id='AIRFLOW_CONN_SLACK_WEBHOOK',
        webhook_token=conn.password,
        message=":poop: " + message,
        username='airflow',
        dag=context.get('dag')
        )

    return slack_post

def slackpostonsuccess(dag, message='Oh, happy day!'):
    """Task Method to Post Successful DAG or Task Completion on Slack."""
    conn = BaseHook.get_connection('AIRFLOW_CONN_SLACK_WEBHOOK')

    slack_post = SlackWebhookOperator(
        task_id="slackpostonsuccess",
        http_conn_id='AIRFLOW_CONN_SLACK_WEBHOOK',
        webhook_token=conn.password,
        message=':partygritty: ' + message,
        username='airflow',
        trigger_rule="all_success",
        dag=dag)

    return slack_post
