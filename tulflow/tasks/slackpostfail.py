"""Generic Airflow Task to Post Task Failure on Slack."""
from airflow.hooks.base_hook import BaseHook
from airflow.contrib.operators.slack_webhook_operator import SlackWebhookOperator

def task_generic_slackpostonfail(context):
    """Task Method to Post Failed DAG or Task Completion on Slack."""
    conn = BaseHook.get_connection('AIRFLOW_CONN_SLACK_WEBHOOK')
    task_instance = context.get('task_instance')
    log_url = task_instance.log_url
    task_id = task_instance.task_id
    dag_id = task_instance.dag_id
    task_date = context.get('execution_date')

    slack_post = SlackWebhookOperator(
        task_id='slackpostonfail',
        http_conn_id='AIRFLOW_CONN_SLACK_WEBHOOK',
        webhook_token=conn.password,
        message=":poop: Task failed: {} {} {} {}".format(dag_id, task_id, task_date, log_url),
        username='airflow',
        dag=context.get('dag'))

    return slack_post.execute(context=context)
