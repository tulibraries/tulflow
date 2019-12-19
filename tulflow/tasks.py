"""Generic Airflow Tasks Functions, Abstracted for Reuse."""
import re
import pprint
from airflow.hooks.base_hook import BaseHook
from airflow.contrib.operators.slack_webhook_operator import SlackWebhookOperator
from airflow.operators.http_operator import SimpleHttpOperator
from airflow.operators.python_operator import PythonOperator
from tulflow.solr_api_utils import SolrApiUtils

PP = pprint.PrettyPrinter(indent=4)


def execute_slackpostonfail(context, conn_id="AIRFLOW_CONN_SLACK_WEBHOOK", message=None):
    """Task Method to Post Failed DAG or Task Completion on Slack."""
    conn = BaseHook.get_connection(conn_id)
    task_instance = context.get("task_instance")
    log_url = task_instance.log_url
    task_id = task_instance.task_id
    dag_id = task_instance.dag_id
    task_date = context.get("execution_date")
    if not message:
        message = "Task failed: {} {} {} {}".format(dag_id, task_id, task_date, log_url)

    slack_post = SlackWebhookOperator(
        task_id="slackpostonfail",
        http_conn_id=conn_id,
        webhook_token=conn.password,
        message=":poop: " + message,
        username="airflow",
        dag=context.get("dag")
        )

    return slack_post.execute(context=context)


def execute_slackpostonsuccess(context, conn_id="AIRFLOW_CONN_SLACK_WEBHOOK", message=None):
    """Task Method to Post Successful DAG or Task Completion on Slack."""
    conn = BaseHook.get_connection(conn_id)
    task_instance = context.get("task_instance")
    log_url = task_instance.log_url
    task_id = task_instance.task_id
    dag_id = task_instance.dag_id
    task_date = context.get("execution_date")
    if not message:
        message = "DAG success: {} {} {} {}".format(dag_id, task_id, task_date, log_url)

    slack_post = SlackWebhookOperator(
        task_id="slackpostonsuccess",
        http_conn_id=conn_id,
        webhook_token=conn.password,
        message=":partygritty: " + message,
        username="airflow",
        trigger_rule="all_success",
        dag=context.get("dag")
    )

    return slack_post.execute(context=context)


def create_sc_collection(dag, sc_conn_id, sc_coll_name, sc_coll_repl, sc_configset_name):
    """Creates a new SolrCloud Collection."""
    task_instance = SimpleHttpOperator(
        task_id="create_collection",
        method="GET",
        http_conn_id=sc_conn_id,
        endpoint="solr/admin/collections",
        data={
            "action": "CREATE",
            "name": sc_coll_name,
            "numShards": "1",
            "replicationFactor": sc_coll_repl,
            "maxShardsPerNode": "1",
            "collection.configName": sc_configset_name
        },
        headers={},
        dag=dag,
        log_response=True
    )

    return task_instance


def swap_sc_alias(dag, sc_conn_id, sc_coll_name, sc_configset_name):
    """Create or point an existing SolrCloud Alias to an existing SolrCloud Collection."""
    task_instance = SimpleHttpOperator(
        task_id="solr_alias_swap",
        method="GET",
        http_conn_id=sc_conn_id,
        endpoint="solr/admin/collections",
        data={
            "action": "CREATEALIAS",
            "name": sc_configset_name,
            "collections": [sc_coll_name]
            },
        headers={},
        dag=dag,
        log_response=True
    )

    return task_instance

def refresh_sc_collection_for_alias(dag, sc_conn, sc_coll_name, sc_alias, configset,numShards=None, replicationFactor=None, maxShardsPerNode=None):
    """Removes collection from alias, deletes collection, creates new collection & adds to alias"""
    task_instance = PythonOperator(
        task_id="refresh_sc_collection_for_alias",
        python_callable=SolrApiUtils.remove_and_recreate_collection_from_alias,
        op_kwargs={
            "collection": sc_coll_name,
            "configset": configset,
            "alias": sc_alias,
            "solr_url": sc_conn.host,
            "solr_port": sc_conn.port,
            "solr_auth_user": sc_conn.login or "",
            "solr_auth_pass": sc_conn.password or "",
            "numShards": numShards,
            "replicationFactor": replicationFactor,
            "maxShardsPerNode": maxShardsPerNode
        },
        dag=dag
    )
    return task_instance

def get_solr_url(conn, core):
    """  Generates a solr url from  passed in connection and core.

    Parameters:
        conn (airflow.models.connection): Connection object representing solr we index to.
        core (str)  The solr collection or configuration  to use.

    Returns:
        solr_url (str): A solr URL.

    """
    solr_url = conn.host

    if not re.match("^http", solr_url):
        solr_url = "http://" + solr_url

    if conn.port:
        solr_url += ":" + str(conn.port)

    solr_url += "/solr/" + core

    return solr_url

def conditionally_trigger(context, dag_run_obj):
    """This function decides whether or not to Trigger the remote DAG"""
    c_p = context['params']['condition_param']
    print("Controller DAG : conditionally_trigger = {}".format(c_p))
    if context['params']['condition_param']:
        dag_run_obj.payload = context['params']
        PP.pprint(dag_run_obj.payload)
        return dag_run_obj
