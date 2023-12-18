"""Generic Airflow Tasks Functions, Abstracted for Reuse."""
import re
import pprint
from airflow.hooks.base import BaseHook
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.operators.python import PythonOperator
from tulflow.solr_api_utils import SolrApiUtils

PP = pprint.PrettyPrinter(indent=4)

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
