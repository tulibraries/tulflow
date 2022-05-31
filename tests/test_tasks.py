"""Tests suite for tulflow tasks (generic Airflow Tasks as Functions)."""
import unittest
from unittest.mock import patch
import pytest
from airflow.providers.slack.operators.slack_webhook import SlackWebhookOperator
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.hooks.base import BaseHook
from airflow.models import Connection, DAG, TaskInstance, DagRun
from airflow.utils import timezone
from tulflow.tasks import create_sc_collection, get_solr_url, refresh_sc_collection_for_alias, swap_sc_alias
from airflow.utils.state import State
from airflow import settings

DEFAULT_DATE = timezone.datetime(2019, 8, 16)


class TestSolrCloudTasks(unittest.TestCase):
    """Test Class for SolrCloud Tasks."""


    @patch.object(BaseHook, "get_connection", return_value=Connection(
        conn_id='SOLRCLOUD',
        conn_type='http',
        host='http://localhost',
        password='puppies'
        ))

    def test_create_sc_collection(self, mocker):
        """Test create_sc_collection operator instance contains expected config values."""
        session = settings.Session()
        dag = DAG(dag_id='test_create_sc_collection', start_date=DEFAULT_DATE)
        with dag:
            dr = dag.create_dagrun(
                run_id="test_existing_value_create", state=State.SUCCESS,
                start_date=DEFAULT_DATE, data_interval=(DEFAULT_DATE, DEFAULT_DATE),
                session=session,
                )
        operator = create_sc_collection(dag, 'SOLRCLOUD', 'test-collection', '2', 'test-configset')
        task_instance = TaskInstance(task=operator, run_id=dr.run_id, state=State.SUCCESS)

        self.assertEqual("SOLRCLOUD", operator.http_conn_id)
        self.assertEqual("CREATE", operator.data['action'])
        self.assertEqual("test-collection", operator.data['name'])
        self.assertEqual("1", operator.data['numShards'])
        self.assertEqual("2", operator.data['replicationFactor'])
        self.assertEqual("test-configset", operator.data['collection.configName'])
        self.assertEqual("create_collection", task_instance.task_id)

    @patch.object(BaseHook, "get_connection", return_value=Connection(
        conn_id='SOLRCLOUD',
        conn_type='http',
        host='http://localhost',
        password='puppies'
        ))

    def test_swap_sc_alias(self, mocker):
        """Test swap_sc_alias operator instance contains expected config values."""
        session = settings.Session()
        dag = DAG(dag_id='test_swap_sc_alias', start_date=DEFAULT_DATE)
        with dag:
            dr = dag.create_dagrun(
                run_id="test_existing_value_swap", state=State.SUCCESS,
                start_date=DEFAULT_DATE, data_interval=(DEFAULT_DATE, DEFAULT_DATE),
                session=session,
                )
        operator = swap_sc_alias(dag, 'SOLRCLOUD', 'new-collection', 'my-alias')
        task_instance = TaskInstance(task=operator, run_id=dr.run_id, state=State.SUCCESS)

        self.assertEqual("SOLRCLOUD", operator.http_conn_id)
        self.assertEqual("CREATEALIAS", operator.data['action'])
        self.assertEqual("my-alias", operator.data['name'])
        self.assertEqual(["new-collection"], operator.data['collections'])
        self.assertEqual("solr_alias_swap", task_instance.task_id)

    @patch.object(BaseHook, "get_connection", return_value=Connection(
        conn_id='SOLRCLOUD',
        conn_type='http',
        host='http://localhost',
        password='puppies',
        login="wholoves"
        ))

    def test_refresh_sc_collection_for_alias(self, mocker):
        """Test refresh_sc_collection_for_alias task instance contains expected config values."""
        session = settings.Session()
        dag = DAG(dag_id='test_refresh_sc_collection_for_alias', start_date=DEFAULT_DATE)
        with dag:
            dr = dag.create_dagrun(
                run_id="test_existing_value_refresh", state=State.SUCCESS,
                start_date=DEFAULT_DATE, data_interval=(DEFAULT_DATE, DEFAULT_DATE),
                session=session,
                )
        task = refresh_sc_collection_for_alias(dag=dag, sc_conn=mocker, sc_coll_name='my-collection', sc_alias='my-alias', configset="my-configset")
        task_instance = TaskInstance(task=task, run_id=dr.run_id, state=State.SUCCESS)

        self.assertEqual("my-collection", task.op_kwargs['collection'])
        self.assertEqual("my-alias", task.op_kwargs['alias'])
        self.assertEqual("my-configset", task.op_kwargs['configset'])
        self.assertEqual("refresh_sc_collection_for_alias", task_instance.task_id)

class TestTasksGetSolrUrl(unittest.TestCase):
    """Tests for tasks.get_solr_url function."""

    def test_get_solr_url_without_http_in_host(self):
        conn = Connection(host="example.com", port="8983")
        core = "foo"
        self.assertEqual(get_solr_url(conn, core), "http://example.com:8983/solr/foo")

    def test_get_solr_url_with_http_in_host(self):
        conn = Connection(host="https://example.com", port="8983")
        core = "foo"
        self.assertEqual(get_solr_url(conn, core), "https://example.com:8983/solr/foo")

    def test_get_solr_url_without_port(self):
        conn = Connection(host="https://example.com")
        core = "foo"
        self.assertEqual(get_solr_url(conn, core), "https://example.com/solr/foo")
