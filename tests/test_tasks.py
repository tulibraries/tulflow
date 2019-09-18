"""Tests suite for tulflow tasks (generic Airflow Tasks as Functions)."""
import unittest
import pytest
from unittest.mock import patch
from airflow.contrib.operators.slack_webhook_operator import SlackWebhookOperator
from airflow.operators.http_operator import SimpleHttpOperator
from airflow.hooks.base_hook import BaseHook
from airflow.models import Connection, DAG, TaskInstance
from airflow.utils import timezone
from tulflow.tasks import execute_slackpostonfail, slackpostonsuccess, create_sc_collection, swap_sc_alias, get_solr_url

DEFAULT_DATE = timezone.datetime(2019, 8, 16)


class TestSlackPostSuccess(unittest.TestCase):
    """Test Class for Successes being posted to Slack generic task function tests."""
    @patch.object(BaseHook, "get_connection", return_value=Connection(
        conn_id='AIRFLOW_CONN_SLACK_WEBHOOK',
        conn_type='http',
        host='https://hooks.slack.com/services',
        password='puppies'
        )
    )

    def test_slackpostonsuccess(self, mocker):
        """Test slackpostsuccess operator instance contains expected config values."""
        dag = DAG(dag_id='test_slacksuccess', start_date=DEFAULT_DATE)
        task = SlackWebhookOperator(dag=dag, task_id='test_slacksuccess')
        task_instance = TaskInstance(task=task, execution_date=DEFAULT_DATE)
        task_instance.get_template_context()
        operator = slackpostonsuccess(dag)

        self.assertEqual("AIRFLOW_CONN_SLACK_WEBHOOK", operator.http_conn_id)
        self.assertEqual("slackpostonsuccess", operator.task_id)
        self.assertEqual("puppies", operator.webhook_token)
        self.assertIn(":partygritty:", operator.message)
        self.assertEqual("airflow", operator.username)


class TestSolrCloudTasks(unittest.TestCase):
    """Test Class for SolrCloud Tasks."""


    @patch.object(BaseHook, "get_connection", return_value=Connection(
        conn_id='SOLRCLOUD',
        conn_type='http',
        host='http://localhost',
        password='puppies'
        )
    )
    def test_create_sc_collection(self, mocker):
        """Test create_sc_collection operator instance contains expected config values."""
        dag = DAG(dag_id='test_create_sc_collection', start_date=DEFAULT_DATE)
        operator = create_sc_collection(dag, 'SOLRCLOUD', 'test-collection', '2', 'test-configset')
        task_instance = TaskInstance(task=operator, execution_date=DEFAULT_DATE)

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
        )
    )
    def test_swap_sc_alias(self, mocker):
        """Test swap_sc_alias operator instance contains expected config values."""
        dag = DAG(dag_id='test_swap_sc_alias', start_date=DEFAULT_DATE)
        operator = swap_sc_alias(dag, 'SOLRCLOUD', 'new-collection', 'my-alias')
        task_instance = TaskInstance(task=operator, execution_date=DEFAULT_DATE)

        self.assertEqual("SOLRCLOUD", operator.http_conn_id)
        self.assertEqual("CREATEALIAS", operator.data['action'])
        self.assertEqual("my-alias", operator.data['name'])
        self.assertEqual(["new-collection"], operator.data['collections'])
        self.assertEqual("solr_alias_swap", task_instance.task_id)

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
