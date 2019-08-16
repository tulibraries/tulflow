"""Tests suite for tulflow tasks (generic Airflow Tasks as Functions)."""
import unittest
import pytest
from unittest.mock import patch
from airflow.contrib.operators.slack_webhook_operator import SlackWebhookOperator
from airflow.hooks.base_hook import BaseHook
from airflow.models import Connection, DAG, TaskInstance
from airflow.utils import timezone
from tulflow.tasks import slackpostonfail, slackpostonsuccess

DEFAULT_DATE = timezone.datetime(2019, 8, 16)

class TestSlackPostFail(unittest.TestCase):
    """Test Class for Failures being posted to Slack generic task function tests."""
    @patch.object(BaseHook, "get_connection", return_value=Connection(
        conn_id='AIRFLOW_CONN_SLACK_WEBHOOK',
        conn_type='http',
        host='https://hooks.slack.com/services',
        password='kittens'
        )
    )

    def test_slackpostonfail(self, mocker):
        """Test slackpostfail operator instance contains expected config values."""
        dag = DAG(dag_id='test_slackfail', start_date=DEFAULT_DATE)
        task = SlackWebhookOperator(dag=dag, task_id='test_slackfail')
        ti = TaskInstance(task=task, execution_date=DEFAULT_DATE)
        context = ti.get_template_context()
        operator = slackpostonfail(context)

        self.assertEqual("AIRFLOW_CONN_SLACK_WEBHOOK", operator.http_conn_id)
        self.assertEqual("slackpostonfail", operator.task_id)
        self.assertEqual("kittens", operator.webhook_token)
        self.assertIn(":poop:", operator.message)
        self.assertIn("test_slackfail", operator.message)
        self.assertEqual("airflow", operator.username)

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
        ti = TaskInstance(task=task, execution_date=DEFAULT_DATE)
        context = ti.get_template_context()
        operator = slackpostonsuccess(dag)

        self.assertEqual("AIRFLOW_CONN_SLACK_WEBHOOK", operator.http_conn_id)
        self.assertEqual("slackpostonsuccess", operator.task_id)
        self.assertEqual("puppies", operator.webhook_token)
        self.assertIn(":partygritty:", operator.message)
        self.assertEqual("airflow", operator.username)
