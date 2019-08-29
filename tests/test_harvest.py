"""Tests suite for tulflow harvest (Functions for harvesting OAI in Airflow Tasks)."""
import unittest
from unittest.mock import patch
import hashlib
from datetime import datetime
from airflow.hooks.S3_hook import S3Hook
from tulflow.harvest import dag_s3_prefix, dag_write_string_to_s3


class TestDagS3Interaction(unittest.TestCase):
    @classmethod
    def setUpClass(self):
        self.dag_id = 's3_stuff'

    def test_dag_s3_prefix(self):
        timestamp = datetime.now().strftime('%Y-%m-%dT%H:%M:%SZ')
        prefix = dag_s3_prefix(self.dag_id, timestamp)
        self.assertEqual(prefix, "{}/{}".format(self.dag_id, timestamp))

    @patch.object(S3Hook, 'load_string')
    def test_write_push_string_to_s3(self, mock):
        string = b"<fooooooooo>"
        prefix = "this/thing/here"
        bucket_name = "my-bucket"
        hash = hashlib.md5(string).hexdigest()
        key = "{}/{}".format(prefix,hash)

        dag_write_string_to_s3(string=string, prefix=prefix, s3_conn="s3_conn", bucket_name=bucket_name)
        mock.assert_called_once_with(string, key , bucket_name=bucket_name)


        
