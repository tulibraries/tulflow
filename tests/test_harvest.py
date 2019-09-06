"""Tests suite for tulflow harvest (Functions for harvesting OAI in Airflow Tasks)."""
from datetime import datetime
import hashlib
import os
import unittest
from unittest import mock
from unittest.mock import patch
from airflow.hooks.S3_hook import S3Hook
from airflow.models import DAG
from airflow.utils import timezone
from lxml import etree
from sickle.iterator import OAIItemIterator
import httpretty
from tulflow.harvest import dag_s3_prefix, dag_write_string_to_s3, harvest_oai, process_xml, write_log, oai_to_s3
from types import SimpleNamespace

DEFAULT_DATE = timezone.datetime(2019, 8, 16)

lizards = """
<OAI-PMH xmlns="http://www.openarchives.org/OAI/2.0/"
    xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
    xsi:schemaLocation="http://www.openarchives.org/OAI/2.0/ http://www.openarchives.org/OAI/2.0/OAI-PMH.xsd">
    <responseDate>2019-08-30T13:46:14Z</responseDate>
    <request verb="ListRecords" set="dpla_test">http://10.5.0.10/combine/oai</request>
    <ListRecords>
        <record>
            <header>
                <identifier>oai:lizards</identifier>
                <datestamp>2019-08-30T13:45:28Z</datestamp>
                <setSpec>dpla_test</setSpec>
            </header>
            <metadata>
                <oai_dc:dc xmlns:dc="http://purl.org/dc/elements/1.1/"
                    xmlns:dcterms="http://purl.org/dc/terms/"
                    xmlns:oai_dc="http://www.openarchives.org/OAI/2.0/oai_dc/">
                    <dcterms:title>lizards</dcterms:title>
                </oai_dc:dc>
            </metadata>
        </record>
    </ListRecords>
</OAI-PMH>
"""

animals = """
<OAI-PMH xmlns="http://www.openarchives.org/OAI/2.0/"
    xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
    xsi:schemaLocation="http://www.openarchives.org/OAI/2.0/ http://www.openarchives.org/OAI/2.0/OAI-PMH.xsd">
    <responseDate>2019-08-30T13:46:14Z</responseDate>
    <request verb="ListRecords" set="dpla_test">http://10.5.0.10/combine/oai</request>
    <ListRecords>
        <record>
            <header>
                <identifier>oai:cats</identifier>
                <datestamp>2019-08-30T13:45:28Z</datestamp>
                <setSpec>dpla_test</setSpec>
            </header>
            <metadata>
                <oai_dc:dc xmlns:dc="http://purl.org/dc/elements/1.1/"
                    xmlns:dcterms="http://purl.org/dc/terms/"
                    xmlns:oai_dc="http://www.openarchives.org/OAI/2.0/oai_dc/">
                    <dcterms:title>cats</dcterms:title>
                </oai_dc:dc>
            </metadata>
        </record>
        <record>
            <header>
                <identifier>oai:kittens</identifier>
                <datestamp>2019-08-30T13:45:28Z</datestamp>
                <setSpec>dpla_test</setSpec>
            </header>
            <metadata>
                <oai_dc:dc xmlns:dc="http://purl.org/dc/elements/1.1/"
                    xmlns:dcterms="http://purl.org/dc/terms/"
                    xmlns:oai_dc="http://www.openarchives.org/OAI/2.0/oai_dc/">
                    <dcterms:title>kittens</dcterms:title>
                </oai_dc:dc>
            </metadata>
        </record>
    </ListRecords>
</OAI-PMH>
"""


class TestDagS3Interaction(unittest.TestCase):
    """Test Class for S3 Post Wrapper."""
    @classmethod
    def setUpClass(self):
        self.dag_id = 's3_stuff'
        self.maxDiff = None


    def test_dag_s3_prefix(self):
        """Test Creating S3 Bucket ('prefix')."""
        timestamp = datetime.now().strftime('%Y-%m-%dT%H:%M:%SZ')
        prefix = dag_s3_prefix(self.dag_id, timestamp)
        self.assertEqual(prefix, "{}/{}".format(self.dag_id, timestamp))


    @patch.object(S3Hook, 'load_string')
    def test_write_push_string_to_s3(self, mock):
        """Test Writing String to S3 using our Function."""
        string = "<fooooooooo>"
        prefix = "this/thing/here"
        bucket = "my-bucket"
        our_hash = hashlib.md5(string.encode('utf-8')).hexdigest()
        key = "{}/{}".format(prefix, our_hash)
        s3_conn = SimpleNamespace(conn_id="1")


        dag_write_string_to_s3(string=string, prefix=prefix, s3_conn=s3_conn, bucket_name=bucket)
        mock.assert_called_once_with(string, key, bucket_name=bucket)


class TestOAIHarvestInteraction(unittest.TestCase):
    """Test Class for OAI Harvest Wrapper."""
    @classmethod
    def setUpClass(self):
        self.maxDiff = None

    @httpretty.activate
    def test_harvest_oai(self, **kwargs):
        """Test Calling OAI-PMH HTTP Endpoint & Returning XML String."""
        httpretty.register_uri(
            httpretty.GET,
            "http://127.0.0.1/combine/oai",
            body=lizards
        )

        kwargs['oai_endpoint'] = "http://127.0.0.1/combine/oai"
        kwargs['harvest_params'] = {
            'metadataPrefix': 'generic',
            'set': 'dpla_test',
            'from': None,
            'until': None
        }

        response = harvest_oai(**kwargs)
        xml_output = etree.tostring(response.next().xml, pretty_print=True, encoding='utf-8')
        self.assertEqual(type(response), OAIItemIterator)
        self.assertIn(b"<identifier>oai:lizards</identifier>", xml_output)
        self.assertIn(b"<setSpec>dpla_test</setSpec>", xml_output)
        self.assertIn(b"<dcterms:title>lizards</dcterms:title>", xml_output)


    @httpretty.activate
    def test_process_xml(self, **kwargs):
        """Test Calling handling XML Element to String."""
        httpretty.register_uri(
            httpretty.GET,
            "http://test/combine/oai",
            body=animals
        )

        kwargs['oai_endpoint'] = "http://test/combine/oai"
        kwargs['harvest_params'] = {
            'metadataPrefix': 'generic',
            'set': 'dpla_test',
            'from': None,
            'until': None
        }

        response = harvest_oai(**kwargs)
        count = process_xml(response, write_log, **kwargs)
        self.assertEqual(count, 2)


    @mock.patch('tulflow.harvest.harvest_oai')
    @mock.patch('tulflow.harvest.dag_s3_prefix')
    @mock.patch('tulflow.harvest.process_xml')
    def test_oai_to_s3_harvest(self, mock_harvest, mock_prefix, mock_process, **kwargs):
        """Test oai_to_s3 wraps harvest_oai function."""
        dag = DAG(dag_id='test_slacksuccess', start_date=DEFAULT_DATE)
        kwargs['oai_endpoint'] = "http://test/combine/oai"
        kwargs['set'] = "set"
        kwargs['from'] = "from"
        kwargs['until'] = "until"
        kwargs['dag'] = dag
        kwargs['timestamp'] = datetime.now().strftime('%Y-%m-%d_%H-%M-%S')

        oai_to_s3(**kwargs)
        self.assertTrue(mock_harvest.called)
        self.assertTrue(mock_prefix.called)
        self.assertTrue(mock_process.called)
