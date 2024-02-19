"""Tests suite for tulflow harvest (Functions for harvesting OAI in Airflow Tasks)."""
import unittest
import boto3
import httpretty
from lxml import etree
from moto import mock_aws
from tulflow import process

class TestDataProcessInteractions(unittest.TestCase):
    """Test Class for data processing functions."""

    def test_expand_alma_sftp_tarball_pass(self):
        """Test Untarring AlmaSFTP S3 Tarball bytestream to XML."""
        test_key = "almasftp/alma_bibs__new_1.xml.tar.gz"
        test_object = open("tests/fixtures/alma_bibs__new_1.xml.tar.gz", "rb").read()
        test_run = process.expand_alma_sftp_tarball(test_key, test_object)
        self.assertEqual(type(test_run).__name__, "bytes")
        self.assertEqual(test_run, open("tests/fixtures/alma_bibs__new_1.xml", "rb").read())

    def test_expand_alma_sftp_tarball_empty(self):
        """Test Untarring AlmaSFTP S3 Tarball bytestream to XML."""
        test_key = "almasftp/alma_bibs__new_1.xml.tar.gz"
        test_object = open("tests/fixtures/alma_bibs__empty.xml.tar.gz", "rb").read()
        with self.assertLogs("tulflow_process") as log:
            test_run = process.expand_alma_sftp_tarball(test_key, test_object)
        self.assertEqual(test_run, None)
        logs = ["ERROR:tulflow_process:S3 Object is empty.", "ERROR:tulflow_process:" + test_key]
        self.assertEqual(log.output, logs)

    def test_expand_alma_sftp_tarball_multi(self):
        """Test Untarring AlmaSFTP S3 Tarball bytestream to XML."""
        test_key = "almasftp/alma_bibs__new_1.xml.tar.gz"
        test_object = open("tests/fixtures/alma_bibs__multi.xml.tar.gz", "rb").read()
        with self.assertLogs("tulflow_process") as log:
            test_run = process.expand_alma_sftp_tarball(test_key, test_object)
        self.assertEqual(test_run, None)
        logs = [
            "ERROR:tulflow_process:S3 Object has more than 1 member, which is unexpected.",
            "ERROR:tulflow_process:" + test_key
        ]
        self.assertEqual(log.output, logs)

    def test_add_marc21xml_root_namespace(self):
        """Test converting ALMASFTP XML Collection document as bytes
        to lxml.etree.Element with MARC21 as default namespace."""
        test_bytes = open("tests/fixtures/alma_bibs__new_1.xml", "rb").read()
        test_out = etree.fromstring(open("tests/fixtures/alma_bibs__new_1_ns.xml", "rb").read())
        test_run = process.add_marc21xml_root_ns(test_bytes)
        self.assertEqual(etree.tostring(test_run), etree.tostring(test_out))
        self.assertIn("{http://www.loc.gov/MARC21/slim}", test_run.tag)

    def test_add_marc21xml_root_namespace_dup(self):
        """Test converting ALMASFTP XML Collection document as bytes
        to lxml.etree.Element with MARC21 as default namespace."""
        test_bytes = open("tests/fixtures/alma_bibs__new_1_ns.xml", "rb").read()
        test_out = etree.fromstring(open("tests/fixtures/alma_bibs__new_1_ns.xml", "rb").read())
        test_run = process.add_marc21xml_root_ns(test_bytes)
        self.assertEqual(etree.tostring(test_run), etree.tostring(test_out))
        self.assertIn("{http://www.loc.gov/MARC21/slim}", test_run.tag)

    def test_get_record_001(self):
        """Test validating & returning a MARC/XML OO1 field."""
        test_xml = etree.fromstring(open("tests/fixtures/record_001.xml", "rb").read())
        test_run = process.get_record_001(test_xml)
        self.assertEqual(test_run, "991022063789703811")

    def test_get_record_001_empty(self):
        """Test validating & returning a MARC/XML OO1 field."""
        test_xml = etree.fromstring(open("tests/fixtures/record_001_empty.xml", "rb").read())
        with self.assertLogs("tulflow_process") as log:
            test_run = process.get_record_001(test_xml)
        self.assertEqual(test_run, None)
        logs = [
            "ERROR:tulflow_process:Record without an 001 MMS Identifier:",
            "ERROR:tulflow_process:" + str(etree.tostring(test_xml))
        ]
        self.assertEqual(log.output, logs)

    def test_get_record_001_missing(self):
        """Test validating & returning a MARC/XML OO1 field."""
        test_xml = etree.fromstring(open("tests/fixtures/record_001_missing.xml", "rb").read())
        with self.assertLogs("tulflow_process") as log:
            test_run = process.get_record_001(test_xml)
        self.assertEqual(test_run, None)
        logs = [
            "ERROR:tulflow_process:Record without an 001 MMS Identifier:",
            "ERROR:tulflow_process:" + str(etree.tostring(test_xml))
        ]
        self.assertEqual(log.output, logs)

    def test_get_record_001_dup(self):
        """Test validating & returning a MARC/XML OO1 field."""
        test_xml = etree.fromstring(open("tests/fixtures/record_001_dup.xml", "rb").read())
        test_run = process.get_record_001(test_xml)
        with self.assertLogs("tulflow_process") as log:
            test_run = process.get_record_001(test_xml)
        self.assertEqual(test_run, None)
        logs = [
            "ERROR:tulflow_process:Record with multiple 001 MMS Identifiers:",
            "ERROR:tulflow_process:" + str(etree.tostring(test_xml))
        ]
        self.assertEqual(log.output, logs)

    def test_generate_bw_parent_new_field(self):
        desired_xml = b"""<marc21:datafield xmlns:marc21="http://www.loc.gov/MARC21/slim" ind1=" " ind2=" " tag="ADF"><marc21:subfield code="a">FAKE_PARENT_ID</marc21:subfield></marc21:datafield>"""
        test_run = process.generate_bw_parent_field("FAKE_PARENT_ID")
        self.assertEqual(etree.tostring(test_run), desired_xml)

    @httpretty.activate
    def test_get_github_content_exists(self):
        """Test getting existing file from GitHub."""
        httpretty.register_uri(
            httpretty.GET,
            "https://raw.github.com/tulibraries/aggregator_mdx/main/transforms/temple.xsl",
            body=open("tests/fixtures/temple.xsl").read()
        )
        repository = "tulibraries/aggregator_mdx"
        filename = "transforms/temple.xsl"

        test_run = process.get_github_content(repository, filename)
        self.assertEqual(test_run, open("tests/fixtures/temple.xsl", "rb").read())

    @httpretty.activate
    def test_get_github_content_missing(self):
        """Test getting missing file from GitHub & return appropriate error."""
        httpretty.register_uri(
            httpretty.GET,
            "https://raw.github.com/tulibraries/aggregator_mdx/main/transforms/temple-fake.xsl",
            status=404
        )
        repository = "tulibraries/aggregator_mdx"
        filename = "transforms/temple-fake.xsl"

        with self.assertLogs() as log:
            with self.assertRaises(SystemExit) as pytest_wrapped_e:
                process.get_github_content(repository, filename)
        self.assertEqual(pytest_wrapped_e.exception.code, 1)
        self.assertIn("ERROR:root:404 Client Error: Not Found for url: https://raw.github.com/tulibraries/aggregator_mdx/main/transforms/temple-fake.xsl", log.output)

class TestS3ProcessInteractions(unittest.TestCase):
    """Test Class for S3 data processing functions."""

    @mock_aws
    def test_remove_s3_object(self):
        bucket = "test_bucket"
        key = "test_key"
        access_id = "test_access_id"
        access_secret = "test_access_secret"
        conn = boto3.client("s3", aws_access_key_id=access_id, aws_secret_access_key=access_secret)
        conn.create_bucket(Bucket=bucket)
        conn.put_object(Bucket=bucket, Key=key, Body="test content")
        test_content_exists = conn.get_object(Bucket=bucket, Key=key)
        test_object_exists = conn.list_objects(Bucket=bucket)
        self.assertEqual(test_content_exists["Body"].read(), b"test content")
        self.assertEqual(test_content_exists["ResponseMetadata"]["HTTPStatusCode"], 200)
        self.assertEqual(test_object_exists["Contents"][0]["Key"], key)
        process.remove_s3_object(bucket, key, access_id, access_secret)
        test_object_gone = conn.list_objects(Bucket=bucket)
        self.assertEqual(test_object_gone["ResponseMetadata"]["HTTPStatusCode"], 200)
        self.assertEqual(test_object_gone.get("Contents"), None)

    @mock_aws
    def test_get_s3_content(self):
        bucket = "test_bucket"
        key = "test_key_2"
        access_id = "test_access_id"
        access_secret = "test_access_secret"
        conn = boto3.client("s3", aws_access_key_id=access_id, aws_secret_access_key=access_secret)
        conn.create_bucket(Bucket=bucket)
        conn.put_object(Bucket=bucket, Key=key, Body="test more content")
        test_content_exists = conn.get_object(Bucket=bucket, Key=key)
        test_object_exists = conn.list_objects(Bucket=bucket)
        self.assertEqual(test_content_exists["Body"].read(), b"test more content")
        self.assertEqual(test_content_exists["ResponseMetadata"]["HTTPStatusCode"], 200)
        self.assertEqual(test_object_exists["Contents"][0]["Key"], key)
        test_run = process.get_s3_content(bucket, key, access_id, access_secret)
        self.assertEqual(test_run, b"test more content")

    @mock_aws
    def test_list_s3_content(self):
        bucket = "test_bucket"
        prefix = "test_prefix"
        access_id = "test_access_id"
        access_secret = "test_access_secret"
        conn = boto3.client("s3", aws_access_key_id=access_id, aws_secret_access_key=access_secret)
        conn.create_bucket(Bucket=bucket)
        conn.put_object(Bucket=bucket, Key="test_prefix/item1", Body="test listing items")
        conn.put_object(Bucket=bucket, Key="item2", Body="including more items")
        conn.put_object(Bucket=bucket, Key="item2.magic", Body="and even more items")
        test_run_all = process.list_s3_content(bucket, access_id, access_secret)
        self.assertEqual(test_run_all, ["item2", "item2.magic", "test_prefix/item1"])
        test_run_pref = process.list_s3_content(bucket, access_id, access_secret, prefix=prefix)
        self.assertEqual(test_run_pref, ["test_prefix/item1"])

    @mock_aws
    def test_genereate_s3_object(self):
        bucket = "test_bucket"
        key = "test_key_2"
        body = b"<test>even more content</test>"
        access_id = "test_access_id"
        access_secret = "test_access_secret"
        conn = boto3.client("s3", aws_access_key_id=access_id, aws_secret_access_key=access_secret)
        conn.create_bucket(Bucket=bucket)
        process.generate_s3_object(body, bucket, key, access_id, access_secret)
        test_content_exists = conn.get_object(Bucket=bucket, Key=key)
        test_object_exists = conn.list_objects(Bucket=bucket)
        self.assertEqual(test_content_exists["Body"].read(), body)
        self.assertEqual(test_content_exists["ResponseMetadata"]["HTTPStatusCode"], 200)
        self.assertEqual(test_object_exists["Contents"][0]["Key"], key)
