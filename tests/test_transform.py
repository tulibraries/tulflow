"""Tests suite for tulflow.transform (functions for transforming XML or JSON in Airflow Tasks)."""
import unittest
import boto3
from lxml import etree
from moto import mock_s3
from tulflow import transform
import logging
from mock import patch

class TestXSLTransform(unittest.TestCase):
    """Test Class for functions that transform XML from S3 with XSL."""
    maxDiff = None
    kwargs = {
        "source_prefix": "dpla_test/new-updated-filtered",
        "destination_prefix": "dpla_test/transformed",
        "bucket": "tulib-airflow-test",
        "schematron_filename": "transforms/dplah.xsl",
        "access_id": "kittens",
        "access_secret": "puppies"
    }

    @mock_s3
    @patch('subprocess.check_output')
    def test_transform_s3_xml_simple(self, mocked_subprocess):
        """Test Pulling S3 XML, Transforming with XSLT, & Writing to S3."""
        # setup kwargs for test runs
        access_id = self.kwargs.get("access_id")
        access_secret = self.kwargs.get("access_secret")
        bucket = self.kwargs.get("bucket")
        test_key = self.kwargs.get("source_prefix") + "/xsl-sample.xml"

        # create expected mocked s3 resources
        conn = boto3.client("s3", aws_access_key_id=access_id, aws_secret_access_key=access_secret)
        conn.create_bucket(Bucket=bucket)
        conn.put_object(Bucket=bucket, Key=test_key, Body=open("tests/fixtures/xsl-sample.xml").read())
        test_content_exists = conn.get_object(Bucket=bucket, Key=test_key)
        test_object_exists = conn.list_objects(Bucket=bucket)
        self.assertEqual(test_content_exists["Body"].read(), open("tests/fixtures/xsl-sample.xml", "rb").read())
        self.assertEqual(test_content_exists["ResponseMetadata"]["HTTPStatusCode"], 200)
        self.assertEqual(test_object_exists["Contents"][0]["Key"], test_key)

        # setup mocked subprocess result
        mocked_subprocess.side_effect = [
            open("tests/fixtures/xsl-sample-simple-output-record1.xml", "rb").read(),
            open("tests/fixtures/xsl-sample-simple-output-record2.xml", "rb").read(),
            open("tests/fixtures/xsl-sample-simple-output-record3.xml", "rb").read()
        ]

        # run tests
        with self.assertLogs() as log:
            transform.transform_s3_xsl(**self.kwargs)
        self.assertIn("INFO:root:Transforming File dpla_test/new-updated-filtered/xsl-sample.xml", log.output)
        self.assertIn("INFO:root:Transforming Record oai:digital.library.villanova.edu:vudl:293113", log.output)
        self.assertIn("INFO:root:Transforming Record oai:digital.library.villanova.edu:vudl:469533", log.output)
        self.assertIn("INFO:root:Transforming Record oai:digital.library.villanova.edu:vudl:469545", log.output)
        test_output_objects = conn.list_objects(Bucket=bucket, Prefix=self.kwargs.get("destination_prefix"))
        self.assertEqual(test_output_objects["ResponseMetadata"]["HTTPStatusCode"], 200)
        test_output_objects_ar = [object.get("Key") for object in test_output_objects["Contents"]]
        self.assertEqual(test_output_objects_ar, ["dpla_test/transformed/xsl-sample.xml"])
        test_output_content = etree.fromstring(conn.get_object(Bucket=bucket, Key="dpla_test/transformed/xsl-sample.xml")["Body"].read())
        should_match_output = etree.fromstring(open("tests/fixtures/xsl-sample-simple-output-all.xml", "rb").read())
        self.assertEqual(
            etree.tostring(test_output_content, pretty_print=True),
            etree.tostring(should_match_output, pretty_print=True)
        )


    @mock_s3
    @patch('subprocess.check_output')
    def test_transform_s3_xml_complex(self, mocked_subprocess):
        """Test Pulling S3 XML, Transforming with Complex XSLT, & Writing to S3."""
        # setup kwargs for test runs
        access_id = self.kwargs.get("access_id")
        access_secret = self.kwargs.get("access_secret")
        bucket = self.kwargs.get("bucket")
        test_key = self.kwargs.get("source_prefix") + "/xsl-sample.xml"

        # create expected mocked s3 resources
        conn = boto3.client("s3", aws_access_key_id=access_id, aws_secret_access_key=access_secret)
        conn.create_bucket(Bucket=bucket)
        conn.put_object(Bucket=bucket, Key=test_key, Body=open("tests/fixtures/xsl-sample.xml").read())
        test_content_exists = conn.get_object(Bucket=bucket, Key=test_key)
        test_object_exists = conn.list_objects(Bucket=bucket)
        self.assertEqual(test_content_exists["Body"].read(), open("tests/fixtures/xsl-sample.xml", "rb").read())
        self.assertEqual(test_content_exists["ResponseMetadata"]["HTTPStatusCode"], 200)
        self.assertEqual(test_object_exists["Contents"][0]["Key"], test_key)

        # setup mocked subprocess result
        mocked_subprocess.side_effect = [
            open("tests/fixtures/xsl-sample-complex-output-record1.xml", "rb").read(),
            open("tests/fixtures/xsl-sample-complex-output-record2.xml", "rb").read(),
            open("tests/fixtures/xsl-sample-complex-output-record3.xml", "rb").read()
        ]

        # run tests
        with self.assertLogs() as log:
            transform.transform_s3_xsl(**self.kwargs)
        self.assertIn("INFO:root:Transforming File dpla_test/new-updated-filtered/xsl-sample.xml", log.output)
        self.assertIn("INFO:root:Transforming Record oai:digital.library.villanova.edu:vudl:293113", log.output)
        self.assertIn("INFO:root:Transforming Record oai:digital.library.villanova.edu:vudl:469533", log.output)
        self.assertIn("INFO:root:Transforming Record oai:digital.library.villanova.edu:vudl:469545", log.output)
        test_output_objects = conn.list_objects(Bucket=bucket, Prefix=self.kwargs.get("destination_prefix"))
        self.assertEqual(test_output_objects["ResponseMetadata"]["HTTPStatusCode"], 200)
        test_output_objects_ar = [object.get("Key") for object in test_output_objects["Contents"]]
        self.assertEqual(test_output_objects_ar, ["dpla_test/transformed/xsl-sample.xml"])
        test_output_content = etree.fromstring(conn.get_object(Bucket=bucket, Key="dpla_test/transformed/xsl-sample.xml")["Body"].read())
        should_match_output = etree.fromstring(open("tests/fixtures/xsl-sample-complex-output-all.xml", "rb").read())
        self.assertEqual(
            etree.tostring(test_output_content, pretty_print=True),
            etree.tostring(should_match_output, pretty_print=True)
        )
