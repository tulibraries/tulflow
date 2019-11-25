"""Tests suite for tulflow.validate (functions for validating XML or JSON in Airflow Tasks)."""
import unittest
import boto3
from lxml import etree
from moto import mock_s3
from tulflow import process, validate
import logging
from mock import patch

class TestSchematronFiltering(unittest.TestCase):
    """Test Class for functions that filtering XML with Schematron."""
    maxDiff = None
    kwargs = {
        "source_prefix": "dpla_test/transformed",
        "destination_prefix": "dpla_test/transformed-filtered",
        "bucket": "tulib-airflow-test",
        "record_parent_element": "{http://www.openarchives.org/OAI/2.0/oai_dc/}dc",
        "schematron_filename": "validations/padigital_reqd_fields.sch",
        "access_id": "kittens",
        "access_secret": "puppies"
    }

    @mock_s3
    @patch("tulflow.process.get_github_content")
    def test_filter_s3_schematron_all_valid(self, mocked_get_github_content):
        """Test Filtering S3 XML File with Schematron."""
        # setup kwargs for test runs
        access_id = self.kwargs.get("access_id")
        access_secret = self.kwargs.get("access_secret")
        bucket = self.kwargs.get("bucket")
        test_key = self.kwargs.get("source_prefix") + "/sch-oai-valid.xml"

        # create expected mocked s3 resources
        conn = boto3.client("s3", aws_access_key_id=access_id, aws_secret_access_key=access_secret)
        conn.create_bucket(Bucket=bucket)
        conn.put_object(Bucket=bucket, Key=test_key, Body=open("tests/fixtures/sch-oai-valid.xml").read())
        test_content_exists = conn.get_object(Bucket=bucket, Key=test_key)
        test_object_exists = conn.list_objects(Bucket=bucket)
        self.assertEqual(test_content_exists["Body"].read(), open("tests/fixtures/sch-oai-valid.xml", "rb").read())
        self.assertEqual(test_content_exists["ResponseMetadata"]["HTTPStatusCode"], 200)
        self.assertEqual(test_object_exists["Contents"][0]["Key"], test_key)

        # mock schematron github retrieval response
        mocked_get_github_content.return_value = open("tests/fixtures/sch-sample.sch").read()

        # run tests
        with self.assertLogs() as log:
            validate.filter_s3_schematron(**self.kwargs)
        self.assertIn("INFO:root:Validating & Filtering File: dpla_test/transformed/sch-oai-valid.xml", log.output)
        test_valid_objects = conn.list_objects(Bucket=bucket, Prefix=self.kwargs.get("destination_prefix") + "/")
        test_valid_objects_ar = [object.get("Key") for object in test_valid_objects["Contents"]]
        self.assertEqual(test_valid_objects["ResponseMetadata"]["HTTPStatusCode"], 200)
        self.assertEqual(test_valid_objects_ar, ["dpla_test/transformed-filtered/sch-oai-valid.xml"])

        test_invalid_objects = conn.list_objects(Bucket=bucket, Prefix=self.kwargs.get("destination_prefix") + "-invalid.csv")
        test_invalid_objects_ar = [object.get("Key") for object in test_invalid_objects["Contents"]]
        self.assertEqual(test_invalid_objects["ResponseMetadata"]["HTTPStatusCode"], 200)
        self.assertEqual(test_invalid_objects_ar, ["dpla_test/transformed-filtered-invalid.csv"])
        test_invalid_content = conn.get_object(Bucket=bucket, Key="dpla_test/transformed-filtered-invalid.csv")
        self.assertEqual(test_invalid_content["Body"].read(), b"""id,report,record,source_file\r\n""")


    @mock_s3
    @patch("tulflow.process.get_github_content")
    def test_filter_s3_schematron_all_invalid(self, mocked_get_github_content):
        """Test Filtering S3 XML File with Schematron."""
        # setup kwargs for test runs
        access_id = self.kwargs.get("access_id")
        access_secret = self.kwargs.get("access_secret")
        bucket = self.kwargs.get("bucket")
        test_key = self.kwargs.get("source_prefix") + "/sch-oai-invalid.xml"

        # create expected mocked s3 resources
        conn = boto3.client("s3", aws_access_key_id=access_id, aws_secret_access_key=access_secret)
        conn.create_bucket(Bucket=bucket)
        conn.put_object(Bucket=bucket, Key=test_key, Body=open("tests/fixtures/sch-oai-invalid.xml").read())
        test_content_exists = conn.get_object(Bucket=bucket, Key=test_key)
        test_object_exists = conn.list_objects(Bucket=bucket)
        self.assertEqual(test_content_exists["Body"].read(), open("tests/fixtures/sch-oai-invalid.xml", "rb").read())
        self.assertEqual(test_content_exists["ResponseMetadata"]["HTTPStatusCode"], 200)
        self.assertEqual(test_object_exists["Contents"][0]["Key"], test_key)

        # mock schematron github retrieval response
        mocked_get_github_content.return_value = open("tests/fixtures/sch-sample.sch").read()

        # run tests
        with self.assertLogs() as log:
            validate.filter_s3_schematron(**self.kwargs)
        self.assertIn("INFO:root:Validating & Filtering File: dpla_test/transformed/sch-oai-invalid.xml", log.output)
        self.assertIn("ERROR:root:Invalid record found: invalid-missingtitle", log.output[1])
        self.assertIn("ERROR:root:Invalid record found: invalid-missingrights", log.output[2])
        self.assertEqual(len(log.output), 7)
        test_valid_objects = conn.list_objects(Bucket=bucket, Prefix=self.kwargs.get("destination_prefix") + "/")
        test_valid_objects_ar = [object.get("Key") for object in test_valid_objects["Contents"]]
        test_valid_content = conn.get_object(Bucket=bucket, Key="dpla_test/transformed-filtered/sch-oai-invalid.xml")
        self.assertEqual(test_valid_objects["ResponseMetadata"]["HTTPStatusCode"], 200)
        self.assertEqual(test_valid_objects_ar, ["dpla_test/transformed-filtered/sch-oai-invalid.xml"])
        self.assertEqual(test_valid_content["Body"].read(), b"""<metadata xmlns:dc="http://purl.org/dc/elements/1.1/" xmlns:dcterms="http://purl.org/dc/terms/" xmlns:edm="http://www.europeana.eu/schemas/edm/" xmlns:oai_dc="http://www.openarchives.org/OAI/2.0/oai_dc/" xmlns:dpla="http://dp.la/about/map/" xmlns:schema="http://schema.org" xmlns:oai="http://www.openarchives.org/OAI/2.0/" xmlns:oai_qdc="http://worldcat.org/xmlschemas/qdc-1.0/">\n   </metadata>""")

        invalid_prefix = self.kwargs.get("destination_prefix") + "-invalid.csv"
        test_invalid_objects = conn.list_objects(Bucket=bucket, Prefix=invalid_prefix)
        self.assertEqual(test_invalid_objects["ResponseMetadata"]["HTTPStatusCode"], 200)
        test_invalid_objects_ar = [object.get("Key") for object in test_invalid_objects["Contents"]]
        self.assertEqual(len(test_invalid_objects_ar), 1)
        self.assertIn("dpla_test/transformed-filtered-invalid.csv", test_invalid_objects_ar)
        test_invalid_content = conn.get_object(Bucket=bucket, Key="dpla_test/transformed-filtered-invalid.csv")["Body"].read()
        self.assertIn(b"""id,report,record,source_file\r\n""", test_invalid_content)


    @mock_s3
    @patch("tulflow.process.get_github_content")
    def test_filter_s3_schematron_mix(self, mocked_get_github_content):
        """Test Filtering S3 XML File with Schematron."""
        # setup kwargs for test runs
        access_id = self.kwargs.get("access_id")
        access_secret = self.kwargs.get("access_secret")
        bucket = self.kwargs.get("bucket")
        test_key = self.kwargs.get("source_prefix") + "/sch-oai-mix.xml"

        # create expected mocked s3 resources
        conn = boto3.client("s3", aws_access_key_id=access_id, aws_secret_access_key=access_secret)
        conn.create_bucket(Bucket=bucket)
        conn.put_object(Bucket=bucket, Key=test_key, Body=open("tests/fixtures/sch-oai-mix.xml").read())
        test_content_exists = conn.get_object(Bucket=bucket, Key=test_key)
        test_object_exists = conn.list_objects(Bucket=bucket)
        self.assertEqual(test_content_exists["Body"].read(), open("tests/fixtures/sch-oai-mix.xml", "rb").read())
        self.assertEqual(test_content_exists["ResponseMetadata"]["HTTPStatusCode"], 200)
        self.assertEqual(test_object_exists["Contents"][0]["Key"], test_key)

        # mock schematron github retrieval response
        mocked_get_github_content.return_value = open("tests/fixtures/sch-sample.sch").read()

        # run tests
        with self.assertLogs() as log:
            validate.filter_s3_schematron(**self.kwargs)
        self.assertIn("INFO:root:Validating & Filtering File: dpla_test/transformed/sch-oai-mix.xml", log.output)
        self.assertIn("ERROR:root:Invalid record found: invalid-missingtitle", log.output[1])
        self.assertIn("ERROR:root:Invalid record found: invalid-missingrights", log.output[2])
        self.assertEqual(len(log.output), 7)

        test_valid_objects = conn.list_objects(Bucket=bucket, Prefix=self.kwargs.get("destination_prefix") + "/")
        test_valid_objects_ar = [object.get("Key") for object in test_valid_objects["Contents"]]
        test_valid_content = conn.get_object(Bucket=bucket, Key="dpla_test/transformed-filtered/sch-oai-mix.xml")["Body"].read()
        self.assertEqual(test_valid_objects["ResponseMetadata"]["HTTPStatusCode"], 200)
        self.assertEqual(test_valid_objects_ar, ["dpla_test/transformed-filtered/sch-oai-mix.xml"])
        self.assertIn(b'<oai_dc:dc airflow-record-id="valid">', test_valid_content)
        self.assertIn(b"<dcterms:identifier>valid</dcterms:identifier>", test_valid_content)
        self.assertIn(b"<dcterms:identifier>valid2</dcterms:identifier>", test_valid_content)
        self.assertIn(b"<dcterms:identifier>valid3</dcterms:identifier>", test_valid_content)

        invalid_prefix = self.kwargs.get("destination_prefix") + "-invalid.csv"
        test_invalid_objects = conn.list_objects(Bucket=bucket, Prefix=invalid_prefix)
        self.assertEqual(test_invalid_objects["ResponseMetadata"]["HTTPStatusCode"], 200)
        test_invalid_objects_ar = [object.get("Key") for object in test_invalid_objects["Contents"]]
        self.assertIn("dpla_test/transformed-filtered-invalid.csv", test_invalid_objects_ar)
        self.assertEqual(len(test_invalid_objects_ar), 1)
        test_invalid_content = conn.get_object(Bucket=bucket, Key="dpla_test/transformed-filtered-invalid.csv")["Body"].read()
        self.assertIn(b"<dcterms:identifier>invalid-missingtitle</dcterms:identifier>", test_invalid_content)
        self.assertIn(b"<svrl:text>There must be a rights statement</svrl:text>", test_invalid_content)


    @mock_s3
    @patch("tulflow.process.get_github_content")
    def test_filter_s3_schematron_empty(self, mocked_get_github_content):
        """Test Filtering S3 XML File with Schematron."""
        # setup kwargs for test runs
        access_id = self.kwargs.get("access_id")
        access_secret = self.kwargs.get("access_secret")
        bucket = self.kwargs.get("bucket")
        test_key = self.kwargs.get("source_prefix") + "/sch-oai-empty.xml"

        # create expected mocked s3 resources
        conn = boto3.client("s3", aws_access_key_id=access_id, aws_secret_access_key=access_secret)
        conn.create_bucket(Bucket=bucket)
        conn.put_object(Bucket=bucket, Key=test_key, Body=open("tests/fixtures/sch-oai-empty.xml").read())
        test_content_exists = conn.get_object(Bucket=bucket, Key=test_key)
        test_object_exists = conn.list_objects(Bucket=bucket)
        self.assertEqual(test_content_exists["Body"].read(), open("tests/fixtures/sch-oai-empty.xml", "rb").read())
        self.assertEqual(test_content_exists["ResponseMetadata"]["HTTPStatusCode"], 200)
        self.assertEqual(test_object_exists["Contents"][0]["Key"], test_key)

        # mock schematron github retrieval response
        mocked_get_github_content.return_value = open("tests/fixtures/sch-sample.sch").read()

        # run tests
        with self.assertLogs() as log:
            validate.filter_s3_schematron(**self.kwargs)
        self.assertIn("INFO:root:Validating & Filtering File: dpla_test/transformed/sch-oai-empty.xml", log.output)
        self.assertIn("INFO:root:Invalid Records report: https://tulib-airflow-test.s3.amazonaws.com/dpla_test/transformed-filtered-invalid.csv", log.output)
        test_objects = conn.list_objects(Bucket=bucket, Prefix=self.kwargs.get("destination_prefix") + "/")
        test_objects_ar = [object.get("Key") for object in test_objects["Contents"]]
        test_content = conn.get_object(Bucket=bucket, Key="dpla_test/transformed-filtered/sch-oai-empty.xml")
        self.assertEqual(test_objects["ResponseMetadata"]["HTTPStatusCode"], 200)
        self.assertEqual(test_objects_ar, ["dpla_test/transformed-filtered/sch-oai-empty.xml"])
        self.assertEqual(test_content["Body"].read(), b"""<metadata xmlns:dc="http://purl.org/dc/elements/1.1/" xmlns:dcterms="http://purl.org/dc/terms/" xmlns:edm="http://www.europeana.eu/schemas/edm/" xmlns:oai_dc="http://www.openarchives.org/OAI/2.0/oai_dc/" xmlns:dpla="http://dp.la/about/map/" xmlns:schema="http://schema.org" xmlns:oai="http://www.openarchives.org/OAI/2.0/" xmlns:oai_qdc="http://worldcat.org/xmlschemas/qdc-1.0/">\n</metadata>""")


    @mock_s3
    @patch("tulflow.process.get_github_content")
    def test_filter_s3_schematron_none(self, mocked_get_github_content):
        """Test Filtering S3 XML File with Schematron."""
        # setup kwargs for test runs
        access_id = self.kwargs.get("access_id")
        access_secret = self.kwargs.get("access_secret")
        bucket = self.kwargs.get("bucket")
        test_key = self.kwargs.get("source_prefix") + "/sch-oai-none.xml"

        # create expected mocked s3 resources
        conn = boto3.client("s3", aws_access_key_id=access_id, aws_secret_access_key=access_secret)
        conn.create_bucket(Bucket=bucket)

        # mock schematron github retrieval response
        mocked_get_github_content.return_value = open("tests/fixtures/sch-sample.sch").read()

        # run tests
        validate.filter_s3_schematron(**self.kwargs)
        test_objects = conn.list_objects(Bucket=bucket, Prefix=self.kwargs.get("destination_prefix") + "/")
        self.assertEqual(test_objects["ResponseMetadata"]["HTTPStatusCode"], 200)
        self.assertEqual(test_objects.get("Contents"), None)
