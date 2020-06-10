"""Tests suite for tulflow.validate (functions for validating XML or JSON in Airflow Tasks)."""
import unittest
import boto3
from lxml import etree
from moto import mock_s3
from tulflow import process, validate
import logging
from mock import patch
from airflow import AirflowException

class TestSchematronFiltering(unittest.TestCase):
    """Test Class for functions that filtering XML with Schematron."""
    maxDiff = None
    kwargs = {
        "source_prefix": "dpla_test/transformed",
        "destination_prefix": "dpla_test/transformed-filtered",
        "report_prefix": "dpla_test/harvest_filter",
        "bucket": "tulib-airflow-test",
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
            response = validate.filter_s3_schematron(**self.kwargs)
        self.assertIn("INFO:root:Validating & Filtering File: dpla_test/transformed/sch-oai-valid.xml", log.output)
        test_valid_objects = conn.list_objects(Bucket=bucket, Prefix=self.kwargs.get("destination_prefix") + "/")
        test_valid_objects_ar = [object.get("Key") for object in test_valid_objects["Contents"]]
        self.assertEqual(test_valid_objects["ResponseMetadata"]["HTTPStatusCode"], 200)
        self.assertEqual(test_valid_objects_ar, ["dpla_test/transformed-filtered/sch-oai-valid.xml"])
        self.assertEqual(response, { "filtered": 0 })

        test_invalid_objects = conn.list_objects(Bucket=bucket, Prefix=self.kwargs.get("report_prefix") + "-invalid.csv")
        test_invalid_objects_ar = [object.get("Key") for object in test_invalid_objects["Contents"]]
        self.assertEqual(test_invalid_objects["ResponseMetadata"]["HTTPStatusCode"], 200)
        self.assertEqual(test_invalid_objects_ar, ["dpla_test/harvest_filter-invalid.csv"])
        test_invalid_content = conn.get_object(Bucket=bucket, Key="dpla_test/harvest_filter-invalid.csv")
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
            with self.assertRaises(AirflowException) as context:
                validate.filter_s3_schematron(**self.kwargs)

        errors = "All records were filtered out: 5"
        exception = AirflowException(errors)
        self.assertEqual(str(exception), str(context.exception))

        self.assertIn("INFO:root:Validating & Filtering File: dpla_test/transformed/sch-oai-invalid.xml", log.output)
        self.assertIn("ERROR:root:Invalid record found: invalid-missingtitle", log.output[1])
        self.assertIn("ERROR:root:Invalid record found: invalid-missingrights", log.output[2])
        self.assertIn('WARNING:root:All records filtered from dpla_test/transformed-filtered/sch-oai-invalid.xml. record_count: 5', log.output[6])
        self.assertEqual(len(log.output), 9)

        test_valid_objects = conn.list_objects(Bucket=bucket, Prefix=self.kwargs.get("destination_prefix") + "/")
        test_valid_objects_ar = [object.get("Key") for object in test_valid_objects["Contents"]]
        test_valid_content = conn.get_object(Bucket=bucket, Key="dpla_test/transformed-filtered/sch-oai-invalid.xml")
        self.assertEqual(test_valid_objects["ResponseMetadata"]["HTTPStatusCode"], 200)
        self.assertEqual(test_valid_objects_ar, ["dpla_test/transformed-filtered/sch-oai-invalid.xml"])
        self.assertEqual(test_valid_content["Body"].read(), b"""<metadata xmlns:dc="http://purl.org/dc/elements/1.1/" xmlns:dcterms="http://purl.org/dc/terms/" xmlns:edm="http://www.europeana.eu/schemas/edm/" xmlns:oai_dc="http://www.openarchives.org/OAI/2.0/oai_dc/" xmlns:dpla="http://dp.la/about/map/" xmlns:schema="http://schema.org" xmlns:oai="http://www.openarchives.org/OAI/2.0/" xmlns:oai_qdc="http://worldcat.org/xmlschemas/qdc-1.0/">\n   </metadata>""")

        invalid_prefix = self.kwargs.get("report_prefix") + "-invalid.csv"
        test_invalid_objects = conn.list_objects(Bucket=bucket, Prefix=invalid_prefix)
        self.assertEqual(test_invalid_objects["ResponseMetadata"]["HTTPStatusCode"], 200)
        test_invalid_objects_ar = [object.get("Key") for object in test_invalid_objects["Contents"]]
        self.assertEqual(len(test_invalid_objects_ar), 1)
        self.assertIn("dpla_test/harvest_filter-invalid.csv", test_invalid_objects_ar)
        test_invalid_content = conn.get_object(Bucket=bucket, Key="dpla_test/harvest_filter-invalid.csv")["Body"].read()
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
            response = validate.filter_s3_schematron(**self.kwargs)
        self.assertIn("INFO:root:Validating & Filtering File: dpla_test/transformed/sch-oai-mix.xml", log.output)
        self.assertIn("ERROR:root:Invalid record found: invalid-missingtitle", log.output[1])
        self.assertIn("ERROR:root:Invalid record found: invalid-missingrights", log.output[2])
        self.assertEqual(len(log.output), 8)
        self.assertEqual(response, { "filtered": 5 })

        test_valid_objects = conn.list_objects(Bucket=bucket, Prefix=self.kwargs.get("destination_prefix") + "/")
        test_valid_objects_ar = [object.get("Key") for object in test_valid_objects["Contents"]]
        test_valid_content = conn.get_object(Bucket=bucket, Key="dpla_test/transformed-filtered/sch-oai-mix.xml")["Body"].read()
        self.assertEqual(test_valid_objects["ResponseMetadata"]["HTTPStatusCode"], 200)
        self.assertEqual(test_valid_objects_ar, ["dpla_test/transformed-filtered/sch-oai-mix.xml"])
        self.assertIn(b'<oai_dc:dc airflow-record-id="valid">', test_valid_content)
        self.assertIn(b"<dcterms:identifier>valid</dcterms:identifier>", test_valid_content)
        self.assertIn(b"<dcterms:identifier>valid2</dcterms:identifier>", test_valid_content)
        self.assertIn(b"<dcterms:identifier>valid3</dcterms:identifier>", test_valid_content)

        invalid_prefix = self.kwargs.get("report_prefix") + "-invalid.csv"
        test_invalid_objects = conn.list_objects(Bucket=bucket, Prefix=invalid_prefix)
        self.assertEqual(test_invalid_objects["ResponseMetadata"]["HTTPStatusCode"], 200)
        test_invalid_objects_ar = [object.get("Key") for object in test_invalid_objects["Contents"]]
        self.assertIn("dpla_test/harvest_filter-invalid.csv", test_invalid_objects_ar)
        self.assertEqual(len(test_invalid_objects_ar), 1)
        test_invalid_content = conn.get_object(Bucket=bucket, Key="dpla_test/harvest_filter-invalid.csv")["Body"].read()
        self.assertIn(b"invalid-missingtitle", test_invalid_content)
        self.assertIn(b"here must be a rights statement", test_invalid_content)


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
            response = validate.filter_s3_schematron(**self.kwargs)
        self.assertIn("INFO:root:Validating & Filtering File: dpla_test/transformed/sch-oai-empty.xml", log.output)
        self.assertIn("INFO:root:Invalid Records report: https://tulib-airflow-test.s3.amazonaws.com/dpla_test/harvest_filter-invalid.csv", log.output)
        test_objects = conn.list_objects(Bucket=bucket, Prefix=self.kwargs.get("destination_prefix") + "/")
        test_objects_ar = [object.get("Key") for object in test_objects["Contents"]]
        test_content = conn.get_object(Bucket=bucket, Key="dpla_test/transformed-filtered/sch-oai-empty.xml")
        self.assertEqual(test_objects["ResponseMetadata"]["HTTPStatusCode"], 200)
        self.assertEqual(test_objects_ar, ["dpla_test/transformed-filtered/sch-oai-empty.xml"])
        self.assertEqual(test_content["Body"].read(), b"""<metadata xmlns:dc="http://purl.org/dc/elements/1.1/" xmlns:dcterms="http://purl.org/dc/terms/" xmlns:edm="http://www.europeana.eu/schemas/edm/" xmlns:oai_dc="http://www.openarchives.org/OAI/2.0/oai_dc/" xmlns:dpla="http://dp.la/about/map/" xmlns:schema="http://schema.org" xmlns:oai="http://www.openarchives.org/OAI/2.0/" xmlns:oai_qdc="http://worldcat.org/xmlschemas/qdc-1.0/">\n</metadata>""")
        self.assertEqual(response, { "filtered": 0 })

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
        response = validate.filter_s3_schematron(**self.kwargs)
        test_objects = conn.list_objects(Bucket=bucket, Prefix=self.kwargs.get("destination_prefix") + "/")
        self.assertEqual(test_objects["ResponseMetadata"]["HTTPStatusCode"], 200)
        self.assertEqual(test_objects.get("Contents"), None)
        self.assertEqual(response, { "filtered": 0 })

class TestSchematronReporting(unittest.TestCase):
    """Test Class for functions that generate reports on XML validated with Schematron."""
    maxDiff = None
    kwargs = {
        "source_prefix": "dpla_test/transformed",
        "destination_prefix": "dpla_test/transformed-filtered",
        "bucket": "tulib-airflow-test",
        "schematron_filename": "validations/padigital_reqd_fields.sch",
        "access_id": "kittens",
        "access_secret": "puppies"
    }

    @mock_s3
    @patch("tulflow.process.get_github_content")
    def test_report_s3_schematron_all_valid(self, mocked_get_github_content):
        """Test Reporting on Valid S3 XML File Validated with Schematron."""
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
            response = validate.report_s3_schematron(**self.kwargs)
        self.assertIn("INFO:root:Validating & Reporting On File: dpla_test/transformed/sch-oai-valid.xml", log.output)
        test_report_objects = conn.list_objects(Bucket=bucket, Prefix=self.kwargs.get("destination_prefix") + "-report.csv")
        self.assertEqual(test_report_objects["ResponseMetadata"]["HTTPStatusCode"], 200)
        test_report_objects_ar = [object.get("Key") for object in test_report_objects["Contents"]]
        self.assertEqual(test_report_objects_ar, ["dpla_test/transformed-filtered-report.csv"])
        test_report_content = conn.get_object(Bucket=bucket, Key="dpla_test/transformed-filtered-report.csv")["Body"].read()
        self.assertEqual(test_report_content, b'id,report,record,source_file\r\nvalid,,valid,https://s3.console.aws.amazon.com/s3/object/tulib-airflow-test/dpla_test/transformed/sch-oai-valid.xml\r\nvalid2,,valid2,https://s3.console.aws.amazon.com/s3/object/tulib-airflow-test/dpla_test/transformed/sch-oai-valid.xml\r\nvalid3,,valid3,https://s3.console.aws.amazon.com/s3/object/tulib-airflow-test/dpla_test/transformed/sch-oai-valid.xml\r\n')
        self.assertIn(b'id,report,record,source_file\r\nvalid', test_report_content)
        self.assertEqual(response, { "transformed": 3 })


    @mock_s3
    @patch("tulflow.process.get_github_content")
    def test_report_s3_schematron_all_invalid(self, mocked_get_github_content):
        """Test Reporting on Invalid S3 XML File Validated with Schematron."""
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
            response = validate.report_s3_schematron(**self.kwargs)
        self.assertIn("INFO:root:Validating & Reporting On File: dpla_test/transformed/sch-oai-invalid.xml", log.output)
        test_report_objects = conn.list_objects(Bucket=bucket, Prefix=self.kwargs.get("destination_prefix") + "-report.csv")
        self.assertEqual(test_report_objects["ResponseMetadata"]["HTTPStatusCode"], 200)
        test_report_objects_ar = [object.get("Key") for object in test_report_objects["Contents"]]
        self.assertEqual(test_report_objects_ar, ["dpla_test/transformed-filtered-report.csv"])
        test_report_content = conn.get_object(Bucket=bucket, Key="dpla_test/transformed-filtered-report.csv")["Body"].read()
        self.assertIn(b'id,report,record,source_file\r\ninvalid-missingtitle', test_report_content)
        self.assertIn(b'\r\ninvalid-missingrights', test_report_content)
        self.assertIn(b'\r\ninvalid-missingitemurl', test_report_content)
        self.assertEqual(response, { "transformed": 5 })


    @mock_s3
    @patch("tulflow.process.get_github_content")
    def test_report_s3_schematron_mix(self, mocked_get_github_content):
        """Test Reporting on Valid & Invalid S3 XML File Validated with Schematron."""
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
            response = validate.report_s3_schematron(**self.kwargs)
        self.assertIn("INFO:root:Validating & Reporting On File: dpla_test/transformed/sch-oai-mix.xml", log.output)
        test_report_objects = conn.list_objects(Bucket=bucket, Prefix=self.kwargs.get("destination_prefix") + "-report.csv")
        self.assertEqual(test_report_objects["ResponseMetadata"]["HTTPStatusCode"], 200)
        test_report_objects_ar = [object.get("Key") for object in test_report_objects["Contents"]]
        self.assertEqual(test_report_objects_ar, ["dpla_test/transformed-filtered-report.csv"])
        test_report_content = conn.get_object(Bucket=bucket, Key="dpla_test/transformed-filtered-report.csv")["Body"].read()
        self.assertIn(b'id,report,record,source_file\r\nvalid', test_report_content)
        self.assertIn(b'\r\ninvalid-missingtitle', test_report_content)
        self.assertIn(b'\r\ninvalid-missingrights,', test_report_content)
        self.assertIn(b'\r\ninvalid-missingitemurl,', test_report_content)
        self.assertEqual(response, { "transformed": 8 })

    @mock_s3
    @patch("tulflow.process.get_github_content")
    def test_report_s3_schematron_empty(self, mocked_get_github_content):
        """Test Reporting on Empty S3 XML File Validated with Schematron."""
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
            response = validate.report_s3_schematron(**self.kwargs)
        self.assertIn("INFO:root:Validating & Reporting On File: dpla_test/transformed/sch-oai-empty.xml", log.output)
        test_report_objects = conn.list_objects(Bucket=bucket, Prefix=self.kwargs.get("destination_prefix") + "-report.csv")
        self.assertEqual(test_report_objects["ResponseMetadata"]["HTTPStatusCode"], 200)
        test_report_objects_ar = [object.get("Key") for object in test_report_objects["Contents"]]
        self.assertEqual(test_report_objects_ar, ["dpla_test/transformed-filtered-report.csv"])
        test_report_content = conn.get_object(Bucket=bucket, Key="dpla_test/transformed-filtered-report.csv")["Body"].read()
        self.assertIn(b'id,report,record,source_file\r\n', test_report_content)
        self.assertEqual(response, { "transformed": 0 })

    @mock_s3
    @patch("tulflow.process.get_github_content")
    def test_report_s3_schematron_none(self, mocked_get_github_content):
        """Test Reporting on No provided S3 XML File Validated with Schematron."""
        # setup kwargs for test runs
        access_id = self.kwargs.get("access_id")
        access_secret = self.kwargs.get("access_secret")
        bucket = self.kwargs.get("bucket")

        # create expected mocked s3 resources
        conn = boto3.client("s3", aws_access_key_id=access_id, aws_secret_access_key=access_secret)
        conn.create_bucket(Bucket=bucket)

        # mock schematron github retrieval response
        mocked_get_github_content.return_value = open("tests/fixtures/sch-sample.sch").read()

        # run tests
        with self.assertLogs() as log:
            response = validate.report_s3_schematron(**self.kwargs)
        self.assertEqual(log.output, ["INFO:root:Records report: https://tulib-airflow-test.s3.amazonaws.com/dpla_test/transformed-filtered-report.csv", 'INFO:root:Total Transform Count: 0'])
        test_report_objects = conn.list_objects(Bucket=bucket, Prefix=self.kwargs.get("destination_prefix") + "-report.csv")
        self.assertEqual(test_report_objects["ResponseMetadata"]["HTTPStatusCode"], 200)
        test_report_objects_ar = [object.get("Key") for object in test_report_objects["Contents"]]
        self.assertEqual(test_report_objects_ar, ["dpla_test/transformed-filtered-report.csv"])
        test_report_content = conn.get_object(Bucket=bucket, Key="dpla_test/transformed-filtered-report.csv")["Body"].read()
        self.assertEqual(b'id,report,record,source_file\r\n', test_report_content)
        self.assertEqual(response, { "transformed": 0 })

    def test_schematron_failed_validation_text(self):
        single_failure = etree.fromstring(b'<svrl:schematron-output xmlns:svrl="http://purl.oclc.org/dsdl/svrl" xmlns:xs="http://www.w3.org/2001/XMLSchema" xmlns:schold="http://www.ascc.net/xml/schematron" xmlns:sch="http://www.ascc.net/xml/schematron" xmlns:iso="http://purl.oclc.org/dsdl/schematron" xmlns:dcterms="http://purl.org/dc/terms/" xmlns:edm="http://www.europeana.eu/schemas/edm/" xmlns:oai_dc="http://www.openarchives.org/OAI/2.0/oai_dc/" title="" schemaVersion=""><!--  &#160;\n\t\t  &#160;\n\t\t  &#160;\n\t\t --><svrl:ns-prefix-in-attribute-values uri="http://purl.org/dc/terms/" prefix="dcterms"/><svrl:ns-prefix-in-attribute-values uri="http://www.europeana.eu/schemas/edm/" prefix="edm"/><svrl:ns-prefix-in-attribute-values uri="http://www.openarchives.org/OAI/2.0/oai_dc/" prefix="oai_dc"/><svrl:active-pattern id="RequiredElementsPattern" name="Required PA Digital Elements"/><svrl:fired-rule context="oai_dc:dc"/><svrl:failed-assert test="edm:preview" id="Required1" role="error" location="/*[local-name()=\'dc\' and namespace-uri()=\'http://www.openarchives.org/OAI/2.0/oai_dc/\']"><svrl:text>There must be a thumbnail URL</svrl:text></svrl:failed-assert><svrl:active-pattern id="ThumbnailURLElementPattern" name="Additional Thumbnail URL Requirements"/></svrl:schematron-output>')
        response = validate.schematron_failed_validation_text(single_failure)
        self.assertEqual(response, 'There must be a thumbnail URL')

        multiple_failures = etree.fromstring(b'<svrl:schematron-output xmlns:svrl="http://purl.oclc.org/dsdl/svrl" xmlns:xs="http://www.w3.org/2001/XMLSchema" xmlns:schold="http://www.ascc.net/xml/schematron" xmlns:sch="http://www.ascc.net/xml/schematron" xmlns:iso="http://purl.oclc.org/dsdl/schematron" xmlns:dcterms="http://purl.org/dc/terms/" xmlns:edm="http://www.europeana.eu/schemas/edm/" xmlns:oai_dc="http://www.openarchives.org/OAI/2.0/oai_dc/" title="" schemaVersion=""><!--  &#160;\n\t\t  &#160;\n\t\t  &#160;\n\t\t --><svrl:ns-prefix-in-attribute-values uri="http://purl.org/dc/terms/" prefix="dcterms"/><svrl:ns-prefix-in-attribute-values uri="http://www.europeana.eu/schemas/edm/" prefix="edm"/><svrl:ns-prefix-in-attribute-values uri="http://www.openarchives.org/OAI/2.0/oai_dc/" prefix="oai_dc"/> <svrl:active-pattern id="RequiredElementsPattern" name="Required PA Digital Elements"/><svrl:fired-rule context="oai_dc:dc"/><svrl:failed-assert test="edm:preview" id="Required1" role="error" location="/*[local-name()=\'dc\' and namespace-uri()=\'http://www.openarchives.org/OAI/2.0/oai_dc/\']"><svrl:text>Needs Another Thing</svrl:text></svrl:failed-assert><svrl:active-pattern id="OtherPattern" name="Other Thing"/> <svrl:active-pattern id="RequiredElementsPattern" name="Required PA Digital Elements"/><svrl:fired-rule context="oai_dc:dc"/><svrl:failed-assert test="edm:preview" id="Required1" role="error" location="/*[local-name()=\'dc\' and namespace-uri()=\'http://www.openarchives.org/OAI/2.0/oai_dc/\']"><svrl:text>There must be a thumbnail URL</svrl:text></svrl:failed-assert><svrl:active-pattern id="ThumbnailURLElementPattern" name="Additional Thumbnail URL Requirements"/></svrl:schematron-output>')
        response = validate.schematron_failed_validation_text(multiple_failures)
        self.assertEqual(response, 'Needs Another Thing\nThere must be a thumbnail URL')

        no_failures = etree.fromstring(b'<svrl:schematron-output xmlns:svrl="http://purl.oclc.org/dsdl/svrl" xmlns:xs="http://www.w3.org/2001/XMLSchema" xmlns:schold="http://www.ascc.net/xml/schematron" xmlns:sch="http://www.ascc.net/xml/schematron" xmlns:iso="http://purl.oclc.org/dsdl/schematron" xmlns:dcterms="http://purl.org/dc/terms/" xmlns:edm="http://www.europeana.eu/schemas/edm/" xmlns:oai_dc="http://www.openarchives.org/OAI/2.0/oai_dc/" title="" schemaVersion=""><!--  &#160;\n\t\t  &#160;\n\t\t  &#160;\n\t\t --><svrl:ns-prefix-in-attribute-values uri="http://purl.org/dc/terms/" prefix="dcterms"/><svrl:ns-prefix-in-attribute-values uri="http://www.europeana.eu/schemas/edm/" prefix="edm"/><svrl:ns-prefix-in-attribute-values uri="http://www.openarchives.org/OAI/2.0/oai_dc/" prefix="oai_dc"/><svrl:active-pattern id="RequiredElementsPattern" name="Required PA Digital Elements"/><svrl:fired-rule context="oai_dc:dc"/><svrl:active-pattern id="ThumbnailURLElementPattern" name="Additional Thumbnail URL Requirements"/><svrl:fired-rule context="oai_dc:dc/edm:preview"/></svrl:schematron-output>')
        response = validate.schematron_failed_validation_text(no_failures)
        self.assertEqual(response, '')

    def test_identifier_or_full_record(self):
        record = etree.fromstring(b'<oai_dc:dc xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xmlns:dc="http://purl.org/dc/elements/1.1/" xmlns:dcterms="http://purl.org/dc/terms/" xmlns:dpla="http://dp.la/about/map/" xmlns:edm="http://www.europeana.eu/schemas/edm/" xmlns:oai="http://www.openarchives.org/OAI/2.0/" xmlns:oai_dc="http://www.openarchives.org/OAI/2.0/oai_dc/" xmlns:oai_qdc="http://worldcat.org/xmlschemas/qdc-1.0/" xmlns:oclc="http://purl.org/oclc/terms/" xmlns:oclcdc="http://worldcat.org/xmlschemas/oclcdc-1.0/" xmlns:oclcterms="http://purl.org/oclc/terms/" xmlns:schema="http://schema.org" xmlns:padig="http://padigital.org/ns" xmlns:svcs="http://rdfs.org/sioc/services" airflow-record-id="pitt:00add0682m">\n   <dcterms:isPartOf>Historic Pittsburgh Book Collection</dcterms:isPartOf>\n   <dcterms:title>Thomas Mellon and his times</dcterms:title>\n   <dcterms:creator>Mellon, Thomas , 1813-1908</dcterms:creator>\n   <dcterms:subject>Mellon family</dcterms:subject>\n   <dcterms:description>pt. I. Family history.--pt. II. Autobiography.</dcterms:description>\n   <dcterms:description>Printed for his family and descendants exclusively.</dcterms:description>\n   <dcterms:publisher>W. G. Johnston &amp; Co., printers</dcterms:publisher>\n   <edm:dataProvider>University of Pittsburgh</edm:dataProvider>\n   <dcterms:date>1885</dcterms:date>\n   <dcterms:type>Text</dcterms:type>\n   <dcterms:format>bibliography</dcterms:format>\n   <dcterms:format>biography</dcterms:format>\n    <dcterms:identifier>pitt:00add0682m</dcterms:identifier>\n   <dcterms:language>eng</dcterms:language>\n   <dcterms:rights>No Copyright - United States. The organization that has made the Item available believes that the Item is in the Public Domain under the laws of the United States, but a determination was not made as to its copyright status under the copyright laws of other countries. The Item may not be in the Public Domain under the laws of other countries. Please refer to the organization that has made the Item available for more information.</dcterms:rights>\n   <edm:rights>http://rightsstatements.org/vocab/NoC-US/1.0/</edm:rights>\n   <edm:preview>http://historicpittsburgh.org/islandora/object/pitt%3A00add0682m/datastream/TN/view/Thomas%20Mellon%20and%20his%20times.jpg</edm:preview>\n   <edm:isShownAt>http://historicpittsburgh.org/islandora/object/pitt%3A00add0682m</edm:isShownAt>\n   <dpla:intermediateProvider>Historic Pittsburgh</dpla:intermediateProvider>\n   <edm:provider>PA Digital</edm:provider>\n</oai_dc:dc>\n')
        identifiers = validate.identifier_or_full_record(record)
        self.assertEqual(identifiers, "pitt:00add0682m")

        multiple_identifiers = etree.fromstring(b'<oai_dc:dc xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xmlns:dc="http://purl.org/dc/elements/1.1/" xmlns:dcterms="http://purl.org/dc/terms/" xmlns:dpla="http://dp.la/about/map/" xmlns:edm="http://www.europeana.eu/schemas/edm/" xmlns:oai="http://www.openarchives.org/OAI/2.0/" xmlns:oai_dc="http://www.openarchives.org/OAI/2.0/oai_dc/" xmlns:oai_qdc="http://worldcat.org/xmlschemas/qdc-1.0/" xmlns:oclc="http://purl.org/oclc/terms/" xmlns:oclcdc="http://worldcat.org/xmlschemas/oclcdc-1.0/" xmlns:oclcterms="http://purl.org/oclc/terms/" xmlns:schema="http://schema.org" xmlns:padig="http://padigital.org/ns" xmlns:svcs="http://rdfs.org/sioc/services" airflow-record-id="pitt:00add0682m">\n   <dcterms:isPartOf>Historic Pittsburgh Book Collection</dcterms:isPartOf>\n   <dcterms:title>Thomas Mellon and his times</dcterms:title>\n   <dcterms:creator>Mellon, Thomas , 1813-1908</dcterms:creator>\n   <dcterms:subject>Mellon family</dcterms:subject>\n   <dcterms:description>pt. I. Family history.--pt. II. Autobiography.</dcterms:description>\n   <dcterms:description>Printed for his family and descendants exclusively.</dcterms:description>\n   <dcterms:publisher>W. G. Johnston &amp; Co., printers</dcterms:publisher>\n   <edm:dataProvider>University of Pittsburgh</edm:dataProvider>\n   <dcterms:date>1885</dcterms:date>\n   <dcterms:type>Text</dcterms:type>\n   <dcterms:format>bibliography</dcterms:format>\n   <dcterms:format>biography</dcterms:format>\n  <dcterms:identifier>pitt:00add0682m</dcterms:identifier>\n   <dcterms:language>eng</dcterms:language>\n   <dcterms:identifier>other:identifier</dcterms:identifier>\n   <dcterms:rights>No Copyright - United States. The organization that has made the Item available believes that the Item is in the Public Domain under the laws of the United States, but a determination was not made as to its copyright status under the copyright laws of other countries. The Item may not be in the Public Domain under the laws of other countries. Please refer to the organization that has made the Item available for more information.</dcterms:rights>\n   <edm:rights>http://rightsstatements.org/vocab/NoC-US/1.0/</edm:rights>\n   <edm:preview>http://historicpittsburgh.org/islandora/object/pitt%3A00add0682m/datastream/TN/view/Thomas%20Mellon%20and%20his%20times.jpg</edm:preview>\n   <edm:isShownAt>http://historicpittsburgh.org/islandora/object/pitt%3A00add0682m</edm:isShownAt>\n   <dpla:intermediateProvider>Historic Pittsburgh</dpla:intermediateProvider>\n   <edm:provider>PA Digital</edm:provider>\n</oai_dc:dc>\n')
        identifiers = validate.identifier_or_full_record(multiple_identifiers)
        self.assertEqual(identifiers, "pitt:00add0682m\nother:identifier")

        no_identifiers_bytestring = b'<oai_dc:dc xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xmlns:dc="http://purl.org/dc/elements/1.1/" xmlns:dcterms="http://purl.org/dc/terms/" xmlns:dpla="http://dp.la/about/map/" xmlns:edm="http://www.europeana.eu/schemas/edm/" xmlns:oai="http://www.openarchives.org/OAI/2.0/" xmlns:oai_dc="http://www.openarchives.org/OAI/2.0/oai_dc/" xmlns:oai_qdc="http://worldcat.org/xmlschemas/qdc-1.0/" xmlns:oclc="http://purl.org/oclc/terms/" xmlns:oclcdc="http://worldcat.org/xmlschemas/oclcdc-1.0/" xmlns:oclcterms="http://purl.org/oclc/terms/" xmlns:schema="http://schema.org" xmlns:padig="http://padigital.org/ns" xmlns:svcs="http://rdfs.org/sioc/services" airflow-record-id="pitt:00add0682m">\n   <dcterms:isPartOf>Historic Pittsburgh Book Collection</dcterms:isPartOf>\n   <dcterms:title>Thomas Mellon and his times</dcterms:title>\n   <dcterms:creator>Mellon, Thomas , 1813-1908</dcterms:creator>\n   <dcterms:subject>Mellon family</dcterms:subject>\n   <dcterms:description>pt. I. Family history.--pt. II. Autobiography.</dcterms:description>\n   <dcterms:description>Printed for his family and descendants exclusively.</dcterms:description>\n   <dcterms:publisher>W. G. Johnston &amp; Co., printers</dcterms:publisher>\n   <edm:dataProvider>University of Pittsburgh</edm:dataProvider>\n   <dcterms:date>1885</dcterms:date>\n   <dcterms:type>Text</dcterms:type>\n   <dcterms:format>bibliography</dcterms:format>\n   <dcterms:format>biography</dcterms:format>\n   <dcterms:language>eng</dcterms:language>\n   <dcterms:rights>No Copyright - United States. The organization that has made the Item available believes that the Item is in the Public Domain under the laws of the United States, but a determination was not made as to its copyright status under the copyright laws of other countries. The Item may not be in the Public Domain under the laws of other countries. Please refer to the organization that has made the Item available for more information.</dcterms:rights>\n   <edm:rights>http://rightsstatements.org/vocab/NoC-US/1.0/</edm:rights>\n   <edm:preview>http://historicpittsburgh.org/islandora/object/pitt%3A00add0682m/datastream/TN/view/Thomas%20Mellon%20and%20his%20times.jpg</edm:preview>\n   <edm:isShownAt>http://historicpittsburgh.org/islandora/object/pitt%3A00add0682m</edm:isShownAt>\n   <dpla:intermediateProvider>Historic Pittsburgh</dpla:intermediateProvider>\n   <edm:provider>PA Digital</edm:provider>\n</oai_dc:dc>'
        no_identifiers = etree.fromstring(no_identifiers_bytestring)
        identifiers = validate.identifier_or_full_record(no_identifiers)
        self.assertEqual(identifiers, no_identifiers_bytestring)
