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
            validate.filter_s3_schematron(**self.kwargs)
        self.assertIn("INFO:root:Validating & Filtering File: dpla_test/transformed/sch-oai-valid.xml", log.output)
        test_valid_objects = conn.list_objects(Bucket=bucket, Prefix=self.kwargs.get("destination_prefix") + "/")
        test_valid_objects_ar = [object.get("Key") for object in test_valid_objects["Contents"]]
        self.assertEqual(test_valid_objects["ResponseMetadata"]["HTTPStatusCode"], 200)
        self.assertEqual(test_valid_objects_ar, ["dpla_test/transformed-filtered/sch-oai-valid.xml"])

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

        invalid_prefix = self.kwargs.get("report_prefix") + "-invalid.csv"
        test_invalid_objects = conn.list_objects(Bucket=bucket, Prefix=invalid_prefix)
        self.assertEqual(test_invalid_objects["ResponseMetadata"]["HTTPStatusCode"], 200)
        test_invalid_objects_ar = [object.get("Key") for object in test_invalid_objects["Contents"]]
        self.assertIn("dpla_test/harvest_filter-invalid.csv", test_invalid_objects_ar)
        self.assertEqual(len(test_invalid_objects_ar), 1)
        test_invalid_content = conn.get_object(Bucket=bucket, Key="dpla_test/harvest_filter-invalid.csv")["Body"].read()
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
        self.assertIn("INFO:root:Invalid Records report: https://tulib-airflow-test.s3.amazonaws.com/dpla_test/harvest_filter-invalid.csv", log.output)
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
            validate.report_s3_schematron(**self.kwargs)
        self.assertIn("INFO:root:Validating & Reporting On File: dpla_test/transformed/sch-oai-valid.xml", log.output)
        test_report_objects = conn.list_objects(Bucket=bucket, Prefix=self.kwargs.get("destination_prefix") + "-report.csv")
        self.assertEqual(test_report_objects["ResponseMetadata"]["HTTPStatusCode"], 200)
        test_report_objects_ar = [object.get("Key") for object in test_report_objects["Contents"]]
        self.assertEqual(test_report_objects_ar, ["dpla_test/transformed-filtered-report.csv"])
        test_report_content = conn.get_object(Bucket=bucket, Key="dpla_test/transformed-filtered-report.csv")["Body"].read()
        self.assertEqual(test_report_content, b'id,report,record,source_file\r\nvalid,"b\'<svrl:schematron-output xmlns:svrl=""http://purl.oclc.org/dsdl/svrl"" xmlns:xs=""http://www.w3.org/2001/XMLSchema"" xmlns:schold=""http://www.ascc.net/xml/schematron"" xmlns:sch=""http://www.ascc.net/xml/schematron"" xmlns:iso=""http://purl.oclc.org/dsdl/schematron"" xmlns:dcterms=""http://purl.org/dc/terms/"" xmlns:edm=""http://www.europeana.eu/schemas/edm/"" xmlns:oai_dc=""http://www.openarchives.org/OAI/2.0/oai_dc/"" title="""" schemaVersion=""""><!--  &#160;\\n\\t\\t  &#160;\\n\\t\\t  &#160;\\n\\t\\t --><svrl:ns-prefix-in-attribute-values uri=""http://purl.org/dc/terms/"" prefix=""dcterms""/><svrl:ns-prefix-in-attribute-values uri=""http://www.europeana.eu/schemas/edm/"" prefix=""edm""/><svrl:ns-prefix-in-attribute-values uri=""http://www.openarchives.org/OAI/2.0/oai_dc/"" prefix=""oai_dc""/><svrl:active-pattern id=""RequiredElementsPattern"" name=""Required PA Digital Elements""/><svrl:fired-rule context=""oai_dc:dc""/><svrl:active-pattern id=""TitleElementPattern"" name=""Additional Title Requirements""/><svrl:fired-rule context=""oai_dc:dc/dcterms:title""/><svrl:active-pattern id=""DCTRightsElementPattern"" name=""Additional Rights Requirements""/><svrl:fired-rule context=""oai_dc:dc/dcterms:rights""/><svrl:active-pattern id=""EDMRightsElementPattern"" name=""Additional Rights Requirements""/><svrl:active-pattern id=""ItemURLElementPattern"" name=""Additional Trackback URL Requirements""/><svrl:fired-rule context=""oai_dc:dc/edm:isShownAt""/><svrl:active-pattern id=""EDMDataProviderElementPattern"" name=""Additional Contributing Institution Requirements""/><svrl:fired-rule context=""oai_dc:dc/edm:dataProvider""/></svrl:schematron-output>\'","b\'<oai_dc:dc xmlns:oai_dc=""http://www.openarchives.org/OAI/2.0/oai_dc/"" xmlns:dc=""http://purl.org/dc/elements/1.1/"" xmlns:dcterms=""http://purl.org/dc/terms/"" xmlns:edm=""http://www.europeana.eu/schemas/edm/"" xmlns:dpla=""http://dp.la/about/map/"" xmlns:schema=""http://schema.org"" xmlns:oai=""http://www.openarchives.org/OAI/2.0/"" xmlns:oai_qdc=""http://worldcat.org/xmlschemas/qdc-1.0/"" airflow-record-id=""valid"">\\n      <dcterms:title>Celebration at Independence Hall</dcterms:title>\\n      <dcterms:creator>pdcp_noharvest</dcterms:creator>\\n      <dcterms:creator>Philadelphia Evening Bulletin</dcterms:creator>\\n      <dcterms:publisher>Philadelphia Evening Bulletin</dcterms:publisher>\\n      <dcterms:publisher>Philadelphia Evening Bulletin</dcterms:publisher>\\n      <dcterms:description>Photo shows pennants and streamers hanging from clock and bell tower of\\n         Independence Hall and Commodore John Barry monument. There is a tent and loud speaker\\n         system immediately in front of Independence Hall entrance. Note dirigible in left upper\\n         quadrant of photo.</dcterms:description>\\n      <dcterms:description>This is a test record from the PDCP Metadata Team. Please do not\\n         reference this item for research. Thank you.</dcterms:description>\\n      <dcterms:spatial>Philadelphia (Pa.);Independence National Historical Park (Philadelphia,\\n         Pa.)</dcterms:spatial>\\n      <dcterms:date>1924</dcterms:date>\\n      <dcterms:subject>Statues</dcterms:subject>\\n      <dcterms:subject/>\\n      <dcterms:type>Image</dcterms:type>\\n      <dcterms:rights>This material is made available for private study, scholarship, and research\\n         use. For access to the original, contact: Temple University Libraries. Special Collections\\n         Research Center, scrc@temple.edu, 215-204-8257.</dcterms:rights>\\n      <schema:fileFormat>image/jp2</schema:fileFormat>\\n      <dcterms:identifier>valid</dcterms:identifier>\\n      <edm:isShownAt>http://digital.library.temple.edu/cdm/ref/collection/p16002coll25/id/0</edm:isShownAt>\\n      <edm:dataProvider>Temple University Libraries</edm:dataProvider>\\n      <edm:provider>PA Digital</edm:provider>\\n   </oai_dc:dc>\\n   \'",dpla_test/transformed/sch-oai-valid.xml\r\nvalid2,"b\'<svrl:schematron-output xmlns:svrl=""http://purl.oclc.org/dsdl/svrl"" xmlns:xs=""http://www.w3.org/2001/XMLSchema"" xmlns:schold=""http://www.ascc.net/xml/schematron"" xmlns:sch=""http://www.ascc.net/xml/schematron"" xmlns:iso=""http://purl.oclc.org/dsdl/schematron"" xmlns:dcterms=""http://purl.org/dc/terms/"" xmlns:edm=""http://www.europeana.eu/schemas/edm/"" xmlns:oai_dc=""http://www.openarchives.org/OAI/2.0/oai_dc/"" title="""" schemaVersion=""""><!--  &#160;\\n\\t\\t  &#160;\\n\\t\\t  &#160;\\n\\t\\t --><svrl:ns-prefix-in-attribute-values uri=""http://purl.org/dc/terms/"" prefix=""dcterms""/><svrl:ns-prefix-in-attribute-values uri=""http://www.europeana.eu/schemas/edm/"" prefix=""edm""/><svrl:ns-prefix-in-attribute-values uri=""http://www.openarchives.org/OAI/2.0/oai_dc/"" prefix=""oai_dc""/><svrl:active-pattern id=""RequiredElementsPattern"" name=""Required PA Digital Elements""/><svrl:fired-rule context=""oai_dc:dc""/><svrl:active-pattern id=""TitleElementPattern"" name=""Additional Title Requirements""/><svrl:fired-rule context=""oai_dc:dc/dcterms:title""/><svrl:active-pattern id=""DCTRightsElementPattern"" name=""Additional Rights Requirements""/><svrl:fired-rule context=""oai_dc:dc/dcterms:rights""/><svrl:active-pattern id=""EDMRightsElementPattern"" name=""Additional Rights Requirements""/><svrl:active-pattern id=""ItemURLElementPattern"" name=""Additional Trackback URL Requirements""/><svrl:fired-rule context=""oai_dc:dc/edm:isShownAt""/><svrl:active-pattern id=""EDMDataProviderElementPattern"" name=""Additional Contributing Institution Requirements""/><svrl:fired-rule context=""oai_dc:dc/edm:dataProvider""/></svrl:schematron-output>\'","b\'<oai_dc:dc xmlns:dc=""http://purl.org/dc/elements/1.1/"" xmlns:dcterms=""http://purl.org/dc/terms/"" xmlns:edm=""http://www.europeana.eu/schemas/edm/"" xmlns:oai_dc=""http://www.openarchives.org/OAI/2.0/oai_dc/"" xmlns:dpla=""http://dp.la/about/map/"" xmlns:schema=""http://schema.org"" xmlns:oai=""http://www.openarchives.org/OAI/2.0/"" xmlns:oai_qdc=""http://worldcat.org/xmlschemas/qdc-1.0/"" airflow-record-id=""valid2"">\\n      <dcterms:title>Micky Dolenz of the Monkees with the Temple owl mascot and a Temple University\\n         football player</dcterms:title>\\n      <dcterms:description>Micky Dolenz identified from other photos of the Monkees rock group. The\\n         Monkees played a concert at a Temple University football game. The date of the\\n         concert/football game was found to be 1986 through research in newspaper\\n         databases.</dcterms:description>\\n      <dcterms:spatial>1985-1987</dcterms:spatial>\\n      <dcterms:temporal>1985-1987</dcterms:temporal>\\n      <dcterms:date>circa=1986</dcterms:date>\\n      <dcterms:subject>Football players</dcterms:subject>\\n      <dcterms:subject>Mascots</dcterms:subject>\\n      <dcterms:type>Image</dcterms:type>\\n      <dcterms:rights>This material is subject to copyright law and is made available for private\\n         study, scholarship, and research purposes only. For access to the original or a high\\n         resolution reproduction, and for permission to publish, please contact Temple University\\n         Libraries, Special Collection Research Center, scrc@temple.edu,\\n         215-204-8257.</dcterms:rights>\\n      <schema:fileFormat>image/jp2;</schema:fileFormat>\\n      <dcterms:identifier>valid2</dcterms:identifier>\\n      <edm:isShownAt>http://digital.library.temple.edu/cdm/ref/collection/p16002coll25/id/34</edm:isShownAt>\\n      <edm:dataProvider>Temple University Libraries</edm:dataProvider>\\n      <edm:provider>PA Digital</edm:provider>\\n   </oai_dc:dc>\\n   \'",dpla_test/transformed/sch-oai-valid.xml\r\nvalid3,"b\'<svrl:schematron-output xmlns:svrl=""http://purl.oclc.org/dsdl/svrl"" xmlns:xs=""http://www.w3.org/2001/XMLSchema"" xmlns:schold=""http://www.ascc.net/xml/schematron"" xmlns:sch=""http://www.ascc.net/xml/schematron"" xmlns:iso=""http://purl.oclc.org/dsdl/schematron"" xmlns:dcterms=""http://purl.org/dc/terms/"" xmlns:edm=""http://www.europeana.eu/schemas/edm/"" xmlns:oai_dc=""http://www.openarchives.org/OAI/2.0/oai_dc/"" title="""" schemaVersion=""""><!--  &#160;\\n\\t\\t  &#160;\\n\\t\\t  &#160;\\n\\t\\t --><svrl:ns-prefix-in-attribute-values uri=""http://purl.org/dc/terms/"" prefix=""dcterms""/><svrl:ns-prefix-in-attribute-values uri=""http://www.europeana.eu/schemas/edm/"" prefix=""edm""/><svrl:ns-prefix-in-attribute-values uri=""http://www.openarchives.org/OAI/2.0/oai_dc/"" prefix=""oai_dc""/><svrl:active-pattern id=""RequiredElementsPattern"" name=""Required PA Digital Elements""/><svrl:fired-rule context=""oai_dc:dc""/><svrl:active-pattern id=""TitleElementPattern"" name=""Additional Title Requirements""/><svrl:fired-rule context=""oai_dc:dc/dcterms:title""/><svrl:active-pattern id=""DCTRightsElementPattern"" name=""Additional Rights Requirements""/><svrl:fired-rule context=""oai_dc:dc/dcterms:rights""/><svrl:active-pattern id=""EDMRightsElementPattern"" name=""Additional Rights Requirements""/><svrl:active-pattern id=""ItemURLElementPattern"" name=""Additional Trackback URL Requirements""/><svrl:fired-rule context=""oai_dc:dc/edm:isShownAt""/><svrl:active-pattern id=""EDMDataProviderElementPattern"" name=""Additional Contributing Institution Requirements""/><svrl:fired-rule context=""oai_dc:dc/edm:dataProvider""/></svrl:schematron-output>\'","b\'<oai_dc:dc xmlns:dc=""http://purl.org/dc/elements/1.1/"" xmlns:dcterms=""http://purl.org/dc/terms/"" xmlns:edm=""http://www.europeana.eu/schemas/edm/"" xmlns:oai_dc=""http://www.openarchives.org/OAI/2.0/oai_dc/"" xmlns:dpla=""http://dp.la/about/map/"" xmlns:schema=""http://schema.org"" xmlns:oai=""http://www.openarchives.org/OAI/2.0/"" xmlns:oai_qdc=""http://worldcat.org/xmlschemas/qdc-1.0/"" airflow-record-id=""valid3"">\\n      <dcterms:title>Russell Conwell and others at the dedication of Conwell Hall</dcterms:title>\\n      <dcterms:description>""Participants in the dedication of Conwell Hall in January of 1924\\n         included (from the left) Josiah Penniman, President of the University of Pennsylvania; the\\n         Rev. Floyd Tomkins, rector of Holy Trinity Protestant Episcopal Church; President Conwell;\\n         Charles E. Beury, Trustee and Chairman of the Building Committee; Laura Carnell, Associate\\n         President; Wilmer Krusen, Vice President of the University and Philadelphia\\\'s Director of\\n         Health.""</dcterms:description>\\n      <dcterms:description>This is a test record from the PDCP Metadata Team. Please do not\\n         reference this item for research. Thank you.</dcterms:description>\\n      <dcterms:description>Test local rights and DPLA rights.</dcterms:description>\\n      <dcterms:date>1924-01-23</dcterms:date>\\n      <dcterms:subject>Conwell, Russell Herman, 1843-1925</dcterms:subject>\\n      <dcterms:subject> Penniman, Josiah Harmar, 1868-1941</dcterms:subject>\\n      <dcterms:subject> Tomkins, Floyd W. (Floyd Williams), 1850-1932</dcterms:subject>\\n      <dcterms:subject> Beury, Charles Ezra, 1879-1953</dcterms:subject>\\n      <dcterms:subject> Carnell, Laura, 1867-1929</dcterms:subject>\\n      <dcterms:subject>Krusen, Wilmer</dcterms:subject>\\n      <dcterms:type>Image</dcterms:type>\\n      <dcterms:rights>This material is subject to copyright law and is made available for private\\n         study, scholarship, and research purposes only. For access to the original or a high\\n         resolution reproduction, and for permission to publish, please contact Temple University\\n         Libraries, Special Collections Research Center, scrc@temple.edu,\\n         215-204-8257.</dcterms:rights>\\n      <schema:fileFormat>image/jp2;</schema:fileFormat>\\n      <dcterms:identifier>valid3</dcterms:identifier>\\n      <edm:isShownAt>http://digital.library.temple.edu/cdm/ref/collection/p16002coll25/id/35</edm:isShownAt>\\n      <edm:dataProvider>Temple University Libraries</edm:dataProvider>\\n      <edm:provider>PA Digital</edm:provider>\\n   </oai_dc:dc>\\n\'",dpla_test/transformed/sch-oai-valid.xml\r\n')
        self.assertIn(b'id,report,record,source_file\r\nvalid', test_report_content)


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
            validate.report_s3_schematron(**self.kwargs)
        self.assertIn("INFO:root:Validating & Reporting On File: dpla_test/transformed/sch-oai-invalid.xml", log.output)
        test_report_objects = conn.list_objects(Bucket=bucket, Prefix=self.kwargs.get("destination_prefix") + "-report.csv")
        self.assertEqual(test_report_objects["ResponseMetadata"]["HTTPStatusCode"], 200)
        test_report_objects_ar = [object.get("Key") for object in test_report_objects["Contents"]]
        self.assertEqual(test_report_objects_ar, ["dpla_test/transformed-filtered-report.csv"])
        test_report_content = conn.get_object(Bucket=bucket, Key="dpla_test/transformed-filtered-report.csv")["Body"].read()
        self.assertIn(b'id,report,record,source_file\r\ninvalid-missingtitle,"b\'<svrl:schematron-output', test_report_content)
        self.assertIn(b'\r\ninvalid-missingrights,"b\'<svrl:schematron-output', test_report_content)
        self.assertIn(b'\r\ninvalid-missingitemurl,"b\'<svrl:schematron-output', test_report_content)


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
            validate.report_s3_schematron(**self.kwargs)
        self.assertIn("INFO:root:Validating & Reporting On File: dpla_test/transformed/sch-oai-mix.xml", log.output)
        test_report_objects = conn.list_objects(Bucket=bucket, Prefix=self.kwargs.get("destination_prefix") + "-report.csv")
        self.assertEqual(test_report_objects["ResponseMetadata"]["HTTPStatusCode"], 200)
        test_report_objects_ar = [object.get("Key") for object in test_report_objects["Contents"]]
        self.assertEqual(test_report_objects_ar, ["dpla_test/transformed-filtered-report.csv"])
        test_report_content = conn.get_object(Bucket=bucket, Key="dpla_test/transformed-filtered-report.csv")["Body"].read()
        self.assertIn(b'id,report,record,source_file\r\nvalid,"b\'<svrl:schematron-output', test_report_content)
        self.assertIn(b'\r\ninvalid-missingtitle,"b\'<svrl:schematron-output', test_report_content)
        self.assertIn(b'\r\ninvalid-missingrights,"b\'<svrl:schematron-output', test_report_content)
        self.assertIn(b'\r\ninvalid-missingitemurl,"b\'<svrl:schematron-output', test_report_content)


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
            validate.report_s3_schematron(**self.kwargs)
        self.assertIn("INFO:root:Validating & Reporting On File: dpla_test/transformed/sch-oai-empty.xml", log.output)
        test_report_objects = conn.list_objects(Bucket=bucket, Prefix=self.kwargs.get("destination_prefix") + "-report.csv")
        self.assertEqual(test_report_objects["ResponseMetadata"]["HTTPStatusCode"], 200)
        test_report_objects_ar = [object.get("Key") for object in test_report_objects["Contents"]]
        self.assertEqual(test_report_objects_ar, ["dpla_test/transformed-filtered-report.csv"])
        test_report_content = conn.get_object(Bucket=bucket, Key="dpla_test/transformed-filtered-report.csv")["Body"].read()
        self.assertIn(b'id,report,record,source_file\r\n', test_report_content)

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
            validate.report_s3_schematron(**self.kwargs)
        self.assertEqual(log.output, ["INFO:root:Records report: https://tulib-airflow-test.s3.amazonaws.com/dpla_test/transformed-filtered-report.csv"])
        test_report_objects = conn.list_objects(Bucket=bucket, Prefix=self.kwargs.get("destination_prefix") + "-report.csv")
        self.assertEqual(test_report_objects["ResponseMetadata"]["HTTPStatusCode"], 200)
        test_report_objects_ar = [object.get("Key") for object in test_report_objects["Contents"]]
        self.assertEqual(test_report_objects_ar, ["dpla_test/transformed-filtered-report.csv"])
        test_report_content = conn.get_object(Bucket=bucket, Key="dpla_test/transformed-filtered-report.csv")["Body"].read()
        self.assertEqual(b'id,report,record,source_file\r\n', test_report_content)
