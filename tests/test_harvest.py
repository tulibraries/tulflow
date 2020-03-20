"""Tests suite for tulflow harvest (Functions for harvesting OAI in Airflow Tasks)."""
from datetime import datetime
import hashlib
import unittest
import boto3
from lxml import etree
from moto import mock_s3
from unittest import mock
from unittest.mock import patch
from airflow.hooks.S3_hook import S3Hook
from airflow.models import DAG
from airflow.utils import timezone
from lxml import etree
from sickle.iterator import OAIItemIterator
import httpretty
from tulflow import harvest
from types import SimpleNamespace
from moto import mock_s3

DEFAULT_DATE = timezone.datetime(2019, 8, 16)
NS = {
    "marc21": "http://www.loc.gov/MARC21/slim",
    "oai": "http://www.openarchives.org/OAI/2.0/"
    }

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

marc = """
<OAI-PMH xmlns="http://www.openarchives.org/OAI/2.0/"
         xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
         xsi:schemaLocation="http://www.openarchives.org/OAI/2.0/ http://www.openarchives.org/OAI/2.0/OAI-PMH.xsd">
    <responseDate>2019-11-08T13:43:00Z</responseDate>
    <request verb="ListRecords" metadataPrefix="marc21" set="blacklight">https://na02.alma.exlibrisgroup.com/view/oai/01TULI_INST/request</request>
    <ListRecords>
        <record>
            <header>
                <identifier>oai:alma.01TULI_INST:991000000269703811</identifier>
                <datestamp>2019-07-15T15:17:33Z</datestamp>
                <setSpec>blacklight</setSpec>
                <setSpec>blacklight_qa</setSpec>
                <setSpec>rapid_print_books</setSpec>
            </header>
            <metadata>
                <record xmlns="http://www.loc.gov/MARC21/slim" xsi:schemaLocation="http://www.loc.gov/MARC21/slim http://www.loc.gov/standards/marcxml/schema/MARC21slim.xsd">
                    <leader>01407nam a2200445 4500</leader>
                    <controlfield tag="005">20190715090942.0</controlfield>
                    <controlfield tag="008">690326s1969 nju b 000 0 eng </controlfield>
                    <controlfield tag="001">991000000269703811</controlfield>
                    <datafield tag="010" ind1=" " ind2=" ">
                        <subfield code="a">68020157</subfield>
                    </datafield>
                    <datafield tag="035" ind1=" " ind2=" ">
                        <subfield code="a">(PPT)b10000276-01tuli_inst</subfield>
                    </datafield>
                    <datafield tag="040" ind1=" " ind2=" ">
                        <subfield code="a">DLC</subfield>
                        <subfield code="b">eng</subfield>
                        <subfield code="c">DLC</subfield>
                        <subfield code="d">PPT</subfield>
                    </datafield>
                    <datafield tag="090" ind1=" " ind2=" ">
                        <subfield code="a">HM131.E85</subfield>
                    </datafield>
                    <datafield tag="100" ind1="1" ind2=" ">
                        <subfield code="a">Etzioni, Amitai.</subfield>
                        <subfield code="0">http://id.loc.gov/authorities/names/n79089329</subfield>
                    </datafield>
                    <datafield tag="245" ind1="1" ind2="0">
                        <subfield code="a">Readings on modern organizations.</subfield>
                    </datafield>
                </record>
            </metadata>
        </record>
        <record>
            <header status="deleted">
                <identifier>oai:alma.01TULI_INST:991000000939703811</identifier>
                <datestamp>2018-04-02T21:02:12Z</datestamp>
                <setSpec>blacklight</setSpec>
            </header>
        </record>
        <record>
            <header status="deleted">
                <identifier>oai:alma.01TULI_INST:991000000939703812</identifier>
                <datestamp>2018-04-02T21:02:12Z</datestamp>
                <setSpec>blacklight</setSpec>
            </header>
        </record>
    </ListRecords>
</OAI-PMH>
"""

marc_single = """
<record xmlns="http://www.openarchives.org/OAI/2.0/"
 xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
 xsi:schemaLocation="http://www.openarchives.org/OAI/2.0/ http://www.openarchives.org/OAI/2.0/OAI-PMH.xsd">
    <header>
        <identifier>oai:alma.01TULI_INST:991000000269703811</identifier>
        <datestamp>2019-07-15T15:17:33Z</datestamp>
        <setSpec>blacklight</setSpec>
        <setSpec>blacklight_qa</setSpec>
        <setSpec>rapid_print_books</setSpec>
    </header>
    <metadata>
        <record xmlns="http://www.loc.gov/MARC21/slim" xsi:schemaLocation="http://www.loc.gov/MARC21/slim http://www.loc.gov/standards/marcxml/schema/MARC21slim.xsd">
            <leader>01407nam a2200445 4500</leader>
            <controlfield tag="005">20190715090942.0</controlfield>
            <controlfield tag="008">690326s1969 nju b 000 0 eng </controlfield>
            <controlfield tag="001">991000000269703811</controlfield>
            <datafield tag="010" ind1=" " ind2=" ">
                <subfield code="a">68020157</subfield>
            </datafield>
            <datafield tag="035" ind1=" " ind2=" ">
                <subfield code="a">(PPT)b10000276-01tuli_inst</subfield>
            </datafield>
            <datafield tag="040" ind1=" " ind2=" ">
                <subfield code="a">DLC</subfield>
                <subfield code="b">eng</subfield>
                <subfield code="c">DLC</subfield>
                <subfield code="d">PPT</subfield>
            </datafield>
            <datafield tag="090" ind1=" " ind2=" ">
                <subfield code="a">HM131.E85</subfield>
            </datafield>
            <datafield tag="100" ind1="1" ind2=" ">
                <subfield code="a">Etzioni, Amitai.</subfield>
                <subfield code="0">http://id.loc.gov/authorities/names/n79089329</subfield>
            </datafield>
            <datafield tag="245" ind1="1" ind2="0">
                <subfield code="a">Readings on modern organizations.</subfield>
            </datafield>
        </record>
    </metadata>
</record>
"""

lookup = """child_id,parent_id,parent_xml
991000000269703811,9910367273103811,"<datafield>test</datafield>||<datafield>9910367273103811</datafield>"
"""

listSets = """
<?xml version="1.0" encoding="UTF-8"?>
<OAI-PMH
    xmlns="http://www.openarchives.org/OAI/2.0/"
    xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
    xsi:schemaLocation="http://www.openarchives.org/OAI/2.0/ http://www.openarchives.org/OAI/2.0/OAI-PMH.xsd">
    <responseDate>2019-11-11T21:17:32Z</responseDate>
    <request verb="ListSets">http://digitalcollections.plymouth.edu/oai/oai.php</request>
    <ListSets>
        <set>
            <setSpec>a</setSpec>
            <setName>A</setName>
            <setDescription>
                <oai_dc:dc
                    xmlns:oai_dc="http://www.openarchives.org/OAI/2.0/oai_dc/"
                    xmlns:dc="http://purl.org/dc/elements/1.1/"
                    xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
                    xsi:schemaLocation="http://www.openarchives.org/OAI/2.0/oai_dc/ http://www.openarchives.org/OAI/2.0/oai_dc.xsd" />
            </setDescription>
        </set>
        <set>
            <setSpec>b</setSpec>
            <setName>B</setName>
            <setDescription>
                <oai_dc:dc
                    xmlns:oai_dc="http://www.openarchives.org/OAI/2.0/oai_dc/"
                    xmlns:dc="http://purl.org/dc/elements/1.1/"
                    xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
                    xsi:schemaLocation="http://www.openarchives.org/OAI/2.0/oai_dc/ http://www.openarchives.org/OAI/2.0/oai_dc.xsd" />
            </setDescription>
        </set>
        <set>
            <setSpec>x</setSpec>
            <setName>X</setName>
            <setDescription>
                <oai_dc:dc
                    xmlns:oai_dc="http://www.openarchives.org/OAI/2.0/oai_dc/"
                    xmlns:dc="http://purl.org/dc/elements/1.1/"
                    xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
                    xsi:schemaLocation="http://www.openarchives.org/OAI/2.0/oai_dc/ http://www.openarchives.org/OAI/2.0/oai_dc.xsd" />
            </setDescription>
        </set>
        <set>
            <setSpec>y</setSpec>
            <setName>Y</setName>
            <setDescription>
                <oai_dc:dc
                    xmlns:oai_dc="http://www.openarchives.org/OAI/2.0/oai_dc/"
                    xmlns:dc="http://purl.org/dc/elements/1.1/"
                    xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
                    xsi:schemaLocation="http://www.openarchives.org/OAI/2.0/oai_dc/ http://www.openarchives.org/OAI/2.0/oai_dc.xsd" />
            </setDescription>
        </set>
        <set>
            <setSpec>z</setSpec>
            <setName>Z</setName>
            <setDescription>
                <oai_dc:dc
                    xmlns:oai_dc="http://www.openarchives.org/OAI/2.0/oai_dc/"
                    xmlns:dc="http://purl.org/dc/elements/1.1/"
                    xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
                    xsi:schemaLocation="http://www.openarchives.org/OAI/2.0/oai_dc/ http://www.openarchives.org/OAI/2.0/oai_dc.xsd" />
            </setDescription>
        </set>
    </ListSets>
</OAI-PMH>
"""


class TestDagS3Interaction(unittest.TestCase):
    """Test Class for S3 Post Wrapper."""
    @classmethod
    def setUpClass(self):
        self.dag_id = "s3_stuff"
        self.maxDiff = None


    def test_dag_s3_prefix(self):
        """Test Creating S3 Bucket ("prefix")."""
        timestamp = datetime.now().strftime("%Y-%m-%dT%H:%M:%SZ")
        prefix = harvest.dag_s3_prefix(self.dag_id, timestamp)
        self.assertEqual(prefix, "{}/{}".format(self.dag_id, timestamp))


    @mock.patch("tulflow.process.generate_s3_object")
    def test_write_push_string_to_s3(self, mock):
        """Test Writing String to S3 using our Function."""
        string = "<fooooooooo>"
        prefix = "this/thing/here"
        kwargs = {}
        kwargs["access_id"] = "puppies"
        kwargs["access_secret"] = "kittens"
        kwargs["bucket_name"] = "my-bucket"
        our_hash = hashlib.md5(string.encode("utf-8")).hexdigest()
        key = "{}/{}".format(prefix, our_hash)


        harvest.dag_write_string_to_s3(string=string, prefix=prefix, **kwargs)
        mock.assert_called_once_with(string, "my-bucket", key, "puppies", "kittens")


class TestOAIHarvestInteraction(unittest.TestCase):
    """Test Class for OAI Harvest Wrapper."""
    @classmethod
    def setUpClass(self):
        self.maxDiff = None


    def test_oai_sets_all(self, **kwargs):
        """Test that all sets are lined up for harvest."""
        kwargs["all_sets"] = True
        kwargs["included_sets"] = ["a", "b", "c"]
        kwargs["excluded_sets"] = ["x", "y", "z"]

        returned_sets = harvest.generate_oai_sets(**kwargs)
        self.assertEqual(returned_sets, [])


    def test_oai_sets_incl(self, **kwargs):
        """Test that all sets are lined up for harvest."""
        kwargs["all_sets"] = False
        kwargs["included_sets"] = ["a", "b", "c"]
        kwargs["excluded_sets"] = ["x", "y", "z"]

        returned_sets = harvest.generate_oai_sets(**kwargs)
        self.assertEqual(returned_sets, ["a", "b", "c"])


    @httpretty.activate
    def test_oai_sets_excl(self, **kwargs):
        """Test that all sets are lined up for harvest."""
        httpretty.register_uri(
            httpretty.GET,
            "http://127.0.0.1/combine/oai",
            body=listSets
        )
        kwargs["oai_endpoint"] = "http://127.0.0.1/combine/oai"
        kwargs["all_sets"] = False
        kwargs["included_sets"] = []
        kwargs["excluded_sets"] = ["x", "y", "z"]

        returned_sets = harvest.generate_oai_sets(**kwargs)
        returned_sets.sort()
        self.assertListEqual(returned_sets, ["a", "b"])


    @httpretty.activate
    def test_harvest_oai(self, **kwargs):
        """Test Calling OAI-PMH HTTP Endpoint & Returning XML String."""
        httpretty.register_uri(
            httpretty.GET,
            "http://127.0.0.1/combine/oai",
            body=lizards
        )

        kwargs["oai_endpoint"] = "http://127.0.0.1/combine/oai"
        kwargs["harvest_params"] = {
            "metadataPrefix": "generic",
            "included_sets": "dpla_test",
            "from": None,
            "until": None
        }

        response = harvest.harvest_oai(**kwargs)
        xml_output = etree.tostring(response.next().xml, pretty_print=True, encoding="utf-8")
        self.assertEqual(type(response), OAIItemIterator)
        self.assertIn(b"<identifier>oai:lizards</identifier>", xml_output)
        self.assertIn(b"<setSpec>dpla_test</setSpec>", xml_output)
        self.assertIn(b"<dcterms:title>lizards</dcterms:title>", xml_output)


    @httpretty.activate
    def test_process_xml_dpla(self, **kwargs):
        """Test Calling handling XML Element to String."""
        httpretty.register_uri(
            httpretty.GET,
            "http://test/combine/oai",
            body=animals
        )

        kwargs["oai_endpoint"] = "http://test/combine/oai"
        kwargs["harvest_params"] = {
            "metadataPrefix": "generic",
            "included_sets": ["dpla_test"],
            "from": None,
            "until": None
        }

        with self.assertLogs() as log:
            response = harvest.harvest_oai(**kwargs)
            processed = harvest.process_xml(response, harvest.write_log, "test-dir", **kwargs)
            self.assertIn("INFO:root:OAI Records Harvested & Processed: 2", log.output)
            self.assertIn("INFO:root:OAI Records Harvest & Marked for Deletion: 0", log.output)
            self.assertEqual(processed, {"updated": 2, "deleted": 0 })


    @httpretty.activate
    def test_process_xml_alma(self, **kwargs):
        """Test Calling handling XML Element to String with Deletes."""
        httpretty.register_uri(
            httpretty.GET,
            "http://127.0.0.1/alma/oai",
            body=marc
        )

        kwargs["oai_endpoint"] = "http://127.0.0.1/alma/oai"
        kwargs["harvest_params"] = {
            "metadataPrefix": "marc21",
            "included_sets": ["blacklight"],
            "from": 2019-10-21,
            "until": None
        }

        with self.assertLogs() as log:
            response = harvest.harvest_oai(**kwargs)
            harvest.process_xml(response, harvest.write_log, "test-dir", **kwargs)
        self.assertIn("INFO:root:OAI Records Harvested & Processed: 1", log.output)
        self.assertIn("INFO:root:OAI Records Harvest & Marked for Deletion: 2", log.output)
        self.assertIn('INFO:root:<oai:collection xmlns:oai="http://www.openarchives.org/OAI/2.0/" dag-id="no-dag-provided" dag-timestamp="no-timestamp-provided"><oai:record xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" airflow-record-id="oai:alma.01TULI_INST:991000000269703811"><oai:header><oai:identifier>oai:alma.01TULI_INST:991000000269703811</oai:identifier><oai:datestamp>2019-07-15T15:17:33Z</oai:datestamp><oai:setSpec>blacklight</oai:setSpec><oai:setSpec>blacklight_qa</oai:setSpec><oai:setSpec>rapid_print_books</oai:setSpec></oai:header><oai:metadata><record xmlns="http://www.loc.gov/MARC21/slim" xsi:schemaLocation="http://www.loc.gov/MARC21/slim http://www.loc.gov/standards/marcxml/schema/MARC21slim.xsd"><leader>01407nam a2200445 4500</leader><controlfield tag="005">20190715090942.0</controlfield><controlfield tag="008">690326s1969 nju b 000 0 eng </controlfield><controlfield tag="001">991000000269703811</controlfield><datafield tag="010" ind1=" " ind2=" "><subfield code="a">68020157</subfield></datafield><datafield tag="035" ind1=" " ind2=" "><subfield code="a">(PPT)b10000276-01tuli_inst</subfield></datafield><datafield tag="040" ind1=" " ind2=" "><subfield code="a">DLC</subfield><subfield code="b">eng</subfield><subfield code="c">DLC</subfield><subfield code="d">PPT</subfield></datafield><datafield tag="090" ind1=" " ind2=" "><subfield code="a">HM131.E85</subfield></datafield><datafield tag="100" ind1="1" ind2=" "><subfield code="a">Etzioni, Amitai.</subfield><subfield code="0">http://id.loc.gov/authorities/names/n79089329</subfield></datafield><datafield tag="245" ind1="1" ind2="0"><subfield code="a">Readings on modern organizations.</subfield></datafield></record></oai:metadata></oai:record></oai:collection>', log.output)
        # assert multiple deletions get added as expected
        self.assertIn('INFO:root:<oai:collection xmlns:oai="http://www.openarchives.org/OAI/2.0/" dag-id="no-dag-provided" dag-timestamp="no-timestamp-provided"><oai:record airflow-record-id="oai:alma.01TULI_INST:991000000939703811"><oai:header status="deleted"><oai:identifier>oai:alma.01TULI_INST:991000000939703811</oai:identifier><oai:datestamp>2018-04-02T21:02:12Z</oai:datestamp><oai:setSpec>blacklight</oai:setSpec></oai:header></oai:record><oai:record airflow-record-id="oai:alma.01TULI_INST:991000000939703812"><oai:header status="deleted"><oai:identifier>oai:alma.01TULI_INST:991000000939703812</oai:identifier><oai:datestamp>2018-04-02T21:02:12Z</oai:datestamp><oai:setSpec>blacklight</oai:setSpec></oai:header></oai:record></oai:collection>', log.output)


    @mock_s3
    def test_perform_xml_lookup_with_cache(self, **kwargs):
        """Test Calling handling XML Element to String with Deletes."""
        kwargs["access_id"] = "cats"
        kwargs["access_secret"] = "dogs"
        kwargs["bucket_name"] = "alma-test"
        kwargs["lookup_key"] = "sc_catalog_pipeline/0000-00-00/lookup.tsv"

        conn = boto3.client(
            "s3",
            aws_access_key_id=kwargs.get("access_id"),
            aws_secret_access_key=kwargs.get("access_secret")
        )
        conn.create_bucket(Bucket=kwargs.get("bucket_name"))
        conn.put_object(Bucket=kwargs.get("bucket_name"), Key=kwargs.get("lookup_key"), Body=lookup)
        marc_xml = etree.fromstring(marc_single)
        with self.assertLogs() as log:
            parser = harvest.perform_xml_lookup_with_cache()
            resp_xml = parser(marc_xml, **kwargs)
        self.assertIn("INFO:root:Reading in Record 991000000269703811", log.output)
        self.assertIn("INFO:root:Child XML record found 991000000269703811", log.output)
        self.assertIn(b"<datafield>test</datafield>", etree.tostring(resp_xml))
        self.assertIn(b"<datafield>9910367273103811</datafield>", etree.tostring(resp_xml))


    @mock.patch("tulflow.harvest.harvest_oai")
    @mock.patch("tulflow.harvest.dag_s3_prefix")
    @mock.patch("tulflow.harvest.process_xml")
    def test_oai_to_s3_harvest(self, mock_process, mock_prefix, mock_harvest, **kwargs):
        """Test oai_to_s3 wraps harvest_oai function."""
        dag = DAG(dag_id="test_slacksuccess", start_date=DEFAULT_DATE)
        kwargs["oai_endpoint"] = "http://test/combine/oai"
        kwargs["metadataPrefix"] = "blergh"
        kwargs["included_sets"] = ["set"]
        kwargs["from"] = "from"
        kwargs["until"] = "until"
        kwargs["dag"] = dag
        kwargs["timestamp"] = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")

        harvest.oai_to_s3(**kwargs)
        self.assertTrue(mock_harvest.called)
        self.assertTrue(mock_prefix.called)
        self.assertTrue(mock_process.called)


    @mock.patch("tulflow.harvest.harvest_oai")
    @mock.patch("tulflow.harvest.dag_s3_prefix")
    @mock.patch("tulflow.harvest.process_xml")
    def test_oai_to_s3_harvest_return_value_mult_set(self, mock_process, mock_prefix, mock_harvest, **kwargs):
        """Test oai_to_s3 wraps harvest_oai function."""
        dag = DAG(dag_id="test_slacksuccess", start_date=DEFAULT_DATE)
        kwargs["oai_endpoint"] = "http://test/combine/oai"
        kwargs["metadataPrefix"] = "blergh"
        kwargs["included_sets"] = ["set1", "set2"]
        kwargs["from"] = "from"
        kwargs["until"] = "until"
        kwargs["dag"] = dag
        kwargs["timestamp"] = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
        mock_process.return_value = {"updated": 2, "deleted": 0 }
        response = harvest.oai_to_s3(**kwargs)
        self.assertEqual(response, {"updated": 4, "deleted": 0 })

    @mock.patch("tulflow.harvest.harvest_oai")
    @mock.patch("tulflow.harvest.dag_s3_prefix")
    @mock.patch("tulflow.harvest.process_xml")
    def test_oai_to_s3_harvest_return_value_no_set(self, mock_process, mock_prefix, mock_harvest, **kwargs):
        """Test oai_to_s3 wraps harvest_oai function."""
        dag = DAG(dag_id="test_slacksuccess", start_date=DEFAULT_DATE)
        kwargs["oai_endpoint"] = "http://test/combine/oai"
        kwargs["metadataPrefix"] = "blergh"
        kwargs["from"] = "from"
        kwargs["until"] = "until"
        kwargs["all_sets"] = True
        kwargs["dag"] = dag
        kwargs["timestamp"] = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
        
        mock_process.return_value = {"updated": 2, "deleted": 0 }
        response = harvest.oai_to_s3(**kwargs)
        self.assertEqual(response, {"updated": 2, "deleted": 0 })

