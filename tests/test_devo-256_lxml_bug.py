from tulflow import harvest
from os import listdir
import unittest
import httpretty

class TestDEVO265Bug(unittest.TestCase):
    """Test Class for S3 Post Wrapper."""


    @httpretty.activate
    def test_devo_265_lxml_bug(self, **kwargs):
        """Test Calling handling XML Element to String."""

        fixture_path = "./tests/fixtures/devo-265-failing-record.xml"
        
        body = open(fixture_path, "r").read()


        httpretty.register_uri(
            httpretty.GET,
            "http://test/combine/oai",
            body=body
        )

        kwargs["oai_endpoint"] = "http://test/combine/oai"
        kwargs["harvest_params"] = {
                "metadataPrefix": "marc21",
                "included_sets": ["blacklight"],
                "from": 2019-10-21,
                "until": None
        }

        with self.assertLogs() as log:
            response = harvest.harvest_oai(**kwargs)
            processed = harvest.process_xml(response, harvest.write_log, "test-dir", **kwargs)
