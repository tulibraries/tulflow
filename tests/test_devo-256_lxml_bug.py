import unittest
import httpretty

from tulflow import harvest


class TestDEVO265Bug(unittest.TestCase):
    """Test Class for S3 Post Wrapper."""

    @httpretty.activate
    def test_devo_265_lxml_bug(self, **kwargs):
        """Test Calling handling XML Element to String."""

        fixture_path = "./tests/fixtures/devo-265-failing-record.xml"

        with open(fixture_path, "r", encoding="utf-8") as fixture_file:
            body = fixture_file.read()

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

        with self.assertLogs():
            response = harvest.harvest_oai(**kwargs)
            harvest.process_xml(response, harvest.write_log, "test-dir", **kwargs)
