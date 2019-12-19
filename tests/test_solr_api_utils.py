"""Tests suite for tulflow harvest (Functions for harvesting OAI in Airflow Tasks)."""
import json
from re import compile as re_compile
import unittest
import requests_mock
from tulflow.solr_api_utils import SolrApiUtils
from unittest.mock import Mock, patch


class TestSolrApiUtils(unittest.TestCase):
    """Test Class for Solr Cloud api functions."""
    def setUp(self):
        self.sc_url = "https://sc.example.com"
        self.matcher = re_compile(f"{self.sc_url}/.*")
        self.solrcloud = SolrApiUtils(self.sc_url)

    @requests_mock.mock()
    def test_get_configsets(self, rm):
        response = json.dumps({"configSets":["test", "_default", "test2"]})
        rm.get(self.matcher, text=response)
        configsets = self.solrcloud.get_configsets()
        self.assertEqual(configsets, ["test", "_default", "test2"])

    @requests_mock.mock()
    def test_get_collections(self, rm):
        response = json.dumps({
            "responseHeader": {"status":0, "QTime":1},
            "collections": ["test_collection1", "test_collection2"]
        })
        rm.get(self.matcher, text=response)
        collections = self.solrcloud.get_collections()
        self.assertEqual(collections, ["test_collection1", "test_collection2"])

    @requests_mock.mock()
    def test_get_aliases(self, rm):
        response = json.dumps({
            "responseHeader": {"status":0, "QTime":1},
            "aliases": {
                "test_alias1": "test_coll1,test_coll2",
                "test_alias2": "test_coll3,test_coll4"
            }
        })
        rm.get(self.matcher, text=response)
        aliases = self.solrcloud.get_aliases()
        self.assertEqual(aliases, {
            "test_alias1": "test_coll1,test_coll2",
            "test_alias2": "test_coll3,test_coll4"
        })

    @patch('tulflow.solr_api_utils.SolrApiUtils.get_configsets')
    def test_most_recent_configsets(self, mock_get_configsets):
        mock_get_configsets.return_value = ['tul_cob-catalog-9', 'tul_cob-catalog-10', 'tul_cob-web-2', '_default', 'tul_cob-web-3']
        most_recent = self.solrcloud.most_recent_configsets()
        self.assertEqual(most_recent, ["tul_cob-catalog-10", "tul_cob-web-3"])

    @patch('tulflow.solr_api_utils.SolrApiUtils.get_alias_collections')
    def test_is_collection_in_alias(self, mock_get_alias_collections):
        alias = 'funcake-oai'
        mock_get_alias_collections.return_value = ['funcake-villanova', 'funcake-temple']
        is_present = self.solrcloud.is_collection_in_alias('funcake-villanova', alias)
        self.assertTrue(is_present)

        is_not_present = self.solrcloud.is_collection_in_alias('funcake-yale', alias)
        self.assertFalse(is_not_present)

    @patch('tulflow.solr_api_utils.SolrApiUtils.get_collections')
    def test_collection_exists(self, mock_get_collections):
        mock_get_collections.return_value = ['coll1', 'coll2', 'coll3']
        being = self.solrcloud.collection_exists('coll3')
        self.assertTrue(being)
        nothingness = self.solrcloud.collection_exists('sartre')
        self.assertFalse(nothingness)

    def test_filter_init_collection(self):
        aliases = ["test-init", "test-real", "test-realer"]
        #uses default of ending with "-init"
        filtered = self.solrcloud.filter_init_collection(aliases)
        self.assertFalse("test-init" in filtered)
        # Also accepts specific name
        filtered = self.solrcloud.filter_init_collection(aliases, "test-realer")
        self.assertFalse("test-realer" in filtered)

    @patch('tulflow.solr_api_utils.SolrApiUtils.create_or_modify_alias_and_set_collections')
    @patch('tulflow.solr_api_utils.SolrApiUtils.get_alias_collections')
    def test_remove_collection_from_alias(self, mock_get_alias_collections, mock_create_or_modify_alias_and_set_collections):
        alias = "funcake-oai"
        mock_get_alias_collections.return_value = ['funcake-villanova', 'funcake-temple']
        self.solrcloud.remove_collection_from_alias('funcake-villanova', alias)
        mock_create_or_modify_alias_and_set_collections.assert_called_with(alias=alias, collections=['funcake-temple'])
        # make sure we don't error out of the collection is not part of the alias already
        try:
            self.solrcloud.remove_collection_from_alias('funcake-not-present', alias)
        except Exception as e:
            self.fail(f"Removing a collection not present in the alias raised an error: {e}")


    @patch('tulflow.solr_api_utils.SolrApiUtils.create_or_modify_alias_and_set_collections')
    @patch('tulflow.solr_api_utils.SolrApiUtils.get_alias_collections')
    def test_add_collection_to_alias(self, mock_get_alias_collections, mock_create_or_modify_alias_and_set_collections):
        alias = "funcake-oai"
        mock_get_alias_collections.return_value = ['funcake-temple']
        self.solrcloud.add_collection_to_alias('funcake-villanova', alias)
        mock_create_or_modify_alias_and_set_collections.assert_called_with(alias=alias, collections=['funcake-temple', 'funcake-villanova'])


    @patch('tulflow.solr_api_utils.SolrApiUtils.remove_collection_from_alias')
    @patch('tulflow.solr_api_utils.SolrApiUtils.delete_collection')
    @patch('tulflow.solr_api_utils.SolrApiUtils.create_collection')
    @patch('tulflow.solr_api_utils.SolrApiUtils.add_collection_to_alias')
    def test_remove_and_recreate_collection_from_alias(self, mock_add_collection_to_alias, mock_create_collection, mock_delete_collection, mock_remove_collection_from_alias):
        collection = "test_collection"
        alias = "test_alias"
        configset = "test_configset"
        SolrApiUtils.remove_and_recreate_collection_from_alias(collection=collection, configset=configset, alias=alias, solr_url="http://sc.example.com", solr_port="443", replicationFactor=3)
        mock_remove_collection_from_alias.assert_called_with(collection=collection, alias=alias)
        mock_delete_collection.assert_called_with(collection)
        mock_create_collection.assert_called_with(collection=collection, configset=configset, replicationFactor=3)
        mock_add_collection_to_alias.assert_called_with(collection=collection, alias=alias)

    @patch('tulflow.solr_api_utils.SolrApiUtils.get_from_solr_api')
    def test_create_collection(self, mock_get_from_solr_api):
        collection = "test_collection"
        configset = "test_configset"
        numShards = 5
        replicationFactor = 5
        maxShardsPerNode = 2
        self.solrcloud.create_collection(collection=collection, configset=configset, numShards=numShards, replicationFactor=replicationFactor, maxShardsPerNode=maxShardsPerNode)
        path = f"/solr/admin/collections?action=CREATE&name={collection}&collection.configName={configset}&numShards={numShards}&replicationFactor={replicationFactor}&maxShardsPerNode={maxShardsPerNode}"
        mock_get_from_solr_api.assert_called_with(path)
