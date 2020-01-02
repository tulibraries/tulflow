"""Submodule for interating with SolrCloud API *not* through Airflow Connections."""
import logging
from collections import defaultdict
from re import split as resplit
import requests


class SolrApiUtils():
    """Class for interacting with SolrCloud API over HTTP."""

    @classmethod
    def remove_and_recreate_collection_from_alias(cls, collection, alias, solr_url, configset="_default", solr_port=None, solr_auth_user=None, solr_auth_pass=None, numShards=None, replicationFactor=None, maxShardsPerNode=None):
        """Method to remove & re-add collection to SolrCloud Alias."""
        url = solr_url + (f":{solr_port}" if solr_port else "")
        logging.info("Trying %s", url)
        sc = cls(url, auth_user=solr_auth_user, auth_pass=solr_auth_pass)
        sc.remove_collection_from_alias(collection=collection, alias=alias)
        sc.delete_collection(collection)
        create_params = {"collection": collection, "configset": configset, "numShards": numShards, "replicationFactor": replicationFactor, "maxShardsPerNode": maxShardsPerNode}
        cleaned_create_params = {k: v for k, v in create_params.items() if v is not None}
        sc.create_collection(**cleaned_create_params)
        sc.add_collection_to_alias(collection=collection, alias=alias)

    def __init__(self, solr_url, auth_user=None, auth_pass=None):
        self.solr_url = solr_url
        self.auth_user = auth_user
        self.auth_pass = auth_pass

    def get_from_solr_api(self, path):
        """Issue HTTP Get Request to SolrCloud API."""
        url = self.solr_url + path
        logging.info("Requesting %s", url)
        if self.auth_user and self.auth_pass:
            return requests.get(url, auth=(self.auth_user, self.auth_pass))
        return requests.get(url)

    def get_configsets(self):
        """Issue HTTP Get Request to SolrCloud API to retrieve ConfigSets List."""
        if not hasattr(self, "configsets"):
            json_response = self.get_from_solr_api("/api/cluster/configs?omitHeader=true").json()
            try:
                self.configsets = json_response['configSets']
            except KeyError as solr_error:
                msg = json_response.get('error', {}).get("msg", "Unknown error from Solr")
                raise KeyError(f"{solr_error}: caused by\n{msg}")
        return self.configsets

    def most_recent_configsets(self):
        """Issue HTTP Get Request to SolrCloud API to retrieve Recent ConfigSets."""
        configs = defaultdict(set)
        for configset in self.get_configsets():
            split = resplit(r'-(\d+)', configset)
            if len(split) > 1:
                config, version, *_ = split
                configs[config].add(int(version))
        return [f"{configset}-{max(versions)}" for (configset, versions) in configs.items()]

    def get_collections(self):
        """Issue HTTP Get Request to SolrCloud API to retrieve Collections List."""
        if not hasattr(self, "collections"):
            self.collections = self.get_from_solr_api("/solr/admin/collections?action=List").json()['collections']
        return self.collections

    def delete_collection(self, collection):
        """Issue HTTP Get Request to SolrCloud API to delete a Collection."""
        if self.collection_exists(collection):
            path = "/solr/admin/collections?action=DELETE&name=" + collection
            response = self.get_from_solr_api(path)
            try:
                response.raise_for_status()
            except requests.exceptions.HTTPError as http_error:
                msg = response.json().get('error', {}).get("msg", "Unknown error from Solr")
                raise requests.exceptions.HTTPError(f"{http_error} caused by\n{msg}")
            self.__unsetattr("collections")
            logging.info("Collection %s deleted", collection)
        else:
            logging.info("Collection %s did not exist. Continuing without error", collection)

    def create_collection(self, collection, configset="_default", numShards=1, replicationFactor=1, maxShardsPerNode=1):
        """Create a SolrCloud collection (not through Airflow Task; that is in tasks.py)"""
        path = f"/solr/admin/collections?action=CREATE&name={collection}&collection.configName={configset}&numShards={numShards}&replicationFactor={replicationFactor}&maxShardsPerNode={maxShardsPerNode}"
        response = self.get_from_solr_api(path)
        try:
            response.raise_for_status()
        except requests.exceptions.HTTPError as http_error:
            msg = response.json().get('error', {}).get("msg", "Unknown error from Solr")
            raise requests.exceptions.HTTPError(f"{http_error} caused by\n{msg}")
        self.__unsetattr("collections")
        logging.info("Collection %s created", collection)

    def collection_exists(self, collection):
        """Check if a SolrCloud Collection already exists."""
        return collection in self.get_collections()

    def get_aliases(self):
        """Issue HTTP Get Request to SolrCloud API to retrieve Aliases List."""
        if not hasattr(self, "aliases"):
            self.aliases = self.get_from_solr_api("/solr/admin/collections?action=ListAliases").json()['aliases']
        return self.aliases

    def create_or_modify_alias_and_set_collections(self, alias, collections):
        """Issue HTTP Get Request to SolrCloud API to update an Alias' Collections"""
        collectionlist = ",".join(collections)
        path = f"/solr/admin/collections?action=CREATEALIAS&name={alias}&collections={collectionlist}"
        response = self.get_from_solr_api(path)
        try:
            response.raise_for_status()
        except requests.exceptions.HTTPError as http_error:
            msg = response.json().get('error', {}).get("msg", "Unknown error from Solr")
            raise requests.exceptions.HTTPError(f"{http_error} caused by\n{msg}")
        logging.info("%s is now an alias for collections %s", alias, collectionlist)
        # Force HTTP call for updated state of aliases
        self.__unsetattr("aliases")

    def get_alias_collections(self, alias):
        """Issue HTTP Get Request to SolrCloud API to get Collections behind an Alias."""
        if self.alias_exists(alias):
            return self.get_aliases().get(alias).split(",")
        raise LookupError(f"{alias} is not an existing alias")

    def get_alias_collections_without_init(self, alias):
        """Get list of aliases without the collection used to initialize an alias"""
        collections = self.get_alias_collections(alias)
        return self.filter_init_collection(collections)


    def is_collection_in_alias(self, collection, alias):
        """Issue HTTP Get Request to SolrCloud API to see if Collection is behind an Alias."""
        return collection in self.get_alias_collections(alias)

    def alias_exists(self, alias):
        """Check if a SolrCloud Alias Exists."""
        return alias in self.get_aliases()

    def remove_collection_from_alias(self, collection, alias):
        """Remove a SolrCloud Collection from a given Alias."""
        if self.is_collection_in_alias(collection, alias):
            collections = self.get_alias_collections(alias)
            collections.remove(collection)
            if len(collections) > 0:
                self.create_or_modify_alias_and_set_collections(alias=alias, collections=collections)
                self.__unsetattr("aliases")
            else:
                raise ValueError("Cannot delete only collection from alias")
        else:
            logging.info("Collection %s not already in alias %s. Moving on w/o error", collection, alias)

    def add_collection_to_alias(self, collection, alias):
        """Add a SolrCloud Collection to an Existing Alias."""
        collections = self.get_alias_collections_without_init(alias)
        collections.append(collection)
        self.create_or_modify_alias_and_set_collections(alias=alias, collections=collections)
        self.__unsetattr("aliases")

    def filter_init_collection(self, collections_list, init_collection_name=None):
        """Remove initial dummy collection added to create alias"""
        if not init_collection_name:
            test = lambda collection: not collection.endswith("-init")
        else:
            test = lambda collection: (collection != init_collection_name)
        return [collection for collection in collections_list if test(collection)]



    def __unsetattr(self, attr):
        if hasattr(self, attr):
            delattr(self, attr)
