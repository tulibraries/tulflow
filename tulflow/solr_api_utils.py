import requests
import logging
from re import split as resplit
from collections import defaultdict

class SolrApiUtils():

    @classmethod
    def remove_and_recreate_collection_from_alias(cls, collection, alias, solr_url, configset="_default", solr_port=None, solr_auth_user=None, solr_auth_pass=None):
        url = solr_url + (f":{solr_port}" if solr_port else "")
        logging.info(f"Trying {url}")
        sc = cls(url, auth_user=solr_auth_user, auth_pass=solr_auth_pass)
        sc.remove_collection_from_alias(collection=collection, alias=alias)
        sc.delete_collection(collection)
        sc.create_collection(collection=collection, configset=configset)
        sc.add_collection_to_alias(collection=collection, alias=alias)

    def __init__(self, solr_url, auth_user=None, auth_pass=None):
        self.solr_url = solr_url
        self.auth_user = auth_user
        self.auth_pass = auth_pass

    def get_from_solr_api(self, path):
        url = self.solr_url + path
        logging.info(f"Requesting {url}")
        if self.auth_user and self.auth_pass:
            return requests.get(url, auth=(self.auth_user, self.auth_pass))
        else:
            return requests.get(url)

    def get_configsets(self):
        if not hasattr(self, "configsets"):
            json_response = self.get_from_solr_api("/api/cluster/configs?omitHeader=true").json()
            try:
                self.configsets = json_response['configSets']
            except KeyError as e:
                msg = json_response.get('error',{}).get("msg", "Unknown error from Solr")
                raise KeyError(f"{e}: caused by\n{msg}")
        return self.configsets

    def most_recent_configsets(self):
        configs = defaultdict(set)
        for configset in self.get_configsets():
            split = resplit(r'-(\d+)', configset)
            if len(split) > 1:
                config, version, *_ = split
                configs[config].add(int(version))
        return [f"{configset}-{max(versions)}" for (configset,versions) in configs.items()]

    def get_collections(self):
        if not hasattr(self, "collections"):
            self.collections = self.get_from_solr_api("/solr/admin/collections?action=List").json()['collections']
        return self.collections

    def delete_collection(self, collection):
        if self.collection_exists(collection):
            path = f"/solr/admin/collections?action=DELETE&name={collection}"
            response = self.get_from_solr_api(path)
            try:
                response.raise_for_status()
            except requests.exceptions.HTTPError as e:
                msg = response.json().get('error',{}).get("msg", "Unknown error from Solr")
                raise requests.exceptions.HTTPError(f"{e} caused by\n{msg}")
            self.__unsetattr("collections")
            logging.info(f"Collection {collection} deleted")
        else:
            logging.info(f"Collection {collection} did not exist. Continuing without error")

    def create_collection(self, collection, configset="_default" ,numShards=1, replicationFactor=1, maxShardsPerNode=1 ):
        path = f"/solr/admin/collections?action=CREATE&name={collection}&collection.configSet=configset&numShards=1&replicationFactor=1&maxShardsPerNode=1"
        response = self.get_from_solr_api(path)
        try:
            response.raise_for_status()
        except requests.exceptions.HTTPError as e:
            msg = response.json().get('error',{}).get("msg", "Unknown error from Solr")
            raise requests.exceptions.HTTPError(f"{e} caused by\n{msg}")
        self.__unsetattr("collections")
        logging.info(f"Collection {collection} created")

    def collection_exists(self, collection):
        return collection in self.get_collections()

    def get_aliases(self):
        if not hasattr(self, "aliases"):
            self.aliases = self.get_from_solr_api("/solr/admin/collections?action=ListAliases").json()['aliases']
        return self.aliases

    def create_or_modify_alias_and_set_collections(self, alias, collections):
        collectionlist = ",".join(collections)
        path = f"/solr/admin/collections?action=CREATEALIAS&name={alias}&collections={collectionlist}"
        response = self.get_from_solr_api(path)
        try:
            response.raise_for_status()
        except requests.exceptions.HTTPError as e:
            msg = response.json().get('error',{}).get("msg", "Unknown error from Solr")
            raise requests.exceptions.HTTPError(f"{e} caused by\n{msg}")
        logging.info(f"{alias} is now an alias for collections {collectionlist}")
        # Force HTTP call for updated state of aliases
        self.__unsetattr("aliases")

    def get_alias_collections(self, alias):
        if self.alias_exists(alias):
            return self.get_aliases().get(alias).split(",")
        else:
            raise LookupError(f"{alias} is not an existing alias")

    def is_collection_in_alias(self, collection, alias):
        return collection in self.get_alias_collections(alias)

    def alias_exists(self, alias):
        return alias in self.get_aliases()

    def remove_collection_from_alias(self, collection, alias):
        if self.is_collection_in_alias(collection, alias):
            collections = self.get_alias_collections(alias)
            collections.remove(collection)
            if len(collections) > 0:
                self.create_or_modify_alias_and_set_collections(alias=alias, collections=collections)
                self.__unsetattr("aliases")
            else:
                raise ValueError("Cannot delete only collection from alias")
        else:
            logging.info(f"Collection {collection} not already in alias {alias}. Moving on without erroring")

    def add_collection_to_alias(self, collection, alias):
        collections = self.get_alias_collections(alias)
        collections.append(collection)
        self.create_or_modify_alias_and_set_collections(alias=alias, collections=collections)
        self.__unsetattr("aliases")

    def __unsetattr(self, attr):
        if hasattr(self, attr):
            delattr(self, attr)
