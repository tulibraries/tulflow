"""
tulflow.harvest
~~~~~~~~~~~~~~~
This module contains objects to harvest data from one given location to another.
"""
import hashlib
import io
import logging
import pandas
from lxml import etree
from sickle import Sickle
from tulflow import process


NS = {
    "marc21": "http://www.loc.gov/MARC21/slim",
    "oai": "http://www.openarchives.org/OAI/2.0/"
    }


def oai_to_s3(**kwargs):
    """Wrapper function for using OAI Harvest, Default Processor, and S3 Writer."""
    kwargs["harvest_params"] = {
        "metadataPrefix": kwargs.get("metadata_prefix"),
        "from": kwargs.get("harvest_from_date"),
        "until": kwargs.get("harvest_until_date")
    }
    dag_id = kwargs["dag"].dag_id
    dag_start_date = kwargs["timestamp"]

    oai_sets = generate_oai_sets(**kwargs)
    all_processed = []
    if oai_sets:
        for oai_set in oai_sets:
            kwargs["harvest_params"]["set"] = oai_set
            data = harvest_oai(**kwargs)
            outdir = dag_s3_prefix(dag_id, dag_start_date)
            processed = process_xml(data, dag_write_string_to_s3, outdir, **kwargs)
            all_processed.append(processed)
    else:
        data = harvest_oai(**kwargs)
        outdir = dag_s3_prefix(dag_id, dag_start_date)
        processed = process_xml(data, dag_write_string_to_s3, outdir, **kwargs)
        all_processed.append(processed)
    all_updated = sum([set['updated'] for set in all_processed])
    all_deleted = sum([set['deleted'] for set in all_processed])
    logging.info("Total OAI Records Harvested & Processed: %s", all_updated)
    logging.info("Total OAI Records Harvest & Marked for Deletion: %s", all_deleted)
    return {"updated": all_updated, "deleted": all_deleted}


def generate_oai_sets(**kwargs):
    """Generate the oai sets we want to harvest."""
    all_sets = bool(kwargs.get("all_sets"))
    included_sets = kwargs.get("included_sets")
    excluded_sets = kwargs.get("excluded_sets")
    oai_endpoint = kwargs.get("oai_endpoint")

    if all_sets:
        logging.info("Seeing All Sets Needed.")
        return []
    elif included_sets:
        logging.info("Seeing SetSpec List.")
        if not isinstance(included_sets, list):
            return [included_sets]
        return included_sets
    elif excluded_sets:
        logging.info("Seeing Excluded SetSpec List.")
        if not isinstance(excluded_sets, list):
            excluded_sets = [excluded_sets]
        list_sets = Sickle(oai_endpoint).ListSets()
        all_sets = [oai_set.xml.find("oai:setSpec", namespaces=NS).text for oai_set in list_sets]
        remaining_sets = list(set(all_sets) - set(excluded_sets))
        logging.info(remaining_sets)
        return remaining_sets
    return []


def harvest_oai(**kwargs):
    """Create OAI ListRecords Iterator for Harvesting Data."""
    oai_endpoint = kwargs.get("oai_endpoint")
    harvest_params = kwargs.get("harvest_params")
    logging.info("Harvesting from %s", oai_endpoint)
    logging.info("Harvesting %s", harvest_params)
    request = Sickle(oai_endpoint)
    data = request.ListRecords(**harvest_params)
    return data


class OaiXml:
    """oai-pmh xml etree wrapper"""
    def __init__(self, dag_id, timestamp):
        etree.register_namespace("oai", "http://www.openarchives.org/OAI/2.0/")
        etree.register_namespace("marc21", "http://www.loc.gov/MARC21/slim")
        self.root = etree.Element("{http://www.openarchives.org/OAI/2.0/}collection")
        self.root.attrib["dag-id"] = dag_id
        self.root.attrib["dag-timestamp"] = timestamp

    def append(self, record):
        self.root.append(record)

    def tostring(self):
       return etree.tostring(self.root).decode("utf-8")


def process_xml(data, writer, outdir, **kwargs):
    """Process & Write XML data to S3."""
    parser = kwargs.get("parser")
    records_per_file = kwargs.get("records_per_file")
    if kwargs.get("dag"):
        run_id = kwargs.get("dag").dag_id
    else:
        run_id = "no-dag-provided"
    if kwargs.get("timestamp"):
        timestamp = kwargs.get("timestamp")
    else:
        timestamp = "no-timestamp-provided"
    if not records_per_file:
        records_per_file = 1000

    count = deleted_count = 0
    oai_updates = OaiXml(run_id, timestamp)
    oai_deletes = OaiXml(run_id, timestamp)
    logging.info("Processing XML")

    for record in data:
        record_id = record.header.identifier
        record = record.xml
        record.attrib["airflow-record-id"] = record_id
        if parser:
            record = parser(record, **kwargs)
        if record.xpath(".//oai:header[@status='deleted']", namespaces=NS):
            logging.info("Added record %s to deleted xml file(s)", record_id)
            deleted_count += 1
            oai_deletes.append(record)

            if deleted_count % int(records_per_file) == 0:
                writer(oai_deletes.tostring(), outdir + "/deleted", **kwargs)
                oai_deletes = OaiXml(run_id, timestamp)
        else:
            logging.info("Added record %s to new-updated xml file", record_id)
            count += 1
            oai_updates.append(record)
            if count % int(records_per_file) == 0:
                writer(oai_updates.tostring(), outdir + "/new-updated", **kwargs)
                oai_updates = OaiXml(run_id, timestamp)
    writer(oai_updates.tostring(), outdir + "/new-updated", **kwargs)
    writer(oai_deletes.tostring(), outdir + "/deleted", **kwargs)
    logging.info("OAI Records Harvested & Processed: %s", count)
    logging.info("OAI Records Harvest & Marked for Deletion: %s", deleted_count)
    return {"updated": count, "deleted": deleted_count}


def perform_xml_lookup_with_cache():
    cache = {}

    def perform_xml_lookup(oai_record, **kwargs):
        """Parse additions/updates & add boundwiths."""

        if len(cache) == 0:
            logging.info("*** Fetching CSV lookup file from s3 ***")
            access_id = kwargs.get("access_id")
            access_secret = kwargs.get("access_secret")
            bucket = kwargs.get("bucket_name")
            lookup_key = kwargs.get("lookup_key")
            csv_data = process.get_s3_content(bucket, lookup_key, access_id, access_secret)
            cache["value"] = pandas.read_csv(io.BytesIO(csv_data), header=0)

        lookup_csv = cache["value"]

        for record in oai_record.xpath(".//marc21:record", namespaces=NS):
            record_id = process.get_record_001(record)
            logging.info("Reading in Record %s", record_id)
            parent_txt = lookup_csv.loc[lookup_csv.child_id == int(record_id), "parent_xml"].values
            if len(set(parent_txt)) >= 1:
                logging.info("Child XML record found %s", record_id)
                for parent_node in parent_txt[0].split("||"):
                    try:
                        record.append(etree.fromstring(parent_node))
                    except etree.XMLSyntaxError as error:
                        logging.error("Problem with string syntax:")
                        logging.error(error)
                        logging.error(parent_node)
        return oai_record


    return perform_xml_lookup


def dag_write_string_to_s3(string, prefix, **kwargs):
    """Push a string in memory to s3 with a defined prefix"""
    access_id = kwargs.get("access_id")
    access_secret = kwargs.get("access_secret")
    bucket_name = kwargs.get("bucket_name")
    logging.info("Writing to S3 Bucket %s", bucket_name)

    our_hash = hashlib.md5(string.encode("utf-8")).hexdigest()
    filename = "{}/{}".format(prefix, our_hash)
    process.generate_s3_object(string, bucket_name, filename, access_id, access_secret)


def write_log(string, prefix, **kwargs):
    """Write the data to logging info."""
    prefix = prefix
    logging.info(prefix)
    string = string
    logging.info(string)


def dag_s3_prefix(dag_id, timestamp):
    """Define the prefix that will be prepended to all files created by this dag run"""
    return "{}/{}".format(dag_id, timestamp)
