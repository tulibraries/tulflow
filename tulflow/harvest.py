"""
tulflow.harvest
~~~~~~~~~~~~~~~
This module contains objects to harvest data from one given location to another.
"""
import hashlib
import io
import logging
import re
import pandas
import requests
import sickle

from lxml import etree
from sickle import Sickle
from sickle.models import xml_to_dict
from sickle.response import OAIResponse
from sickle.oaiexceptions import NoRecordsMatch
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
    sets_with_no_records = []
    if oai_sets:
        for oai_set in oai_sets:
            kwargs["harvest_params"]["set"] = oai_set
            data = harvest_oai(**kwargs)
            if data == []:
                sets_with_no_records.append(oai_set)
                logging.info("Skipping processing % set because it has no data.", oai_set)
                continue
            outdir = dag_s3_prefix(dag_id, dag_start_date)
            processed = process_xml(data, dag_write_string_to_s3, outdir, **kwargs)
            all_processed.append(processed)
    else:
        data = harvest_oai(**kwargs)
        if data == []:
            sets_with_no_records.append(oai_set)
        outdir = dag_s3_prefix(dag_id, dag_start_date)
        processed = process_xml(data, dag_write_string_to_s3, outdir, **kwargs)
        all_processed.append(processed)
    all_updated = sum(item["updated"] for item in all_processed)
    all_deleted = sum(item["deleted"] for item in all_processed)
    logging.info("Total OAI Records Harvested & Processed: %s", all_updated)
    logging.info("Total OAI Records Harvest & Marked for Deletion: %s", all_deleted)
    logging.info("Total sets with no records: %s", len(sets_with_no_records))
    logging.info("Sets with no records %s", sets_with_no_records)
    return {
        "updated": all_updated,
        "deleted": all_deleted,
        "sets_with_no_records": sets_with_no_records,
    }


def generate_oai_sets(**kwargs):
    """Generate the oai sets we want to harvest."""
    all_sets = bool(kwargs.get("all_sets"))
    included_sets = kwargs.get("included_sets")
    excluded_sets = kwargs.get("excluded_sets")
    oai_endpoint = kwargs.get("oai_endpoint")

    if all_sets:
        logging.info("Seeing All Sets Needed.")
        return []
    if included_sets:
        logging.info("Seeing SetSpec List.")
        if not isinstance(included_sets, list):
            return [included_sets]
        return included_sets
    if excluded_sets:
        logging.info("Seeing Excluded SetSpec List.")
        if not isinstance(excluded_sets, list):
            excluded_sets = [excluded_sets]
        list_sets = Sickle(oai_endpoint).ListSets()
        all_sets = [oai_set.xml.find("oai:setSpec", namespaces=NS).text for oai_set in list_sets]
        remaining_sets = list(set(all_sets) - set(excluded_sets))
        logging.info(remaining_sets)
        return remaining_sets
    return []

def raise_oai_response_error(response):
    """Log an invalid OAI response and raise an error."""
    body = response.text

    support_id_match = re.search(
        r"support\s+id\s+(?:is|:)\s*:?\s*<?\s*([0-9]+)",
        body,
        re.IGNORECASE,
    )

    support_id = (
        support_id_match.group(1)
        if support_id_match
        else None
    )

    if not support_id:
        for name, value in response.headers.items():
            if "support" in name.lower() or "support" in value.lower():
                header_match = re.search(r"([0-9]{6,})", value)
                if header_match:
                    support_id = header_match.group(1)
                    break

    logging.error(
        "Invalid response received from OAI endpoint. "
        "URL: %s HTTP status: %s\n%s",
        response.url,
        response.status_code,
        body,
    )

    if support_id:
        logging.error(
            "OAI request rejection support ID: %s",
            support_id,
        )

        raise RuntimeError(
            "OAI endpoint returned an invalid response. "
            f"Support ID: {support_id}"
        )

    logging.error(
        "The OAI endpoint did not provide a support ID in the response."
    )

    raise RuntimeError(
        "OAI endpoint returned an invalid response. "
        "No support ID was provided by the remote server."
    )


def validate_oai_response(response):
    """Validate that an OAI endpoint returned an OAI-PMH response."""
    try:
        root_element = etree.QName(response.xml).localname
    except (etree.XMLSyntaxError, TypeError, ValueError):
        root_element = None

    if root_element == "OAI-PMH":
        return

    raise_oai_response_error(response.http_response)


class ValidatingSickle(Sickle):
    """Sickle client that validates responses from OAI endpoints."""

    def harvest(self, **kwargs):
        try:
            response = super().harvest(**kwargs)
        except requests.HTTPError as error:
            if error.response is not None:
                raise_oai_response_error(error.response)

            raise

        validate_oai_response(response)

        return response


class HarvestIterator(sickle.iterator.OAIItemIterator):
    """Custom iterator that skips deleted records and records without metadata."""

    def _next_response(self):
        """Handle noRecordsMatch responses after following a resumption token."""
        resumption_token = getattr(self, "resumption_token", None)
        following_resumption_token = bool(
            resumption_token and resumption_token.token
        )

        try:
            super()._next_response()
        except NoRecordsMatch:
            if not following_resumption_token:
                raise

            logging.warning(
                "Received noRecordsMatch while following a resumption token. "
                "Treating this as the end of the harvest. "
                "Resumption token: %s",
                resumption_token.token,
            )

            self.resumption_token = None
            self._items = iter(())

    def next(self):
        """Return the next record/header/set."""
        while True:
            for item in self._items:
                mapped = self.mapper(item)
                if self.ignore_deleted and mapped.deleted:
                    continue
                if hasattr(mapped, "metadata") and mapped.metadata is None:
                    logging.info("Skipping record with no metadata: %s", mapped.header.identifier)
                    continue
                return mapped
            if self.resumption_token and self.resumption_token.token:
                self._next_response()
            else:
                raise StopIteration


# TODO: Remove when Sickle handles records with missing metadata upstream.
# See https://github.com/mloesch/sickle/pull/47.
class HarvestRecord(sickle.models.Record):
    """Custom Sickle record that unwraps metadata children."""

    def get_metadata(self):
        meta_data = self.xml.find(".//" + self._oai_namespace + "metadata")
        if meta_data is not None:
            return xml_to_dict(meta_data.getchildren()[0], strip_ns=self._strip_ns)
        return None


def harvest_oai(**kwargs):
    """Create OAI ListRecords Iterator for Harvesting Data."""
    oai_endpoint = kwargs.get("oai_endpoint")
    harvest_params = kwargs.get("harvest_params")
    logging.info("Harvesting from %s", oai_endpoint)
    logging.info("Harvesting %s", harvest_params)
    sickle_client = ValidatingSickle(oai_endpoint, retry_status_codes=[500, 503, 504], max_retries=3)
    class_mapping = harvest_params.get(
        "class_mapping",
        {
            "ListRecords": HarvestRecord,
        },
    )
    iterator = harvest_params.get("iterator", HarvestIterator)
    for key in class_mapping:
        sickle_client.class_mapping[key] = class_mapping[key]

    sickle_client.iterator = iterator

    try:
        return sickle_client.ListRecords(**harvest_params)
    except NoRecordsMatch:
        logging.info("No records found.")
        return []


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
        return etree.tostring(self.root, encoding="utf-8").decode("utf-8")


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
    filename = f"{prefix}/{our_hash}"
    process.generate_s3_object(string, bucket_name, filename, access_id, access_secret)


def write_log(string, prefix, **_kwargs):
    """Write the data to logging info."""
    logging.info(prefix)
    logging.info(string)


def dag_s3_prefix(dag_id, timestamp):
    """Define the prefix that will be prepended to all files created by this dag run"""
    return f"{dag_id}/{timestamp}"
