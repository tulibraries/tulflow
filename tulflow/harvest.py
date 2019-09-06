"""
tulflow.harvest
~~~~~~~~~~~~~~~
This module contains objects to harvest data from one given location to another.
"""
import hashlib
import logging
from lxml import etree
from sickle import Sickle
from airflow.hooks.S3_hook import S3Hook


def oai_to_s3(**kwargs):
    """Wrapper function for using OAI Harvest, Default Processor, and S3 Writer."""
    kwargs['harvest_params'] = {
        'metadataPrefix': kwargs.get('metadata_prefix'),
        'set': kwargs.get('set'),
        'from': kwargs.get('harvest_from_date'),
        'until': kwargs.get('harvest_until_date')
    }
    dag_id = kwargs.get('dag').dag_id
    dag_start_date = kwargs.get('timestamp')

    data = harvest_oai(**kwargs)
    kwargs['prefix'] = dag_s3_prefix(dag_id, dag_start_date)
    count = process_xml(data, dag_write_string_to_s3, **kwargs)
    logging.info("OAI Records Harvested & Processed: %s", count)


def harvest_oai(**kwargs):
    """Create OAI ListRecords Iterator for Harvesting Data."""
    oai_endpoint = kwargs.get('oai_endpoint')
    harvest_params = kwargs.get('harvest_params')
    logging.info("Harvesting from %s", oai_endpoint)
    logging.info("Harvesting %s", harvest_params)
    request = Sickle(oai_endpoint)
    data = request.ListRecords(harvest_params)
    return data


def process_xml(data, writer, **kwargs):
    """Process & Write XML data to S3."""
    parser = kwargs.get('parser')
    records_per_file = kwargs.get('records_per_file')
    if not records_per_file:
        records_per_file = 1000
    count = 0
    collection = etree.Element("collection")

    for record in data:
        count += 1
        record = record.xml
        if parser:
            record = parser(record)
        collection.append(record)
        if count % int(records_per_file) == 0:
            kwargs['string'] = etree.tostring(collection).decode('utf-8')
            writer(**kwargs)
            collection = etree.Element("collection")
    kwargs['string'] = etree.tostring(collection).decode('utf-8')
    writer(**kwargs)
    return count


def dag_write_string_to_s3(**kwargs):
    """Push a string in memory to s3 with a defined prefix"""
    string = kwargs.get('string')
    prefix = kwargs.get('prefix')
    s3_conn = kwargs.get('s3_conn')
    bucket_name = kwargs.get('bucket_name')
    logging.info("Writing to S3 Bucket %s", bucket_name)

    hook = S3Hook(s3_conn.conn_id)
    our_hash = hashlib.md5(string.encode('utf-8')).hexdigest()
    filename = "{}/{}".format(prefix, our_hash)
    hook.load_string(string, filename, bucket_name=bucket_name)


def write_log(**kwargs):
    """Write the data to logging info."""
    string = kwargs.get('string')
    logging.info(string)


def dag_s3_prefix(dag_id, timestamp):
    """Define the prefix that will be prepended to all files created by this dag run"""
    return "{}/{}".format(dag_id, timestamp)
