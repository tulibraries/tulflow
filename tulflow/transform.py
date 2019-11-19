"""
tulflow.transform
~~~~~~~~~~~~~~~
This module contains objects to transform data using a known transform language.
"""
import io
import logging
from lxml import etree
from tulflow import process


def transform_s3_xsl(**kwargs):
    """Transform & Write XML data to S3."""
    source_prefix = kwargs.get("source_prefix")
    dest_prefix = kwargs.get("destination_prefix")
    bucket = kwargs.get("bucket")
    access_id = kwargs.get("access_id")
    access_secret = kwargs.get("access_secret")
    xslt_filename = kwargs.get("xslt_filename")

    transformed = etree.Element("collection")
    transformed.attrib["dag-run"] = kwargs.get("dag").dag_id
    transformed.attrib["dag-timestamp"] = kwargs.get("timestamp")
    xslt_doc = process.get_github_content("tulibraries/aggregator_mdx", xslt_filename)
    xslt = etree.XSLT(etree.fromstring(xslt_doc))

    for s3_key in process.list_s3_content(bucket, access_id, access_secret, source_prefix):
        logging.info("Transforming File %s", s3_key)
        s3_content = process.get_s3_content(bucket, s3_key, access_id, access_secret)
        s3_xml = etree.fromstring(s3_content)
        for record in s3_xml.iterchildren():
            record_id = record.get("airflow-record-id")
            result = xslt(record)
            result.attrib["airflow-record-id"] = record_id
            transformed.append(record)
        filename = s3_key.replace(source_prefix, dest_prefix)
        transformed_xml = etree.tostring(transformed)
        process.generate_s3_object(transformed_xml, bucket, filename, access_id, access_secret)
