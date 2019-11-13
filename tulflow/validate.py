"""Generic Data (primarily XML & JSON) Validation Methods."""
from lxml import etree, isoschematron
import logging
from tulflow import process


def filter_s3_schematron(**kwargs):
    """Wrapper function for using S3 Retrieval, Schematron Filtering, and S3 Writer."""
    source_prefix = kwargs.get("source_prefix")
    dest_prefix = kwargs.get("destination_prefix")
    bucket = kwargs.get("bucket")
    record_element = kwargs.get("record_parent_element")
    schematron_file = kwargs.get("schematron_filename")
    access_id = kwargs.get("access_id")
    access_secret = kwargs.get("access_secret")

    # get schematron doc & return lxml.etree.Schematron validator
    schematron_doc = process.get_github_content("tulibraries/aggregator_mdx", schematron_file)
    schematron = isoschematron.Schematron(etree.fromstring(schematron_doc), store_report=True)

    for s3_key in process.list_s3_content(bucket, access_id, access_secret, source_prefix):
        logging.info("Validating & Filtering File: %s", s3_key)
        s3_content = process.get_s3_content(bucket, s3_key, access_id, access_secret)
        s3_xml = etree.fromstring(s3_content)
        record_count = 0
        for record in s3_xml.findall(record_element):
            record_count += 1
            if not schematron.validate(record):
                logging.error("Invalid record found: %s", etree.tostring(record))
                logging.error("Schematron Report: %s", etree.tostring(schematron.validation_report))
                error_dest_prefix = dest_prefix + "-invalid"
                error_filename = s3_key.replace(source_prefix, error_dest_prefix) + str(record_count)
                process.generate_s3_object(
                    etree.tostring(record),
                    bucket,
                    error_filename,
                    access_id,
                    access_secret
                )
                process.generate_s3_object(
                    etree.tostring(schematron.validation_report),
                    bucket,
                    error_filename + "-report",
                    access_id,
                    access_secret
                )
                s3_xml.remove(record)
        filename = s3_key.replace(source_prefix, dest_prefix)
        updated_s3_xml = etree.tostring(s3_xml)
        process.generate_s3_object(updated_s3_xml, bucket, filename, access_id, access_secret)
