"""Generic Data (primarily XML & JSON) Validation Methods."""
import csv
import io
from lxml import etree, isoschematron
import logging
import re
from tulflow import process


def filter_s3_schematron(**kwargs):
    """Wrapper function for using S3 Retrieval, Schematron Filtering, and S3 Writer."""
    source_prefix = kwargs.get("source_prefix")
    dest_prefix = kwargs.get("destination_prefix")
    report_prefix = kwargs.get("report_prefix")
    bucket = kwargs.get("bucket")
    schematron_file = kwargs.get("schematron_filename")
    access_id = kwargs.get("access_id")
    access_secret = kwargs.get("access_secret")
    if kwargs.get("dag"):
        run_id = kwargs.get("dag").dag_id
    else:
        run_id = "no-dag-provided"
    if kwargs.get("timestamp"):
        timestamp = kwargs.get("timestamp")
    else:
        timestamp = "no-timestamp-provided"

    # create invalid records reporting csv
    csv_in_mem = io.StringIO()
    invalid_csv = csv.DictWriter(csv_in_mem, fieldnames=["id", "report", "record", "source_file"])
    invalid_csv.writeheader()

    # get schematron doc & return lxml.etree.Schematron validator
    schematron_doc = process.get_github_content("tulibraries/aggregator_mdx", schematron_file)
    schematron = isoschematron.Schematron(etree.fromstring(schematron_doc), store_report=True)

    for s3_key in process.list_s3_content(bucket, access_id, access_secret, source_prefix):
        logging.info("Validating & Filtering File: %s", s3_key)
        s3_content = process.get_s3_content(bucket, s3_key, access_id, access_secret)
        s3_xml = etree.fromstring(s3_content)
        invalid_xml = etree.Element("collection")
        invalid_xml.attrib["dag-id"] = run_id
        invalid_xml.attrib["dag-timestamp"] = timestamp
        for record in s3_xml.iterchildren():
            if not schematron.validate(record):
                record_id = record.get("airflow-record-id")
                logging.error("Invalid record found: %s", record_id)
                s3_xml.remove(record)
                invalid_csv.writerow({
                    "id": record_id,
                    "report": etree.tostring(schematron.validation_report),
                    "record": etree.tostring(record),
                    "source_file": s3_key
                })
        filename = s3_key.replace(source_prefix, dest_prefix)
        updated_s3_xml = etree.tostring(s3_xml)
        process.generate_s3_object(updated_s3_xml, bucket, filename, access_id, access_secret)
    invalid_filename = report_prefix + "-invalid.csv"
    logging.info("Invalid Records report: https://%s.s3.amazonaws.com/%s", bucket, invalid_filename)
    process.generate_s3_object(csv_in_mem.getvalue(), bucket, invalid_filename, access_id, access_secret)


def report_s3_schematron(**kwargs):
    """Wrapper function for using S3 Retrieval, Schematron Reporting, and S3 Writer."""
    source_prefix = kwargs.get("source_prefix")
    dest_prefix = kwargs.get("destination_prefix")
    bucket = kwargs.get("bucket")
    schematron_file = kwargs.get("schematron_filename")
    access_id = kwargs.get("access_id")
    access_secret = kwargs.get("access_secret")

    # create reporting csv
    csv_in_mem = io.StringIO()
    report_csv = csv.DictWriter(csv_in_mem, fieldnames=["id", "report", "record", "source_file"])
    report_csv.writeheader()

    # get schematron doc & return lxml.etree.Schematron validator
    schematron_doc = process.get_github_content("tulibraries/aggregator_mdx", schematron_file)
    schematron = isoschematron.Schematron(etree.fromstring(schematron_doc), store_report=True)

    # Iterate through S3 Files, Validate, & Save Report to CSV
    for s3_key in process.list_s3_content(bucket, access_id, access_secret, source_prefix):
        logging.info("Validating & Reporting On File: %s", s3_key)
        s3_content = process.get_s3_content(bucket, s3_key, access_id, access_secret)
        s3_xml = etree.fromstring(s3_content)
        for record in s3_xml.iterchildren():
            record_id = record.get("airflow-record-id")
            logging.info("Ran report on record: %s", record_id)
            schematron.validate(record)
            report_csv.writerow({
                "id": record_id,
                "report": etree.tostring(schematron.validation_report),
                "record": etree.tostring(record),
                "source_file": s3_key
            })
    report_filename = dest_prefix + "-report.csv"
    logging.info("Records report: https://%s.s3.amazonaws.com/%s", bucket, report_filename)
    process.generate_s3_object(csv_in_mem.getvalue(), bucket, report_filename, access_id, access_secret)
