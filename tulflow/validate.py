"""Generic Data (primarily XML & JSON) Validation Methods."""
import logging
import csv
import io
from airflow.exceptions import AirflowFailException
from lxml import etree, isoschematron
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
    total_filter_count = 0
    total_record_count = 0
    for s3_key in process.list_s3_content(bucket, access_id, access_secret, source_prefix):
        logging.info("Validating & Filtering File: %s", s3_key)
        s3_content = process.get_s3_content(bucket, s3_key, access_id, access_secret)
        s3_xml = etree.fromstring(s3_content)
        invalid_xml = etree.Element("collection")
        invalid_xml.attrib["dag-id"] = run_id
        invalid_xml.attrib["dag-timestamp"] = timestamp
        filter_count = 0
        record_count = 0
        for record in s3_xml.iterchildren():
            record_count += 1
            total_record_count += 1
            if not schematron.validate(record):
                record_id = record.get("airflow-record-id")
                logging.error("Invalid record found: %s", record_id)
                s3_xml.remove(record)
                filter_count += 1
                invalid_csv.writerow({
                    "id": record_id,
                    "report": schematron_failed_validation_text(schematron.validation_report),
                    "record": identifier_or_full_record(record),
                    "source_file": f"https://s3.console.aws.amazon.com/s3/object/{bucket}/{s3_key}"
                })
        total_filter_count += filter_count
        filename = s3_key.replace(source_prefix, dest_prefix)
        updated_s3_xml = etree.tostring(s3_xml, encoding="utf-8").decode("utf-8")
        process.generate_s3_object(updated_s3_xml, bucket, filename, access_id, access_secret)
        if filter_count == record_count and record_count != 0:
            logging.warning(f"All records filtered from {filename}. record_count: {record_count}")

    invalid_filename = report_prefix + "-invalid.csv"
    logging.info("Total Filter Count: %s", total_filter_count)
    logging.info("Invalid Records report: https://%s.s3.amazonaws.com/%s", bucket, invalid_filename)
    process.generate_s3_object(csv_in_mem.getvalue(), bucket, invalid_filename, access_id, access_secret)
    if total_filter_count == total_record_count and total_record_count != 0:
        raise AirflowFailException(f"All records were filtered out: {total_record_count}") # pylint: disable
    return {"filtered": total_filter_count}

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
    total_transform_count = 0
    for s3_key in process.list_s3_content(bucket, access_id, access_secret, source_prefix):
        logging.info("Validating & Reporting On File: %s", s3_key)
        s3_content = process.get_s3_content(bucket, s3_key, access_id, access_secret)
        s3_xml = etree.fromstring(s3_content)
        for record in s3_xml.iterchildren():
            total_transform_count += 1
            record_id = record.get("airflow-record-id")
            logging.info("Ran report on record: %s", record_id)
            schematron.validate(record)
            report_csv.writerow({
                "id": record_id,
                "report": schematron_failed_validation_text(schematron.validation_report),
                "record": identifier_or_full_record(record),
                "source_file": f"https://s3.console.aws.amazon.com/s3/object/{bucket}/{s3_key}"
            })
    report_filename = dest_prefix + "-report.csv"
    logging.info("Records report: https://%s.s3.amazonaws.com/%s", bucket, report_filename)
    logging.info("Total Transform Count: %s", total_transform_count)
    process.generate_s3_object(csv_in_mem.getvalue(), bucket, report_filename, access_id, access_secret)

    return {"transformed": total_transform_count}

def identifier_or_full_record(record, identifier_xpath="./dcterms:identifier/text()", identifier_namespaces={"dcterms": "http://purl.org/dc/terms/"}):
    indentifiers = record.xpath(identifier_xpath, namespaces=identifier_namespaces)
    if indentifiers:
        return "\n".join(indentifiers)
    else:
        return etree.tostring(record, encoding="utf-8").decode("utf-8")


def schematron_failed_validation_text(validation_report):
    return "\n".join(
        validation_report.xpath("./svrl:failed-assert/svrl:text/text()",namespaces={"svrl": "http://purl.oclc.org/dsdl/svrl"})
                    )
