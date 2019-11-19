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
    bucket = kwargs.get("bucket")
    schematron_file = kwargs.get("schematron_filename")
    access_id = kwargs.get("access_id")
    access_secret = kwargs.get("access_secret")

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
        invalid_xml.attrib["airflow-run-dag"] = kwargs.get("dag").dag_id
        invalid_xml.attrib["airflow-run-timestamp"] = kwargs.get("timestamp")
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
    invalid_filename = dest_prefix + "-invalid.csv"
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

    # create invalid records reporting csv
    csv_in_mem = io.StringIO()
    report_csv = csv.DictWriter(csv_in_mem, fieldnames=["id", "report", "record", "source_file"])
    report_csv.writeheader()

    # get schematron doc & return lxml.etree.Schematron validator
    schematron_doc = process.get_github_content("tulibraries/aggregator_mdx", schematron_file)
    schematron = isoschematron.Schematron(etree.fromstring(schematron_doc), store_report=True)
    for s3_key in process.list_s3_content(bucket, access_id, access_secret, source_prefix):
        logging.info("Validating & Filtering File: %s", s3_key)
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


def report_xml_analysis(**kwargs):
    """Wrapper function for using S3 Retrieval, Python Analysis Reporting, and S3 Writer."""
    source_prefix = kwargs.get("source_prefix")
    bucket = kwargs.get("bucket")
    access_id = kwargs.get("access_id")
    access_secret = kwargs.get("access_secret")

    count = 0
    stats_aggregate = {"record_count": 0, "field_info": {}}
    for s3_key in process.list_s3_content(bucket, access_id, access_secret, source_prefix):
        logging.info("Reading key: %s", s3_key)
        s3_content = process.get_s3_content(bucket, s3_key, access_id, access_secret)
        s3_xml = etree.fromstring(s3_content)
        for record in s3_xml.iterchildren():
            if (count % 1000) == 0 and count != 0:
                logging.info("%d records processed", count)
            count += 1
            collect_stats(stats_aggregate, get_stats(record))
    stats_averages = create_stats_averages(stats_aggregate)
    pretty_print_stats(stats_averages)


def get_stats(record):
    """Generate stats per field in lxml.etree.Element record given."""
    stats = {}
    logging.info("Getting stats for record: %s", record.get("airflow-record-id"))
    for field in record.iter():
        if len(field) is not False and field.text is not None:
            tree = record.getroottree()
            stats.setdefault(re.sub(r"\[\d+\]", "", tree.getelementpath(field)), 0)
            stats[re.sub(r"\[\d+\]", "", tree.getelementpath(field))] += 1
    return stats


def collect_stats(stats_aggregate, stats):
    """Create totals for field stats given."""
    stats_aggregate["record_count"] += 1
    for field in stats:
        # get the total number of times a field occurs
        stats_aggregate["field_info"].setdefault(field, {"field_count": 0})
        stats_aggregate["field_info"][field]["field_count"] += 1

        # get average of all fields
        stats_aggregate["field_info"][field].setdefault("field_count_total", 0)
        stats_aggregate["field_info"][field]["field_count_total"] += stats[field]


def create_stats_averages(stats_aggregate):
    """Create averages from the totals generated for field stats given."""
    for field in stats_aggregate["field_info"]:
        field_count = stats_aggregate["field_info"][field]["field_count"]
        field_count_total = stats_aggregate["field_info"][field]["field_count_total"]

        field_count_total_avg = (float(field_count_total) / float(stats_aggregate["record_count"]))
        stats_aggregate["field_info"][field]["field_count_total_average"] = field_count_total_avg

        field_count_elem_avg = (float(field_count_total) / float(field_count))
        stats_aggregate["field_info"][field]["field_count_element_average"] = field_count_elem_avg
    return stats_aggregate


def pretty_print_stats(stats_averages):
    """Print out field stats report."""
    record_count = stats_averages["record_count"]
    element_length = 0
    for element in stats_averages["field_info"]:
        if element_length < len(element):
            element_length = len(element)

    logging.info("\n\n")
    for element in sorted(stats_averages["field_info"]):
        percent = (stats_averages["field_info"][element]["field_count"] / float(record_count)) * 100
        column_one = " " * (element_length - len(element)) + element
        logging.info("%s: %6s/%s | %3d%% ", column_one, stats_averages["field_info"][element]["field_count"], record_count, percent)
    logging.info("\n")
