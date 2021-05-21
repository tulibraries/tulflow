"""
tulflow.transform
~~~~~~~~~~~~~~~
This module contains objects to transform data using a known transform language.
"""
import hashlib
import logging
import os
import subprocess
import sys
import requests
from lxml import etree
from tulflow import process
from pathlib import Path

# pylint: disable=unexpected-keyword-arg
def transform_s3_xsl(**kwargs):
    """Transform & Write XML data to S3 using Saxon XSLT Engine."""
    access_id = kwargs.get("access_id")
    access_secret = kwargs.get("access_secret")
    bucket = kwargs.get("bucket")
    dest_prefix = kwargs.get("destination_prefix")
    source_prefix = kwargs.get("source_prefix")
    if kwargs.get("dag"):
        run_id = kwargs.get("dag").dag_id
    else:
        run_id = "no-dag-provided"

    saxon = prepare_saxon_engine()
    transformed = etree.Element("collection")
    transformed.attrib["dag-id"] = run_id
    transformed.attrib["dag-timestamp"] = kwargs.get("timestamp", "no-timestamp-provided")
    xsl = "https://raw.github.com/{repo}/{branch}/{filename}".format(
        repo=kwargs.get("xsl_repository", "tulibraries/aggregator_mdx"),
        branch=kwargs.get("xsl_branch", "main"),
        filename=kwargs.get("xsl_filename")
    )

    for s3_key in process.list_s3_content(bucket, access_id, access_secret, source_prefix):
        logging.info("Transforming File %s", s3_key)
        s3_content = process.get_s3_content(bucket, s3_key, access_id, access_secret)
        s3_xml = etree.fromstring(s3_content)
        for record in s3_xml.iterchildren():
            record_id = record.get("airflow-record-id")
            logging.info("Transforming Record %s", record_id)
            result_str = subprocess.check_output(["java", "-jar", saxon, "-xsl:" + xsl, "-s:-"], input=etree.tostring(record, encoding="utf-8"))
            result = etree.fromstring(result_str)
            result.attrib["airflow-record-id"] = record_id
            transformed.append(result)
        filename = s3_key.replace(source_prefix, dest_prefix)
        transformed_xml = etree.tostring(transformed, encoding="utf-8")
        process.generate_s3_object(transformed_xml, bucket, filename, access_id, access_secret)


def prepare_saxon_engine(saxon_jar="saxon.jar", saxon_path="/tmp/saxon/"):
    """Set up Saxon HE 9 Java Engine for XML & XSL tasks."""
    saxon_version = "9.9.1-5"
    saxon_download_sha1 = "c1f413a1b810dbf0d673ffd3b27c8829a82ac31c"

    if not os.path.exists(saxon_path + saxon_jar):
        Path(saxon_path).mkdir(parents=True, exist_ok=True)
        request_url = "https://repo1.maven.org/maven2/net/sf/saxon/Saxon-HE/"
        request_url += saxon_version + "/Saxon-HE-" + saxon_version + ".jar"
        resp = requests.get(request_url, allow_redirects=True, verify=False)
        if hashlib.sha1(resp.content).hexdigest() == saxon_download_sha1:
            open(saxon_path + saxon_jar, "wb").write(resp.content)
            os.chmod(saxon_path + saxon_jar, 0o744)
            return saxon_path + saxon_jar
        logging.fatal("SHA1 Digests do not match.")
        sys.exit()
    return saxon_path + saxon_jar
