"""Generic Data (primarily XML) Processing Functions, Abstracted for Reuse."""
import io
import logging
import tarfile
from lxml import etree
from botocore.exceptions import ClientError
import boto3

NS = {"marc21": "http://www.loc.gov/MARC21/slim"}
LOGGER = logging.getLogger('tulflow_process')

def add_marc21xml_root_ns(data_in):
    """Given an ALMASFTP XML Collection document as bytes,
    Convert it to lxml.etree.Element & inject MARC21 as default namespace."""
    source_xml = etree.fromstring(data_in)
    if (not source_xml.attrib.get("xmlns")) and ("{http://www.loc.gov/MARC21/slim}" not in source_xml.tag):
        source_xml.attrib["xmlns"] = "http://www.loc.gov/MARC21/slim"
    source_xml = etree.fromstring(etree.tostring(source_xml))
    return source_xml

def expand_alma_sftp_tarball(key, source_obj):
    """Given an AlmaSFTP S3 bytestream, expand and return XML file."""
    source_tar = tarfile.open(fileobj=io.BytesIO(source_obj), mode="r:gz")
    if len(source_tar.getmembers()) == 0:
        LOGGER.error("S3 Object is empty.")
        LOGGER.error(key)
        return None

    if len(source_tar.getmembers()) > 1:
        LOGGER.error("S3 Object has more than 1 member, which is unexpected.")
        LOGGER.error(key)
        return None

    for item in source_tar:
        return source_tar.extractfile(item).read()

def get_record_001(record):
    """Given a MARC/XML record (lxml.etree.Element), validate & return OO1 text."""
    record_ids = record.xpath("marc21:controlfield[@tag='001']", namespaces=NS)

    if record_ids == [] or record_ids[0].text is None:
        LOGGER.error("Record without an 001 MMS Identifier:")
        LOGGER.error(str(etree.tostring(record)))
        return None

    if len(record_ids) > 1:
        LOGGER.error("Record with multiple 001 MMS Identifiers:")
        LOGGER.error(str(etree.tostring(record)))
        return None

    return record_ids[0].text

def generate_bw_parent_field(parent_id):
    """Generates our Parent ID MARC/XML field inserted into the relevant Child Records."""
    new_field = etree.Element("{http://www.loc.gov/MARC21/slim}datafield")
    new_field.set("ind1", " ")
    new_field.set("ind2", " ")
    new_field.set("tag", "ADF")
    subfield = etree.SubElement(new_field, "{http://www.loc.gov/MARC21/slim}subfield")
    subfield.set("code", "a")
    subfield.text = parent_id
    return new_field

def remove_s3_object(bucket, key, access_id, access_secret):
    """Removes an S3 object."""
    s3_client = boto3.client('s3', aws_access_key_id=access_id, aws_secret_access_key=access_secret)
    try:
        s3_client.delete_object(Bucket=bucket, Key=key)
    except ClientError as error:
        LOGGER.error(error)

def get_s3_content(bucket, key, access_id, access_secret):
    """Get the contents of S3 object located at given S3 Key."""
    s3_client = boto3.client('s3', aws_access_key_id=access_id, aws_secret_access_key=access_secret)
    try:
        response = s3_client.get_object(Bucket=bucket, Key=key)
        body = response['Body'].read()
        return body
    except ClientError as error:
        LOGGER.error(error)
        return None

def generate_s3_object(body, bucket, key, access_id, access_secret):
    """Given a bytestring, write it to S3."""
    s3_client = boto3.client("s3", aws_access_key_id=access_id, aws_secret_access_key=access_secret)
    try:
        s3_client.put_object(Bucket=bucket, Key=key, Body=body)
    except ClientError as error:
        LOGGER.error(error)