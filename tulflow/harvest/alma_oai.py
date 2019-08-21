"""Python method instances to harvest OAI-PMH XML from TUL Alma OAI-PMH to S3 Data Bucket."""
import logging
from datetime import datetime, timedelta
import os.path
import xml.etree.ElementTree
import xml.dom.minidom
from airflow.models import Variable
from airflow import AirflowException
from cob_datapipeline.indexing import oai_harvest

NEW_FIRST_LINE = '<?xml version="1.0" encoding="UTF-8"?>'
NEW_ROOT = """
<collection xmlns="http://www.loc.gov/MARC21/slim"
            xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
            xsi:schemaLocation="http://www.loc.gov/MARC21/slim http://www.loc.gov/standards/marcxml/schema/MARC21slim.xsd">
"""
NEW_ROOT_CLOSING_TAG = '</collection>'


def boundwithparents_oai_harvest(url, outfilename, **kwargs):
    """OAI Harvest Function Wrapper to Harvest Boundwith Parents MARC Records from Alma OAI."""
    outfile = oai_harvest.check_or_create_dataout(outfilename)

    outfile.write(NEW_FIRST_LINE)
    outfile.write(NEW_ROOT)

    prefix = 'marc21'
    kwargs = {'set': 'alma_bibs_boundwith_parents'}
    process_kwargs = {'outfile': outfile}
    oai_harvest.oai_harvest(url, prefix, boundwith_process_record, process_kwargs, **kwargs)

    outfile.write(NEW_ROOT_CLOSING_TAG)
    outfile.close()


def boundwithchildren_oai_harvest(url, outfilename, **kwargs):
    """OAI Harvest Function Wrapper to Harvest Boundwith Children MARC Records from Alma OAI."""
    outfile = oai_harvest.check_or_create_dataout(outfilename)

    outfile.write(NEW_FIRST_LINE)
    outfile.write(NEW_ROOT)

    prefix = 'marc21'
    kwargs = {'set': 'alma_bibs_boundwith_children'}
    process_kwargs = {'outfile': outfile}
    oai_harvest.oai_harvest(url, prefix, boundwith_process_record, process_kwargs, **kwargs)

    outfile.write(NEW_ROOT_CLOSING_TAG)
    outfile.close()


def boundwith_process_record(record, process_args):
    """In Alma OAI Harvest, find children records & embed at top level of Collection."""
    outfile = process_args['outfile']
    tree = xml.etree.ElementTree.fromstring(record.raw)
    header = tree[0]
    subrecord = None
    if len(list(tree)) > 1 and list(tree[1]):
        subrecord = tree[1][0]
    if subrecord is not None:
        subrecord.insert(0, header)
        outfile.write('{}\n'.format(xml.etree.ElementTree.tostring(subrecord, encoding='unicode')))
    return process_args


def almaoai_harvest(url, outfilename, deletedfilename, publish_interval, last_harvest, **kwargs):
    """OAI Harvest Function Wrapper to Harvest all MARC Records from Alma OAI."""
    try:
        num_deleted_recs = num_updated_recs = 0

        date_now = datetime.now()
        date_last_harvest = datetime.strptime(last_harvest, '%Y-%m-%dT%H:%M:%SZ')
        harvest_from_delta = date_last_harvest - timedelta(hours=int(publish_interval))
        harvest_from_date = harvest_from_delta.strftime('%Y-%m-%dT%H:%M:%SZ')
        harvest_until_date = date_now.strftime('%Y-%m-%dT%H:%M:%SZ')

        outfile = oai_harvest.check_or_create_dataout(outfilename)
        deletedfile = oai_harvest.check_or_create_dataout(deletedfilename)

        outfile.write(NEW_FIRST_LINE)
        outfile.write(NEW_ROOT)
        deletedfile.write(NEW_FIRST_LINE)
        deletedfile.write(NEW_ROOT)

        prefix = 'marc21'
        kwargs = {
            'set': 'blacklight',
            'harvest_from_date': str(harvest_from_date),
            'harvest_until_date': str(harvest_until_date)
        }
        process_kwargs = {
            'outfile': outfile,
            'deletedfile': deletedfile,
            'num_updated_recs': 0,
            'num_deleted_recs': 0
        }
        oai_harvest.oai_harvest(url, prefix, tulcob_process_records, process_kwargs, **kwargs)

        outfile.write(NEW_ROOT_CLOSING_TAG)
        outfile.close()
        deletedfile.write(NEW_ROOT_CLOSING_TAG)
        deletedfile.close()
        logging.info("num_updated_recs %d", num_updated_recs)
        Variable.set("almaoai_last_num_oai_update_recs", num_updated_recs)
        logging.info("num_deleted_recs %d", num_deleted_recs)
        Variable.set("almaoai_last_num_oai_delete_recs", num_deleted_recs)
        # If we got nothing, it might be because the OAI publish interval
        # changed on us. Don't update harvest date because we should come back
        # to this same time again in hopes the OAI endpoint got new data for
        # this time interval
        if num_updated_recs == 0:
            logging.info("Got no OAI records, we'll revisit this date next harvest.")
        else:
            Variable.set("ALMAOAI_LAST_HARVEST_DATE", harvest_until_date)
            Variable.set("ALMAOAI_LAST_HARVEST_FROM_DATE", harvest_from_date)
    except Exception as ex:
        # If we died in the middle of a harvest, don't keep a partial download
        outfile = oai_harvest.check_or_create_dataout(outfilename)
        deletedfile = oai_harvest.check_or_create_dataout(deletedfilename)
        if outfile is not None:
            if outfile.closed is not True:
                outfile.close()
            os.remove(outfilename)
        if deletedfile is not None:
            if deletedfile.closed is not True:
                deletedfile.close()
            os.remove(deletedfilename)

        logging.error(str(ex))
        raise AirflowException('Harvest failed.')


def tulcob_process_records(record, process_args):
    """In Alma OAI Harvest, find child records & embed at Collection top level; separate deleted."""
    outfile = process_args['outfile']
    deletedfile = process_args['deletedfile']
    num_updated_recs = process_args['num_updated_recs']
    num_deleted_recs = process_args['num_deleted_recs']

    tree = xml.etree.ElementTree.fromstring(record.raw)
    header = tree[0]
    subrecord = None
    if len(list(tree)) > 1 and list(tree[1]):
        subrecord = tree[1][0]
    if subrecord is not None:
        subrecord.insert(0, header)
        outfile.write('{}\n'.format(xml.etree.ElementTree.tostring(subrecord, encoding='unicode')))
        num_updated_recs += 1
    elif header.get('status') == 'deleted':
        record_text = xml.etree.ElementTree.tostring(header, encoding='unicode')
        deletedfile.write('<record>{}</record>\n'.format(record_text))
        num_deleted_recs += 1
    else:
        print('subrecord issue?')
        print(record.raw)
    process_args['num_updated_recs'] = num_updated_recs
    process_args['num_deleted_recs'] = num_deleted_recs
    return process_args
