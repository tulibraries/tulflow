"""
tulflow.harvest
~~~~~~~~~~~~~~~
This module contains objects to harvest data from one given location to another.
"""
from datetime import datetime, timedelta
import logging
import sys
import os
import os.path
import urllib
import xml.etree.ElementTree
import xml.dom.minidom
import xmltodict
from airflow import AirflowException
from airflow.models import Variable
import pexpect
from sickle import Sickle
from sickle.oaiexceptions import NoRecordsMatch

ALMA_REST_ENDPOINT = 'https://api-na.hosted.exlibrisgroup.com/almaws/v1/'
ALMA_SETS_API_PATH = 'conf/sets/'
ALMA_BIBS_API_PATH = 'bibs/'
BOUNDWITH_HOST_RECORDS_SETID = '4165880080003811'
BOUNDWITH_ITEMIZED_SETID = '11201989000003811'
ALMA_SETS_MEMBERS_PATH = '/members'
SET_XML_BEGIN = '<set link="string"> <name>Boundwith Children Testing</name> \
                 <type>ITEMIZED</type>  <content>BIB_MMS</content>  <private>true</private> \
                 <status>ACTIVE</status>'
SET_XML_END = '</set>'
MEMBER_XML = '<member link=""><id>0</id><description>Description</description></member>'
NEW_FIRST_LINE = '<?xml version="1.0" encoding="UTF-8"?>'
NEW_ROOT = """
<collection xmlns="http://www.loc.gov/MARC21/slim"
            xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
            xsi:schemaLocation="http://www.loc.gov/MARC21/slim http://www.loc.gov/standards/marcxml/schema/MARC21slim.xsd">
"""
NEW_ROOT_CLOSING_TAG = '</collection>'


def oai_harvest(oai_endpoint_url, metadata_prefix, process_method, process_method_args, **kwargs):
    """ Harvest data from a given OAI endpoint
        oai_endpoint_url: http URL for the OAI endpoint (required)
        metadata_prefix: the OAI metadata prefix to harvest from OAI (required)
        process_method: a function which takes in a record string and a dictionary of args
            and returns that dictionary of args to be passed to the next record call.
            Responsible for writing the processed record to file.
        **process_method_kwargs: user defined kwargs to be passed to process_method
        **kwargs: optional dictionary of harvest options.
                  if either from or until are present, they will be used to
                  limit the OAI results returned from the OAI feed
    """
    try:
        sickle = Sickle(oai_endpoint_url)
        harvest_args = {
            'metadataPrefix': metadata_prefix
        }

        harvest_args['set'] = kwargs.get('set')
        harvest_args['from'] = kwargs.get('harvest_from_date')
        harvest_args['until'] = kwargs.get('harvest_until_date')

        logging.info("Harvesting %d", harvest_args)
        try:
            records = sickle.ListRecords(**harvest_args)
        except NoRecordsMatch as ex:
            logging.info(str(ex))
            logging.info("No records matched the OAI arguments given.")
            records = []

        for record in records:
            process_args = process_method(record, process_method_args)

    except Exception as ex:
        logging.error(str(ex))
        if record is not None:
            logging.error(record.raw)
        raise AirflowException('Harvest failed.')


def almaoai_harvest(url, outfilename, deletedfilename, publish_interval, last_harvest, **kwargs):
    """OAI Harvest Function Wrapper to Harvest all MARC Records from Alma OAI."""
    try:
        num_deleted_recs = num_updated_recs = 0

        date_now = datetime.now()
        date_last_harvest = datetime.strptime(last_harvest, '%Y-%m-%dT%H:%M:%SZ')
        harvest_from_delta = date_last_harvest - timedelta(hours=int(publish_interval))
        harvest_from_date = harvest_from_delta.strftime('%Y-%m-%dT%H:%M:%SZ')
        harvest_until_date = date_now.strftime('%Y-%m-%dT%H:%M:%SZ')

        outfile = check_or_create_dataout(outfilename)
        deletedfile = check_or_create_dataout(deletedfilename)

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
        oai_harvest(url, prefix, tulcob_process_records, process_kwargs, **kwargs)

        outfile.write(NEW_ROOT_CLOSING_TAG)
        outfile.close()
        deletedfile.write(NEW_ROOT_CLOSING_TAG)
        deletedfile.close()
        logging.info("num_updated_recs %d", num_updated_recs)
        Variable.set("almaoai_last_num_oai_update_recs", num_updated_recs)
        logging.info("num_deleted_recs %d", num_deleted_recs)
        Variable.set("almaoai_last_num_oai_delete_recs", num_deleted_recs)
        if num_updated_recs == 0:
            logging.info("Got no OAI records, we'll revisit this date next harvest.")
        else:
            Variable.set("ALMAOAI_LAST_HARVEST_DATE", harvest_until_date)
            Variable.set("ALMAOAI_LAST_HARVEST_FROM_DATE", harvest_from_date)
    except Exception as ex:
        outfile = check_or_create_dataout(outfilename)
        deletedfile = check_or_create_dataout(deletedfilename)
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


def boundwithparents_oai_harvest(url, outfilename, **kwargs):
    """OAI Harvest Function Wrapper to Harvest Boundwith Parents MARC Records from Alma OAI."""
    outfile = check_or_create_dataout(outfilename)

    outfile.write(NEW_FIRST_LINE)
    outfile.write(NEW_ROOT)

    prefix = 'marc21'
    kwargs = {'set': 'alma_bibs_boundwith_parents'}
    process_kwargs = {'outfile': outfile}
    oai_harvest(url, prefix, boundwith_process_record, process_kwargs, **kwargs)

    outfile.write(NEW_ROOT_CLOSING_TAG)
    outfile.close()


def boundwithchildren_oai_harvest(url, outfilename, **kwargs):
    """OAI Harvest Function Wrapper to Harvest Boundwith Children MARC Records from Alma OAI."""
    outfile = check_or_create_dataout(outfilename)

    outfile.write(NEW_FIRST_LINE)
    outfile.write(NEW_ROOT)

    prefix = 'marc21'
    kwargs = {'set': 'alma_bibs_boundwith_children'}
    process_kwargs = {'outfile': outfile}
    oai_harvest(url, prefix, boundwith_process_record, process_kwargs, **kwargs)

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


def delete_old_boundwith_itemized_children(apikey):
    # get boundwith children itemized set info for num records
    file = urllib.request.urlopen(ALMA_REST_ENDPOINT+ALMA_SETS_API_PATH+
                                  BOUNDWITH_ITEMIZED_SETID+'?apikey='+apikey)
    data = file.read()
    file.close()
    setdata = xmltodict.parse(data, dict_constructor=
                              lambda *args, **kwargs: defaultdict(list, *args, **kwargs))
    numrecords = int(setdata['set'][0]['number_of_members'][0]['#text'][0])
    # loop through results and save them aside to delete later
    offset = 0
    numperpage = 100
    itemized_records = []
    while offset < numrecords:
        # page through boundwith child records
        url = "{}{}{}{}?limit={}&offset={}&apikey={}".format(ALMA_REST_ENDPOINT,
                                                             ALMA_SETS_API_PATH,
                                                             BOUNDWITH_ITEMIZED_SETID,
                                                             ALMA_SETS_MEMBERS_PATH,
                                                             str(numperpage),
                                                             str(offset),
                                                             apikey)
        file = urllib.request.urlopen(url)
        data = file.read()
        file.close()
        # hack up this xml the dumb way because who cares
        membersstart = str(data).find('<members')
        membersend = str(data).find('</members>')
        membersxml = str(data)[membersstart:membersend+10]
        membersxml = xmltodict.parse(membersxml, dict_constructor=
                                     lambda *args, **kwargs: defaultdict(list, *args, **kwargs))
        # just take the xml wholesale and use it for itemized set member request
        # doing it one page at a time ensures we never hit the 1000 member limit
        if membersxml != None:
            itemized_records.append(membersxml['members'])
        offset += numperpage
    # reset count and delete them all
    offset = 0
    for membersxml in itemized_records:
        setdata['set'][0]['members'] = membersxml
        rmsetxml = xmltodict.unparse(setdata)
        # delete members from set
        # POST /almaws/v1/conf/sets/{set_id}
        requrl = "{}{}{}?op=delete_members&apikey={}".format(ALMA_REST_ENDPOINT, ALMA_SETS_API_PATH,
                                                             BOUNDWITH_ITEMIZED_SETID, apikey)
        postreq = urllib.request.Request(requrl, data=rmsetxml.encode('utf-8'),
                                         headers={'Content-Type': 'application/xml'}, method='POST')
        file = urllib.request.urlopen(postreq)
        data = file.read()
        print(data)
        file.close()
    return setdata

def get_boundwith_children(ds, **kwargs):
    apikey = kwargs['apikey']
    # start by getting all the parent records
    # get set info for num records
    file = urllib.request.urlopen(ALMA_REST_ENDPOINT+ALMA_SETS_API_PATH+
                                  BOUNDWITH_HOST_RECORDS_SETID+'?apikey='+apikey)
    data = file.read()
    file.close()
    setdata = xmltodict.parse(data, dict_constructor=lambda *args, **kwargs: defaultdict(list, *args, **kwargs))
    numrecords = int(setdata['set'][0]['number_of_members'][0]['#text'][0])
    #
    #
    # then delete old records from existing boundwith itemized set
    childrenset = delete_old_boundwith_itemized_children(apikey)
    # get the boundwith parent IDs
    offset = 0
    numperpage = 100
    # page through boundwith parent records
    while offset < numrecords:
        requrl = "{}{}{}{}?limit={}&offset={}&apikey={}".format(ALMA_REST_ENDPOINT,
                                                                ALMA_SETS_API_PATH,
                                                                BOUNDWITH_HOST_RECORDS_SETID,
                                                                ALMA_SETS_MEMBERS_PATH,
                                                                str(numperpage),
                                                                str(offset),
                                                                apikey)
        file = urllib.request.urlopen(requrl)
        data = file.read()
        file.close()
        # hack up this xml the dumb way because who cares
        membersstart = str(data).find('<members')
        membersend = str(data).find('</members>')
        membersxml = str(data)[membersstart:membersend+10]
        membersxml = xmltodict.parse(membersxml, dict_constructor=lambda *args, **kwargs: defaultdict(list, *args, **kwargs))
        # iterate over every set member (parent) to get all child ids
        for member in membersxml['members'][0]['member']:
            parentbiburl = member['@link']
            file = urllib.request.urlopen(parentbiburl+'?apikey='+apikey)
            data = file.read()
            file.close()
            childrenxml = ''
            parentbibxml = xmltodict.parse(data, dict_constructor=lambda *args, **kwargs: defaultdict(list, *args, **kwargs))
            # iterate throught the xml to find the child id(s)
            # 774w fields = boundwith children ids
            for datafield in parentbibxml['bib'][0]['record'][0]['datafield']:
                if datafield['@tag'] == '774':
                    for subfield in datafield['subfield']:
                        if subfield['@code'] == 'w':
                            childid = subfield['#text'][0]
                            newmemberxml = xmltodict.parse(MEMBER_XML, dict_constructor=lambda *args, **kwargs: defaultdict(list, *args, **kwargs))
                            newmemberxml['member'][0]['id'] = childid
                            newmemberxml['member'][0]['@link'] = ALMA_REST_ENDPOINT+ALMA_BIBS_API_PATH+str(childid)
                            childrenxml += xmltodict.unparse(newmemberxml, full_document=False)
        # add members to set POST /almaws/v1/conf/sets/{set_id}
        # can't get xmltodict to add children and unparse successfully
        # so we're doing this the dumb way too
        addsetxml = xmltodict.unparse(childrenset)
        membersstart = str(addsetxml).find('</number_of_members>')+len('</number_of_members>')
        addsetxml = str(addsetxml)[:membersstart] + '<members>' + childrenxml + '</members>' + str(addsetxml)[membersstart:]
        postreq = urllib.request.Request(ALMA_REST_ENDPOINT+ALMA_SETS_API_PATH+BOUNDWITH_ITEMIZED_SETID+'?op=add_members&apikey='+apikey, data=addsetxml.encode('utf-8'), headers={'Content-Type': 'application/xml'}, method='POST')
        err = None
        try:
            file = urllib.request.urlopen(postreq)
        except urllib.error.HTTPError as ex:
            err = ex
            print(err)
        data = file.read()
        file.close()
        offset += numperpage


# UNUSED but might be useful later?
def get_boundwith_parents(ds, **kwargs):
    apikey = kwargs['apikey']
    # get set info for num records
    file = urllib.request.urlopen(ALMA_REST_ENDPOINT+ALMA_SETS_API_PATH+BOUNDWITH_HOST_RECORDS_SETID+'?apikey='+apikey)
    data = file.read()
    file.close()
    setdata = xmltodict.parse(data, dict_constructor=lambda *args, **kwargs: defaultdict(list, *args, **kwargs))
    numrecords = int(setdata['set'][0]['number_of_members'][0]['#text'][0])


    offset = 0
    numperpage = 100
    # page through boundwith parent records
    while offset < numrecords:
        requrl = "{}{}{}{}?limit={}&offset={}&apikey={}".format(ALMA_REST_ENDPOINT,
                                                                ALMA_SETS_API_PATH,
                                                                BOUNDWITH_HOST_RECORDS_SETID,
                                                                ALMA_SETS_MEMBERS_PATH,
                                                                str(numperpage),
                                                                str(offset),
                                                                apikey)
        file = urllib.request.urlopen(requrl)
        data = file.read()
        file.close()
        print(data)
        offset += numperpage


def check_or_create_dataout(outfilename):
    """Function to see if harvest file exists already; and if not, create."""
    if os.path.isfile(outfilename):
        logging.info('Not re-harvesting until index_marc completes and moves old %s.', outfilename)
        return
    return open(outfilename, 'w')


def almasftp_fetch():
    host = Variable.get('ALMASFTP_HOST')
    port = Variable.get('ALMASFTP_PORT')
    user = Variable.get('ALMASFTP_USER')
    passwd = Variable.get('ALMASFTP_PASSWD')
    remotepath = '/incoming'
    localpath = Variable.get("AIRFLOW_DATA_DIR") + "/sftpdump"

    if not os.path.exists(localpath):
        os.makedirs(localpath)

    file_prefix = 'alma_bibs__'
    file_extension = '.xml.tar.gz'

    sftpcmd = 'sftp -P {} {}@{}'.format(port, user, host)
    print(sftpcmd)
    p = pexpect.spawn(sftpcmd, encoding='utf-8')

    try:
        p.expect('(?i)password:')
        x = p.sendline(passwd)
        x = p.expect(['Permission denied', 'sftp>'])
        if not x:
            print('Permission denied for password:')
            print(passwd)
            p.kill(0)
        else:
            p.logfile = sys.stdout
            x = p.sendline('cd ' + remotepath)
            x = p.expect('sftp>')
            x = p.sendline('lcd ' + localpath)
            x = p.expect('sftp>')
            x = p.sendline('mget ' + file_prefix + '*' + file_extension)
            x = p.expect('sftp>', timeout=720)
            x = p.isalive()
            p.close()
            retval = p.exitstatus
    except pexpect.EOF:
        print(str(p))
        print('Transfer failed: EOF.')
        raise AirflowException('Transfer failed: EOF.')
    except pexpect.TIMEOUT:
        print(str(p))
        print('Transfer failed: TIMEOUT.')
        raise AirflowException('Transfer failed: TIMEOUT.')
    except Exception as e:
        raise AirflowException('Transfer failed: {}.'.format(str(e)))
