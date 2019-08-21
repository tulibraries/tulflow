"""Python methods to harvest OAI-PMH XML data from a given URL to a given data location."""
import logging
from sickle import Sickle
from sickle.oaiexceptions import NoRecordsMatch
from airflow import AirflowException


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


def check_or_create_dataout(outfilename):
    """Function to see if harvest file exists already; and if not, create."""
    if os.path.isfile(outfilename):
        logging.info('Not re-harvesting until index_marc completes and moves old %s.', outfilename)
        return
    return open(outfilename, 'w')
