# https://github.com/Azure-Samples/storage-python-getting-started/blob/master/AzureStoragePythonGettingStarted/Tables.py
import os, sys
import random
import pprint
import string

from datetime import datetime

from azure.storage import CloudStorageAccount
from azure.storage.table import TableService, Entity

import logging

# create logger
logger = logging.getLogger('azure_table')
logger.setLevel(logging.DEBUG)

# create console handler and set level to debug
ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)
logfh = logging.handlers.RotatingFileHandler('azure_storage.log', maxBytes=10485760, backupCount=10)
logfh.setLevel(logging.DEBUG)

# create formatter
formatter = logging.Formatter(u'%(levelname)8s %(asctime)s %(message)s ')
logging._defaultFormatter = logging.Formatter(u"%(message)s")

# add formatter to ch
ch.setFormatter(formatter)
logfh.setFormatter(formatter)

# add ch to logger
logger.addHandler(ch)
logger.addHandler(logfh)

# Global environments, variables to use later in the functions
AZURE_STORAGE_ACCOUNT_KEY=""
AZURE_STORAGE_ACCOUNT_NAME=""
AZURE_STORAGE_CONNECTIONSTRING="DefaultEndpointsProtocol=https;AccountName=" + AZURE_STORAGE_ACCOUNT_NAME + ";AccountKey=" + AZURE_STORAGE_ACCOUNT_KEY + ";EndpointSuffix=core.windows.net"

class AzureTableService():
    def __init__(self, cloud_account, system_type, language):
        self.table_service = cloud_account.create_table_service()
        self.table_name = "sgdecoding" + system_type + language
 
    def create_table_name(self):
        # Create a new table
        logger.info('Create a table with name - ' + self.table_name)

        try:
            self.table_service.create_table(self.table_name)
        except Exception as err:
            logger.info('Error creating table, ' + self.table_name +
                      'check if it already exists')

    def add_entity_to_table(self, entity):
        '''
        Inserts a new entity into the table. Throws if an entity with the same PartitionKey and RowKey already exists.

        When inserting an entity into a table, you must specify values for the PartitionKey and RowKey system properties. 
        Together, these properties form the primary key and must be unique within the table. Both the PartitionKey and RowKey values must be string values;
        '''
        entity_partition_key = entity.get("PartitionKey", "")
        entity_row_key = entity.get("RowKey", "")

        # Insert the entity into the table
        logger.info('Inserting a new entity into table - ' + self.table_name)
        self.table_service.insert_entity(self.table_name, entity)
        logger.info('Successfully inserted the new entity')

        # Demonstrate how to query the entity
        logger.info('Read the inserted entity.')
        entity = self.table_service.get_entity(self.table_name, entity_partition_key, entity_row_key)
        logger.info(entity['PartitionKey'])
        logger.info(entity['RowKey'])

    def update_table_entity(self, updated_entity):
        ''' '''
        # Demonstrate how to update the entity by changing the phone number
        print('Update an existing entity with new values')
        pprint.pprint(updated_entity)

        self.table_service.update_entity(self.table_name, updated_entity)

    def search_table_entity(partition_key, key):
        # Demonstrate how to query the updated entity, filter the results with a filter query and select only the value in the phone column
        logger.info('Read the updated entity with a filter query')
        entities = self.table_service.query_entities(
                  self.table_name, filter="PartitionKey eq '" + partition_key + "'", select=key)
        for entity in entities:
            logger.info(entity[key])

    def delete_table(self):
        # Demonstrate deleting the table, if you don't want to have the table deleted comment the below block of code
        logger.info('Deleting the table.')
        self.table_service.delete_table(self.table_name)
        logger.info('Successfully deleted the table')

def random_key(length):
        return ''.join(random.choice(string.ascii_lowercase) for i in range(length))

if __name__ == "__main__":
    logger.info ("Hello from upload to Azure Table!")
    account = CloudStorageAccount(AZURE_STORAGE_ACCOUNT_NAME, AZURE_STORAGE_ACCOUNT_KEY)

    partition_key = 'sgdecoding-online-cs'
    row_key = random_key(6)
    current_time = datetime.now()
    # Original name, Datetime, Client IP address, Server IP address, Language,
    #                    Decoding status (success, fail?), Time to decode.
    example_entity = {'PartitionKey': partition_key, 'RowKey': row_key, 
                      'container_name': 'sgdecoding-online',
                      'datetime': current_time,
                      'client_ip_address': '155.69.149.126',
                      'server_ip_address': '40.90.169.207',
                      'language': 'cs',
                      'decoding_status': 'finished',
                      'time_to_decode': 90.1 
                     }

    test_table_service = AzureTableService(account, 'online', 'cs')
    test_table_service.create_table_name()
    test_table_service.add_entity_to_table(example_entity)


