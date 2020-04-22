import os, sys
import mmap
import platform
import time
import re

from azure.storage import CloudStorageAccount
from azure_blob import AzureBlobContainerService
from azure_table import AzureTableService

import logging
# create logger
logger = logging.getLogger('azurestorage')
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

# To clean the data and upload to the storage server
# 
# Step1: List all files in the folder (input, output)
# Step2: Prepare metadata to upload to Table
# Step3: Upload files to blob container
#        Upload metadata files to table
# Step5: Clean, remove data to save spaces in the decoding server

# Global environments, variables to use later in the functions
AZURE_STORAGE_ACCOUNT_KEY=""
AZURE_STORAGE_ACCOUNT_NAME=""
AZURE_STORAGE_CONNECTIONSTRING="DefaultEndpointsProtocol=https;AccountName=" + AZURE_STORAGE_ACCOUNT_NAME + ";AccountKey=" + AZURE_STORAGE_ACCOUNT_KEY + ";EndpointSuffix=core.windows.net"

# Get the date from the file 
def creation_date_str(path_to_file):
    """
    Try to get the date that a file was created, falling back to when it was
    last modified if that isn't possible.
    See http://stackoverflow.com/a/39501288/1709587 for explanation.
    """
    creation_datetime = ""
    if platform.system() == "Windows":
        return time.ctime(os.path.getctime(path_to_file))
    else:
        stat = os.stat(path_to_file)
        try:
            creation_datetime = time.strftime("%Y-%m-%d", time.localtime(stat.st_birthtime))
        except AttributeError:
            # We're probably on Linux. No easy way to get creation dates here,
            # so we'll settle for when its content was last modified.
            creation_datetime =  time.strftime("%Y-%m-%d", time.localtime(stat.st_mtime))

    return creation_datetime

# Listing all local files which match fileID
def list_match_files(fileId, inDir):
    match_files = [os.path.join(inDir,f) for f in os.listdir(inDir) if \
                      ( os.path.isfile(os.path.join(inDir, f)) and \
                       (fileId in os.path.join(inDir, f)))]
    return match_files

# Listing all the local files in inDir 
def list_all_files(inDir):
    match_files = [os.path.join(inDir,f) for f in os.listdir(inDir) if \
                      ( os.path.isfile(os.path.join(inDir, f)))]
    return match_files


import time
from datetime import datetime
def return_timeinseconds(inputStr):
    (t,r) = inputStr.split(",")
    d = datetime.strptime(t, "%Y-%m-%d %H:%M:%S")
    retVal = float(time.mktime(d.timetuple())) + round(float(int(r)*(0.001)),3)
    return retVal


def different_times(time1, time2):
    timeInMilliSeconds_1 = return_timeinseconds(time1)
    timeInMilliSeconds_2 = return_timeinseconds(time2)
    diff = timeInMilliSeconds_2 - timeInMilliSeconds_1
    return diff

'''
    INFO 2020-03-19 00:00:09,827 eea6707b-c108-42ad-8ddc-95be161d853f: OPEN: user='none', content='none'
    INFO 2020-03-19 00:00:09,829 eea6707b-c108-42ad-8ddc-95be161d853f: Using worker <__main__.HttpChunkedRecognizeHandler object at 0x7fb26e4c59b0>
    INFO 2020-03-19 00:00:09,829 eea6707b-c108-42ad-8ddc-95be161d853f: Using content type: audio/x-wav
   DEBUG 2020-03-19 00:00:09,829 eea6707b-c108-42ad-8ddc-95be161d853f: Forwarding client message of length 65536 to worker
    INFO 2020-03-19 00:00:11,049 eea6707b-c108-42ad-8ddc-95be161d853f: Receiving event {'status': 0, 'adaptation_state': {'id': 'eea6707b-c108-42ad-8ddc-95be161d853f', 'value': 'eJxlfV... from worker
    INFO 2020-03-19 00:00:11,050 eea6707b-c108-42ad-8ddc-95be161d853f: Receiving 'close' from worker
    INFO 2020-03-19 00:00:11,050 eea6707b-c108-42ad-8ddc-95be161d853f: Final hyp: what is ai singapore.
'''
def get_decoding_time(fileId, searchDir):
    decoding_time = 0.0
    datafile = []

    log_files = list_match_files("master_server", searchDir)
    for logfile in log_files:
      with open(logfile) as f:
        datafile.extend(f.readlines())

    decoding_start_time = ""
    decoding_end_time = ""

    for line in datafile:
        if (fileId in line) and (': OPEN' in line):
            # Split the Final hyp and return
            #print (line.split())
            decoding_start_time = (" ".join(line.split()[1:3])).strip()
        if (fileId in line) and (': Final hyp' in line):
            #print (line.split())
            decoding_end_time = (" ".join(line.split()[1:3])).strip()

    if (decoding_start_time == "" or decoding_end_time == ""):
        return decoding_time

    #[hours1, minutes1, seconds1,subsecond1] = [x for x in re.split('[:,]', decoding_start_time[1])]
    #time1 = int(hours1) * 3600 + int(minutes1) * 60.0 + int(seconds1) + float(subsecond1)*0.001

    #[hours2, minutes2, seconds2,subsecond2] = [x for x in re.split('[:,]', decoding_end_time[1])]
    #time2 = int(hours2) * 3600 + int(minutes2) * 60.0 + int(seconds2) + float(subsecond2)*0.001
    
    #difftime_in_seconds = time2-time1
    decoding_time = different_times(decoding_start_time, decoding_end_time)
    return decoding_time

def get_client_ip_address(fileId, searchDir):
    '''
    INFO 2020-04-15 00:00:02,760 db80fdb6-6ad9-4231-abdf-641b5c97960c: CLIENT IP 155.69.149.126
    INFO 2020-04-15 00:00:02,786 db80fdb6-6ad9-4231-abdf-641b5c97960c: OPEN: user='none', content='none'
    INFO 2020-04-15 00:00:02,786 db80fdb6-6ad9-4231-abdf-641b5c97960c: Using worker <__main__.HttpChunkedRecognizeHandler object at 0x7f6b0e916b00>
    INFO 2020-04-15 00:00:02,786 db80fdb6-6ad9-4231-abdf-641b5c97960c: Using content type: audio/x-wav, layout=(string)interleaved, rate=(int)16000, format=(string)S16LE, channels=(int)1

    INFO 2020-04-12 02:46:16,509 e2dfc49d-3930-4f00-94cf-882d3198b0ce: OPEN
    INFO 2020-04-12 02:46:16,510 e2dfc49d-3930-4f00-94cf-882d3198b0ce: Request arguments: content-type="?token=eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpZCI6IjVkNzBjYmQ3ZjBkNmUzMDAxYzFlNjViZSIsImlhdCI6MTU4NDg4ODk5MCwiZXhwIjoxNTg3NDgwOTkwfQ.ILO02HU4pfC1HevjulLkcpEZZwvu2Y7Wp0AFnKFHs-g"

    '''
    client_ip_address = ''
    datafile = []

    log_files = list_match_files("master_server", searchDir)
    for logfile in log_files:
      with open(logfile) as f:
        datafile.extend(f.readlines())

    for line in datafile:
        if (fileId in line) and ("CLIENT IP" in line):
            # Split the client IP and return
            client_ip_address = line.split("CLIENT IP")[-1]
            break

    return client_ip_address.strip()

def get_decoding_status(fileId, searchDir):
    '''
    INFO 2020-04-15 00:00:04,009 db80fdb6-6ad9-4231-abdf-641b5c97960c: Receiving 'close' from worker
    INFO 2020-04-15 00:00:04,009 db80fdb6-6ad9-4231-abdf-641b5c97960c: Final hyp: what is ai singapore.
    INFO 2020-04-15 00:00:04,010 Everything done
    '''
    decoding_status = 'unknown'
    datafile = []

    log_files = list_match_files("master_server", searchDir)
    for logfile in log_files:
      with open(logfile) as f:
        datafile.extend(f.readlines())

    for line in datafile:
        if (fileId in line) and ("Final hyp" in line):
            # Split the Final hyp and return
            decoding_result = line.split('Final hyp:',1)[-1]
            if (decoding_result != None) and (len(decoding_result) > 2):
                decoding_status = "finished"
                break

    return decoding_status

def get_requested_arguments(fileId, searchDir):
    requested_args = ''
    datafile = []

    log_files = list_match_files("master_server", searchDir)
    for logfile in log_files:
      with open(logfile) as f:
        datafile.extend(f.readlines())

    for line in datafile:
        if (fileId in line) and ("Request arguments:" in line):
            # Split the client IP and return
            requested_args = line.split("Request arguments:")[-1]
            break

    return requested_args.strip()

def create_metadata_file_entity(full_file_path, server_ip_address, language, server_type, logDir):
    filename, extension = os.path.splitext(os.path.basename(full_file_path))
    #27-104-241-223_db61074e-d339-45ff-a9d4-07358e525b21.raw
    fileId = filename.split('_')[1]
    logger.info("Create the metadata for fileId " + fileId)

    # Get the creation date time of the input file
    creation_time_str = creation_date_str(full_file_path)
    # Get the row key (blob name)
    row_key = os.path.basename(full_file_path)
    # Get the partition key (container name)
    partition_key = "sgdecoding_" + server_type + "_" + language
    
    # Check the client ip address, from the logs
    client_ip_address = get_client_ip_address(fileId, logDir)

    # Check the decoding status from the logs
    decoding_status = get_decoding_status(fileId, logDir)

    # Check the time to decode from the logs
    time_to_decode = get_decoding_time(fileId, logDir)
    requested_args = get_requested_arguments(fileId, logDir)

    metadata_entity = {'PartitionKey': partition_key, 'RowKey': row_key,
                      'container_name': partition_key.replace('_', ''),
                      'datetime': creation_time_str,
                      'client_ip_address': client_ip_address,
                      'server_ip_address': server_ip_address,
                      'language': language,
                      'decoding_status': 'finished',
                      'time_to_decode': time_to_decode,
                      'requested_args': requested_args
                     }

    return metadata_entity
    
def upload_to_storage(dataDir, logDir, serverip, language, systemtype, azureblob, azuretable):
    all_files = list_all_files(dataDir)
    for afile in all_files:
        # (full_file_path, server_ip_address, language, server_type):
        metadata = create_metadata_file_entity(afile, serverip, language, systemtype, logDir)
        logger.info("Uploading a file to blob storage: " + afile)

        success = azureblob.upload_file(afile)
        if success == True:
            logger.info("Inserting file successfully.")
            azuretable.add_entity_to_table(metadata)

import os, shutil
def clean_folder(inFolder):
    folder = inFolder
    for filename in os.listdir(folder):
        file_path = os.path.join(folder, filename)
        try:
            if os.path.isfile(file_path) or os.path.islink(file_path):
                logger.info("Clean the file: " + file_path)
                os.unlink(file_path)
            elif os.path.isdir(file_path):
                logger.info("Clean the directory: " + file_path)
                shutil.rmtree(file_path)
        except Exception as e:
            logger.info('Failed to delete %s. Reason: %s' % (file_path, e))


if __name__ == "__main__":
    logger.info("Hello from from upload to storage server, using Azure BlobContainer and Table!")
    account = CloudStorageAccount(AZURE_STORAGE_ACCOUNT_NAME, AZURE_STORAGE_ACCOUNT_KEY)

    basename = "sgdecoding"
    language = "cs"
    systemtype = "online"
    serverip = "40.90.x.x"

    dataDir = "/export/data/raw"
    logDir = "/export/data/logs"

    azure_blob_service = AzureBlobContainerService(basename + systemtype + language)
    try:
        azure_blob_service.create_container_storage()
    except:
        logger.info("Cannt create the container storage ")

    azure_table_service = AzureTableService(account, language, systemtype)
    try:
        azure_table_service.create_table_name()
    except:
        logger.info("Error while create the table name on azure")

    upload_to_storage(dataDir, logDir, serverip, language, systemtype, azure_blob_service, azure_table_service)
    # Clean the directory
    clean_folder(dataDir)

    #blob_service.list_blobs()
    #blob_service.download_container("testdir")



