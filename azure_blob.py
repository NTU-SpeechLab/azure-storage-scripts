import time
import platform
from datetime import date
from slugify import slugify

import os
import uuid
import sys
from azure.storage.blob import PublicAccess
from azure.storage.blob import * #BlockBlobService

import logging
import logging.handlers

# create logger
logger = logging.getLogger('azure_blob')
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

# Return the creation time of the input file
# Input:
# Output: 
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


class AzureBlobContainerService():

    def __init__(self, container_name):
        self.container_name = container_name
        # Create the BlockBlockService that the system uses to call the Blob service for the storage account.
        self.block_blob_service = BlockBlobService(
            account_name=AZURE_STORAGE_ACCOUNT_NAME, account_key=AZURE_STORAGE_ACCOUNT_KEY)

    # Create a container to store blob on Azure blob storage if not exists, with public permission
    def create_container_storage(self):
        # Create a container called 'container_name'.
        self.block_blob_service.create_container(self.container_name)

        # Set the permission so the blobs are public.
        self.block_blob_service.set_container_acl(
            self.container_name, public_access=PublicAccess.Container)
    
    # Return the blob service 
    def get_blob_service(self):
        return self.block_blob_service 

    # Checking if you have permission to access (read to download / write to upload) Azure blob storage
    def ask_permission(self):
        return True

    # Checking if the file already uploaded to blob storage
    def check_file_in_azstorage(self, blob_file_name):
        blob_name = os.path.basename(blob_file_name)
        logger.info("Checking the existence of the blob name in azure: " + blob_name)
        if (blob_name in self.list_blobs()):
            return True
        else:
            return False

    # To upload file and its metadata to the blob storage
    def upload_file(self,  full_file_path):
        local_file_name = os.path.basename(full_file_path)
        
        if (self.check_file_in_azstorage(local_file_name)):
            logger.info("This file " + local_file_name + " exists in the container. No need to upload!")
            return False

        logger.info("Temp file = " + full_file_path)
        logger.info("\nUploading to Blob storage as blob " + local_file_name)

        # Upload the created file, use local_file_name for the blob name.
        self.block_blob_service.create_blob_from_path(
           self.container_name, local_file_name, full_file_path)

        return True

    def list_blobs(self):
        blobs = []

        # List the blobs in the container.
        logger.info("\nList blobs in the container")
        generator = self.block_blob_service.list_blobs(self.container_name)
        for blob in generator:
            #print("\t Blob name: " + blob.name)
            blobs.append(blob.name)

        return blobs


    # To download file(s) or entirely blob storage
    def download_container(self, download_dir):
        # Check if the download_dir exists
        if not os.path.exists(os.path.abspath(download_dir)):
            os.makedirs(os.path.abspath(download_dir))

        blobs = self.list_blobs()
        for blob_file_name in blobs:
            # Download the blob(s) to the download directory
            local_file_path = os.path.join(download_dir, blob_file_name)
            self.download_blob(blob_file_name, local_file_path)

    # Download the individual blob to the local path
    def download_blob(self, blob_name, blob_local_path):
        logger.info("\nDownloading blob to " + blob_local_path)
        self.block_blob_service.get_blob_to_path(
            self.container_name, blob_name, blob_local_path)


    # Create the link which anonymous user can download if they want.
    def create_link_to_download():
        pass  


if __name__ == "__main__":
    logger.info("Hello from upload to Azure Blob Container!")
    #create_container_storage("quickstartblobs")
    #container = "quickstartblobs"
    container = "sgdecoding-online"

    blob_service = AzureBlobContainerService(container)
    all_files = list_all_files("/export/data/raw") 
    for afile in all_files:
        logger.info("Uploading a file to blob storage: " + afile)
        blob_service.upload_file(afile)

    blob_service.list_blobs()
    blob_service.download_container("testdir")

