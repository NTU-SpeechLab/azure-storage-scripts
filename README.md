# azure-storage-scripts

These scripts to manage and upload all user data (raw audio and transcription) from decoding server to Azure blob storage and table to reduce storage in hard-disk for the decoding server.

## Dependency 

    pip install azure-storage
    pip install slugify

  To run these scripts, you need to have astorage account, and key. Go to the Azure portal and your storage account -> Settings -> Access key
  <add picture here>
  
## Azure Blob Container

  This script/class contain API/functions to create a container. In each container, you can upload blobs to this container. Blob can be any type of your data (text, media file, etc.)
  
  With this script (azure_blob.py), you can do following functions
  * Check if a blob/file already exists in the container (check_file_in_azstorage)
  * Upload your file to Azure container. If the same name, the new file will override the old file (upload_file_
  * List all files/blobs in a container.
  * Download all blobs in a container or specific blobs (download_container, download_blob)
  * [Not available yet] Create a link for anonymous user to download, without exposing our account information.


## Azure Table

  Azure table is more like SQL table, contain primiary key (PartitionKey and RowKey combined) and the other data columns as needed. 
  
  With this script (azure_table.py), you can do the following functions:
  * Create the table name (create_table_name)
  * Add an entity (a row) to the table (add_entity_to_table)
  * Update the existing entity in the table (update_table_entity)
  * Search if entity is already there (search_table_entity)
  * Delete the table (delete_table)
  
  
## Upload online ASR data

  To leverage above scripts, online ASR data is uploading to both Azure blob (real data: audio and transcription) and Azure table (metadata).
  * Azure blob: each pair of audio and text data goes together (same name).
  * Azure table: Metadata is created for audio file only, and including
  
        PartitionKey: sgdecoding_<online|offline>_<eng|cn|malay|csmalay>
        RowKey: filename
        container_name: To retrieve the real data if need
        datetime: The datetime of the audio file created.
        client_ip_address: The IP address where the data from
        server_ip_address: The sgdecoding server IP address.
        language: Language
        decoding_status: finished or unknown (error happened)
        time_to_decode: in seconds
        requested_args: Requested arguments from user, such as content type, token
        ... And other auto-generated columns of the Azure table
        

The end.

  

