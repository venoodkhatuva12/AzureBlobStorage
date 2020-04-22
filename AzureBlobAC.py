#!/bin/python3
#Script to pull blob data from Azure Storage Account
#Author: Vinod.N K
##########################
#Prerequisites
#Usage: Python3, Python3-pip 
#Install the Azure Storage Blobs client library for Python with pip:
### pip3 install azure-storage-blob --pre ###
#Distro : Linux -Centos, Rhel, and any fedora
#####################


#   Azure Storage Blobs client library for Python
from azure.storage.blob import BlockBlobService

from azure.storage.blob import PublicAccess

import os

#name of your storage account and the access key from Settings->AccessKeys->key1
block_blob_service = BlockBlobService(account_name="TestBlobStorageAC",account_key="ABCDEFGHIJKLMNOPKQRSTUVWXYZHcQ4kmNm293q3zx+IdQ2685hj8HUQO1Qg0ZTQMH4HtUZ6NWwLy==")

#Function to sort and clear JSON
def createJsonArray(path=""):
    file=open(path,'r')
    lines = file.readlines()
    single_json_list = []
    
    json_arr = "["
    delimiter=""
    
    for line in lines:
        if line.strip():
            json_arr = json_arr + delimiter + line.strip() + "\n"
            delimiter=","
    json_arr = json_arr + "]"
    file1 = open(path+"_arr.json",'w')
    file1.write(json_arr)
    file1.close()
    os.remove(path)
    

#name of the container
generator = block_blob_service.list_blobs('BlobContainerName')

#code below lists all the blobs in the container and downloads them one after another
for blob in generator:
    print(blob.name)
    print("{}".format(blob.name))
    #check if the path contains a folder structure, create the folder structure
    if "/" in "{}".format(blob.name):
        print("there is a path in this")
        #extract the folder path and check if that folder exists locally, and if not create it
        head, tail = os.path.split("{}".format(blob.name))
        print(head)
        print(tail)
        print(os.getcwd()+ "/" )
        if (os.path.isdir(os.getcwd()+ "/" + head)):
        #       #download the files to this directory
            print("directory and sub directories exist")
            block_blob_service.get_blob_to_path('BlobContainerName',blob.name,os.getcwd()+ "/" + head + "/" + tail)
            createJsonArray(path=os.getcwd()+ "/" + head + "/" + tail)
            
        else:
        #   create the diretcory and download the file to it
            print("directory doesn't exist, creating it now")
            print(" dir -> "+os.getcwd()+ "/" + head)
            os.makedirs(name=os.getcwd()+ "/" + head, exist_ok=True)
            #os.mkdir(os.getcwd()+ "/" + head)
            print("directory created, download initiated")
            block_blob_service.get_blob_to_path('BlobContainerName',blob.name,os.getcwd()+ "/" + head + "/" + tail)
            createJsonArray(path=os.getcwd()+ "/" + head + "/" + tail)
            
#    block_blob_service.get_blob_to_path('cober-test-stand',blob.name,blob.name,blob.name,blob.name         
    else:
        block_blob_service.get_blob_to_path('BlobContainerName',blob.name,blob.name)
        print()
        block_blob_service.get_blob_to_path('BlobContainerName',blob.name,head + "/"+tail)
