using System;
using System.Collections.Generic;
using System.Text;
using System.IO;


namespace AzureWebApp.Model
{

    public class BlobStorageEntry
    {
       
        //  Storage Account Name will be created for Site Entity
        public string StorageAccountName { get; set; } 

        //  Provider Connection String. We need this attribute for on-premize version  
        public string StorageProviderConnectionString { get; set; } 


        // Container Name
        public string ContainerName { get; set; }

        // Search service for indexing Blobs
        public string SearchServiceName { get; set; }

        // Search service Admin Key
        public string SearchServiceAdminApiKey { get; set; }

        // LocalFile Name for Upload to Blob Storage or for DownLoad from Blob Storage 
        public string FileName { get; set; }

        // Local Folder Name for Upload to Blob Storage or for DownLoad from Blob Storage 
        public string FolderName { get; set; }


        // Stream for Upload to Blob Storage or for DownLoad from Blob Storage 
        public Stream StreamObject { get; set; }

        // Idetification of Blob Storage (Azure, Google Blob, Amazon Blob, e.g.)
        public bool UseAzureStorage { get; set; }

        //metaData is attachment defined on: http://www.hl7.org/fhir/datatypes.html#Attachment
        public string metaData { get; set; }
    }

    public abstract class StorageContext
    {
        //Abstract methods or members
    }

    public class BlobStorageAccounts : StorageContext
    {
        // Container Name  will be created for each organization 
        public string ContainerName { get; set; }

        //  Storage Account Name
        public string StorageAccountName  { get; set; } // AccountName
    }   
}