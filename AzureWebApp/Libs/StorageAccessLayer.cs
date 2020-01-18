using System;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Microsoft.Azure.Storage.Auth;
using Microsoft.Azure.Services.AppAuthentication;
using Newtonsoft.Json.Linq;
using Microsoft.Azure.Storage.Blob;
using System.IO;

namespace AzureWebApp.Libs
{
    public class BlobStorageEntry
    {
        //  Storage Account Name will be created for Site Entity
        public string StorageAccountName { get; set; }

        //  Provider Connection String. We need this attribute for on-premize version  
        public string StorageProviderConnectionString { get; set; }


        // Container Name
        public string ContainerName { get; set; }

        // Stream for Upload to Blob Storage or for DownLoad from Blob Storage 
        public Stream StreamObject { get; set; }
    }

    public class StorageAccessLayer
    {
        private readonly ILogger _logger;
        
        public StorageAccessLayer(ILogger logger)
        {
            _logger = logger;
        }
        
        /// <summary>
        /// Azure method for DownLoad Stream from Blob's Storage
        /// </summary>
        /// <param name="blobEntry">Record that includes Account Storage, Blob's Container, StreamObject to Download from Blob Storage</param>
        /// <param name="blobName">String that represents Blob Name</param>
        /// <returns>true in case of success</returns>
        public async Task<bool> DownLoadStreamFromBlobStorage(BlobStorageEntry blobEntry, string blobName)
        {
            bool successDownLoad = false;            

            AzureServiceTokenProvider azureServiceTokenProvider = null;

            long bufferSizeForParallelOperations = 1024 * 1024 * 4; 

            string UriBlobName = String.Format("https://{0}.{1}/{2}/{3}", blobEntry.StorageAccountName, "blob.core.windows.net", blobEntry.ContainerName, blobName);

            string AzureProviderConnectionString = blobEntry.StorageProviderConnectionString;

            if (String.IsNullOrEmpty(AzureProviderConnectionString))
            {
                //  Azure Implementation
                azureServiceTokenProvider = new AzureServiceTokenProvider();
            }
            else
            {
                // On-Premize Implementation
                azureServiceTokenProvider = new AzureServiceTokenProvider(AzureProviderConnectionString);
            }

            // Get the access token. This is refreshing automatically 
            string accessTokenAcquired = await azureServiceTokenProvider.GetAccessTokenAsync("https://storage.azure.com/");

            var tokenCredential = new TokenCredential(accessTokenAcquired);

            StorageCredentials storageCredentials = new StorageCredentials(tokenCredential);

            CloudBlockBlob cloudBlockBlob = new CloudBlockBlob(new Uri(UriBlobName), storageCredentials);

            try
            {
                bool blobExist = await cloudBlockBlob.ExistsAsync();

                if (blobExist)
                {

                    //  Fetch Blob Attributes 
                    await cloudBlockBlob.FetchAttributesAsync();

                    long blobSize = cloudBlockBlob.Properties.Length;

                    _logger.LogInformation("Try to DownLoad Stream Blob blob '{0}' From Container '{1}' ", blobName, blobEntry.ContainerName);


                    if (blobSize > bufferSizeForParallelOperations)
                    {

                        BlobRequestOptions parallelThreadCountOptions = new BlobRequestOptions();
                        // Sets the number of blocks that may be simultaneously downloaded.
                        parallelThreadCountOptions.ParallelOperationThreadCount = 20; // Maximum for file size 2 GB    

                        await cloudBlockBlob.DownloadToStreamAsync(blobEntry.StreamObject, null, parallelThreadCountOptions, null);

                    }
                    else
                    {
                        await cloudBlockBlob.DownloadToStreamAsync(blobEntry.StreamObject);
                    }

                    _logger.LogInformation("Successfull DownLoad Stream Blob '{0}' From Container '{1}' ", blobName, blobEntry.ContainerName);
                   
                    successDownLoad = true;
                }
            }
            catch (Exception ex)
            {                
                _logger.LogError(ex.Message);
            }

            return successDownLoad;
        }        
    }
}
