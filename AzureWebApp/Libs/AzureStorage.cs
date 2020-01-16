
// #define TASK 

using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Microsoft.Azure.Storage;
using System.IO;
using Microsoft.Extensions.Logging;
using Microsoft.Azure.Storage.Auth;
using Microsoft.Azure.Services.AppAuthentication;
using Newtonsoft.Json.Linq;
using Microsoft.Azure.Storage.Blob;
using System.Diagnostics;
using AzureWebApp.Model;

namespace AzureWebApp.Libs
{
    public class AzureStorage
    {
        private readonly ILogger _logger;
        const int DelayRetry = 100;
     
        public AzureStorage(ILogger logger)
        {
            // Create Logger Instance 
            _logger = logger;
        }

        /// <summary>
        /// Azure method for deleting a file from blob storage
        /// </summary>
        /// <param name="blobEntry"></param>
        /// <param name="blobName"></param>
        /// <returns></returns>
        public async Task<bool> DeleteFromBlobStorage(BlobStorageEntry blobEntry, string blobName)
        {
            bool successDelete = false;
            string errorMessage = String.Empty;
            string containerName = String.Empty;

            CloudBlobContainer cloudBlobContainer = null;
            CloudBlockBlob cloudBlockBlob = null;

            string UriBlobName = String.Empty;
         
            AzureServiceTokenProvider azureServiceTokenProvider = null;

            UriBlobName = String.Format("https://{0}.{1}/{2}/{3}", blobEntry.StorageAccountName, "blob.core.windows.net", blobEntry.ContainerName, blobName);

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

            // Get the i access token. This is refreshing automatically 
            string accessTokenAcquired = await azureServiceTokenProvider.GetAccessTokenAsync("https://storage.azure.com/");

            var tokenCredential = new TokenCredential(accessTokenAcquired);

            StorageCredentials storageCredentials = new StorageCredentials(tokenCredential);

            cloudBlockBlob = new CloudBlockBlob(new Uri(UriBlobName), storageCredentials);

            cloudBlobContainer = cloudBlockBlob.Container;

            try
            { 
                bool containerExist = await cloudBlobContainer.ExistsAsync();

                if (containerExist)
                {
                     successDelete = await cloudBlockBlob.DeleteIfExistsAsync();

                    _logger.LogInformation("delete blob '{0}' from Container  '{1}'", blobName, cloudBlobContainer.Name);
                }
            }
            catch (Exception ex)
            {
                errorMessage = ex.Message;
                _logger.LogError(errorMessage);
            }
                     
            return successDelete;
        }
 
       
        /// <summary>
        /// Azure method for Upload File to Blob's Storage
        /// </summary>
        /// <param name="blobEntry">Record that includes Account Storage,  Blob's Container, FileName Location for Upload  </param>
        /// <param name="blobName">String that represents Blob Name</param>
        /// <param name="RetryCount ">Number of Retries in case of error</param> 
        /// <returns>true in case of success</returns>
        public async Task<bool> UploadObjectToBlobStorage(BlobStorageEntry blobEntry, string blobName, string metaData = "", int RetryCount = 5)
        {
            bool successUpload = false;
            string errorMessage = String.Empty;
            string containerName = String.Empty;

            CloudBlobContainer cloudBlobContainer = null;
            CloudBlockBlob cloudBlockBlob = null;
            int errorCreatingBlobContainer = 0;
            string UriBlobName = String.Empty;
            bool bCreateContainer = false;
            AzureServiceTokenProvider azureServiceTokenProvider = null;
            BlobRequestOptions parallelThreadCountOptions = null;


            long bytesToUpload = (new FileInfo(blobEntry.FileName)).Length;

            long bufferSizeForParallelOperations = 1024 * 1024 * 4; // 4 MB; 

            UriBlobName = String.Format("https://{0}.{1}/{2}/{3}", blobEntry.StorageAccountName, "blob.core.windows.net", blobEntry.ContainerName, blobName);

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

            cloudBlockBlob = new CloudBlockBlob(new Uri(UriBlobName), storageCredentials);


            cloudBlobContainer = cloudBlockBlob.Container;

            while (errorCreatingBlobContainer < RetryCount)
            {
                try
                {
                    // Work Around for Possible Error 
                    // See https://github.com/Azure/azure-sdk-for-net/issues/109
                    bool bContainerExists = await cloudBlobContainer.ExistsAsync();
                    if (!bContainerExists)
                    {
                        bCreateContainer = await cloudBlobContainer.CreateIfNotExistsAsync();
                    }

                    if (bCreateContainer)
                    {
                        _logger.LogInformation("Create Container '{0}'", cloudBlobContainer.Name);
                    }
                    else
                    {
                        _logger.LogInformation("Extract container '{0}'", cloudBlobContainer.Name);
                    }

                    //metaData is attachment defined on: http://www.hl7.org/fhir/datatypes.html#Attachment
                    if (!string.IsNullOrEmpty(metaData))
                    {
                        JObject metaObj = JObject.Parse(metaData);
                        if (metaObj.GetValue("contentType") != null)
                        {
                            cloudBlockBlob.Properties.ContentType = metaObj["contentType"].ToString();
                        }
                        if (metaObj.GetValue("language") != null)
                        {
                            cloudBlockBlob.Properties.ContentLanguage = metaObj["language"].ToString();
                        }
                        if (metaObj.GetValue("hash") != null)
                        {
                            cloudBlockBlob.Metadata.Add("hash", metaObj["hash"].ToString());
                        }
                        if (metaObj.GetValue("title") != null)
                        {
                            cloudBlockBlob.Metadata.Add("title", metaObj["title"].ToString());
                        }
                        if (metaObj.GetValue("creation") != null)
                        {
                            cloudBlockBlob.Metadata.Add("creation", metaObj["creation"].ToString());
                        }
                    }

                    //    Below the the important aricle for understanding Blob Storage
                    //    https://stackoverflow.com/questions/36714362/why-upload-to-azure-blob-so-slow
                    //     Example 
                    //    https://docs.microsoft.com/en-us/dotnet/api/microsoft.azure.storage.blob.blobrequestoptions.paralleloperationthreadcount?view=azure-dotnet-legacy#examples
                    //

                    parallelThreadCountOptions = new BlobRequestOptions();

                    // We don't want t calculate CONTENT_MD5 in Client Side
                    parallelThreadCountOptions.StoreBlobContentMD5 = false;

                    if (bytesToUpload <= bufferSizeForParallelOperations) // Small Files
                    {
                        // For Small Files it is more effective perform SingleStream Operations with one thread
                        parallelThreadCountOptions.ParallelOperationThreadCount = 1;
                    }
                    else //  Big Files
                    {
                        cloudBlockBlob.StreamWriteSizeInBytes += 1; // To Set Multiple Streams 
                                            
                        // Sets the number of blocks that may be simultaneously uploaded.
                        parallelThreadCountOptions.ParallelOperationThreadCount = 20; // Maximum for file size 2 GB                         
                    }
  
                    await cloudBlockBlob.UploadFromFileAsync(blobEntry.FileName, null, parallelThreadCountOptions, null);
                    _logger.LogInformation("Upload blob '{0}'  to Container  '{1}'", blobName, cloudBlobContainer.Name);
                    successUpload = true;
                    break; // Exit From Loop
                }
                catch (StorageException storageex)
                {
                    errorCreatingBlobContainer++; // Retry 
                    errorMessage = storageex.Message + String.Format("   ,Retry Number {0}", errorCreatingBlobContainer);
                    _logger.LogError(errorMessage);
                    await Task.Delay(DelayRetry);
                }
                catch (Exception ex)
                {
                    errorMessage = ex.Message;
                    _logger.LogError(errorMessage);
                    break;
                }
            }

            return successUpload;
        }

        /// <summary>
        /// Azure method for Create Container of Blob's Storage
        /// </summary>
        /// <param name="blobEntry">Record that includes Account Storage,  Blob's Container, FileName Location for Upload  </param>
        /// <param name="RetryCount ">Number of Retries in case of error</param> 
        /// <returns>true in case of success</returns>
        public async Task<bool> CreateContainerForBlobStorage(BlobStorageEntry blobEntry, int RetryCount = 5)
        {
            bool successCreation  = false;
            string errorMessage = String.Empty;
            string containerName = String.Empty;

            CloudBlobContainer cloudBlobContainer = null;
            int errorCreatingBlobContainer = 0;
            string UriContainerName = String.Empty;
            bool bCreateContainer = false;
            AzureServiceTokenProvider azureServiceTokenProvider = null;

            UriContainerName = String.Format("https://{0}.{1}/{2}", blobEntry.StorageAccountName, "blob.core.windows.net", blobEntry.ContainerName);

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

            cloudBlobContainer = new CloudBlobContainer(new Uri(UriContainerName), storageCredentials);

            while (errorCreatingBlobContainer < RetryCount)
            {
                try
                {
                    // Work Around for Possible Error 
                    // See https://github.com/Azure/azure-sdk-for-net/issues/109
                    bool bContainerExists = await cloudBlobContainer.ExistsAsync();
                    if (!bContainerExists)
                    {
                        bCreateContainer = await cloudBlobContainer.CreateIfNotExistsAsync();
                    }

                    if (bCreateContainer)
                    {
                        _logger.LogInformation("Create Container '{0}'", cloudBlobContainer.Name);
                    }
                    else
                    {
                        _logger.LogInformation("Extract container '{0}'", cloudBlobContainer.Name);
                    }
  
                    successCreation = true;
                    break; // Exit From Loop
                }
                catch (StorageException storageex)
                {
                    errorCreatingBlobContainer++; // Retry 
                    errorMessage = storageex.Message + String.Format("   ,Retry Number {0}", errorCreatingBlobContainer);
                    _logger.LogError(errorMessage);
                    await Task.Delay(DelayRetry);
                }
                catch (Exception ex)
                {
                    errorMessage = ex.Message;
                    _logger.LogError(errorMessage);
                    break;
                }
            }

            return successCreation;
        }

        /// <summary>
        /// Azure method for DownLoad File from Blob's Storage
        /// </summary>
        /// <param name="blobEntry">Record that includes Account Storage, Blob's Container, FileName Location to Download from Blob Storage</param>
        /// <param name="blobName">String that represents Blob Name</param>
        /// <returns>true in case of success</returns>
        public async Task<bool> DownLoadObjectFromBlobStorage(BlobStorageEntry blobEntry, string blobName)
        {
            bool successDownLoad = false;
            string errorMessage = String.Empty;
            string containerName = String.Empty;
            CloudBlockBlob cloudBlockBlob = null;

            string UriBlobName = String.Empty;

            AzureServiceTokenProvider azureServiceTokenProvider = null;

            // Corresponding to modificatins of Microsoft Storage Team (RSNA - December 2019)
            // Kristof.Rennen@microsoft.com
            // bufferSizeForParallelOperations was modified to 4 MB
            long bufferSizeForParallelOperations = 1024 * 1024 * 4; 

            UriBlobName = String.Format("https://{0}.{1}/{2}/{3}", blobEntry.StorageAccountName, "blob.core.windows.net", blobEntry.ContainerName, blobName);

            string AzureProviderConnectionString = blobEntry.StorageProviderConnectionString;

            if (String.IsNullOrEmpty(AzureProviderConnectionString))
            {
                //  Azure Implementation
                azureServiceTokenProvider = new AzureServiceTokenProvider();
            }
            else
            {
                // No-Premize Implementation
                azureServiceTokenProvider = new AzureServiceTokenProvider(AzureProviderConnectionString);
            }

            // Get the i access token. This is refreshing automatically 
            string accessTokenAcquired = await azureServiceTokenProvider.GetAccessTokenAsync("https://storage.azure.com/");

            var tokenCredential = new TokenCredential(accessTokenAcquired);

            StorageCredentials storageCredentials = new StorageCredentials(tokenCredential);

            cloudBlockBlob = new CloudBlockBlob(new Uri(UriBlobName), storageCredentials);

            try
            {
                bool blobExist = await cloudBlockBlob.ExistsAsync();
                if (blobExist)
                {

                    //  Fetch Blob Attributes 
                    await cloudBlockBlob.FetchAttributesAsync();

                    long blobSize = cloudBlockBlob.Properties.Length;

                    _logger.LogInformation("Try to DownLoad blob '{0}' From Container '{1}' to File '{2}' ", blobName, blobEntry.ContainerName, blobEntry.FileName);        
                                    
                    if (blobSize > bufferSizeForParallelOperations)
                    {
                        // It is recommended by  Microsoft set parallelIOCount to number of CPUs
                        Int32 parallelOperationThreadCount = Environment.ProcessorCount;

                        // For rangeSizeInBytes using default value. This value give good result for 309 MB File 
                        // For the file 309 MB rangeSizeInBytes = 100 MB give the execution time 5432 msec
                        // For the file 309 MB default value of rangeSizeInBytes  give the execution time 3548 msec

                        long? rangeSizeInBytes = bufferSizeForParallelOperations; 

                        // DownLoad 
                        await cloudBlockBlob.DownloadToFileParallelAsync(blobEntry.FileName, FileMode.Create, parallelOperationThreadCount, rangeSizeInBytes);
                    }
                    else // It is recommended by Microsoft for small files using DownloadToFileAsyncint 
                    {
                        await cloudBlockBlob.DownloadToFileAsync(blobEntry.FileName, FileMode.Create);
                    }
      
                                     
                    JObject metaObject = new JObject();
                    metaObject.Add("contentType", cloudBlockBlob.Properties.ContentType);
                    metaObject.Add("language", cloudBlockBlob.Properties.ContentLanguage);
                    string metaValue;
                    cloudBlockBlob.Metadata.TryGetValue("hash", out metaValue);
                    if (!string.IsNullOrEmpty(metaValue))
                    {
                        metaObject.Add("hash", metaValue);
                    }
                    cloudBlockBlob.Metadata.TryGetValue("title", out metaValue);
                    if (!string.IsNullOrEmpty(metaValue))
                    {
                        metaObject.Add("title", metaValue);
                    }
                    cloudBlockBlob.Metadata.TryGetValue("creation", out metaValue);
                    if (!string.IsNullOrEmpty(metaValue))
                    {
                        metaObject.Add("creation", metaValue);
                    }

                    blobEntry.metaData = metaObject.ToString();

                    _logger.LogInformation("Successfully DownLoad blob '{0}' From Container '{1}' to File '{2}' ", blobName, blobEntry.ContainerName, blobEntry.FileName);

                    successDownLoad = true;
                }
            }
            catch (Exception ex)
            {
                errorMessage = ex.Message;
                _logger.LogError(errorMessage);
            }
                      
            return successDownLoad;
        }

        /// <summary>
        /// Azure method for DownLoad File from Blob's Storage
        /// </summary>
        /// <param name="blobEntry">Record that includes Account Storage, Blob's Container, Folder Location to Download from Blob Storage</param>
        /// <param name="blobFolder">String that represents Blob Folder</param>
        /// <returns>true in case of success</returns>
        public async Task<bool> DownLoadFolderFromBlobStorage(BlobStorageEntry blobEntry, string blobFolder)
        {
            bool successDownLoad = false;
            string errorMessage = String.Empty;
            string containerName = String.Empty;

            CloudBlobContainer cloudBlobContainer = null;
            CloudBlobDirectory cloudBlobDirectory = null;
            CloudBlockBlob blockBlob = null;
            BlobResultSegment resultSegment = null;
          
            string UriBlobName = String.Empty;

            AzureServiceTokenProvider azureServiceTokenProvider = null;

            UriBlobName = String.Format("https://{0}.{1}/{2}/{3}", blobEntry.StorageAccountName, "blob.core.windows.net", blobEntry.ContainerName, blobFolder);

            string AzureProviderConnectionString = blobEntry.StorageProviderConnectionString;

            if (String.IsNullOrEmpty(AzureProviderConnectionString))
            {
                //  Azure Implementation
                azureServiceTokenProvider = new AzureServiceTokenProvider();
            }
            else
            {
                // No-Premize Implementation
                azureServiceTokenProvider = new AzureServiceTokenProvider(AzureProviderConnectionString);
            }

            //  Get the access token. This is refreshing automatically
            string accessTokenAcquired = await azureServiceTokenProvider.GetAccessTokenAsync("https://storage.azure.com/");

            var tokenCredential = new TokenCredential(accessTokenAcquired);

            StorageCredentials storageCredentials = new StorageCredentials(tokenCredential);

            blockBlob = new CloudBlockBlob(new Uri(UriBlobName), storageCredentials);

            cloudBlobContainer = blockBlob.Container;

            try
            {

                _logger.LogInformation("Extract container '{0}'", cloudBlobContainer.Name);
              
                bool bBlobExist = await blockBlob.ExistsAsync();
                if (bBlobExist)
                {
                    await DownLoadBlobFromStorage(blobEntry.FolderName, blockBlob);
                }
                else
                {
                    BlobContinuationToken blobContinuationToken = null;
                    cloudBlobDirectory = cloudBlobContainer.GetDirectoryReference(blobFolder);
                    do
                    {
                        resultSegment = await cloudBlobDirectory.ListBlobsSegmentedAsync(blobContinuationToken);
                        blobContinuationToken = resultSegment.ContinuationToken;
                        {
                            foreach (var blobItem in resultSegment.Results)
                            {
                                Type blobItemType = blobItem.GetType();
                                if (blobItemType.Name == "CloudBlockBlob")
                                {
                                    blockBlob = (CloudBlockBlob)blobItem;
                                    await DownLoadBlobFromStorage(blobEntry.FolderName, blockBlob);
                                }
                                else if (blobItemType.Name == "CloudBlobDirectory")
                                {
                                    CloudBlobDirectory blobSubDirectory = (CloudBlobDirectory)blobItem;
                                    await DownLoadSubFoldersFromBlobStorage(blobEntry, blobSubDirectory);
                                }
                            }
                        }
                    } while (blobContinuationToken != null); // Loop while the continuation token is not null. 

                }
                successDownLoad = true;
            }
            catch (Exception ex)
            {
                errorMessage = ex.Message;
                _logger.LogError(errorMessage);
            }

            return successDownLoad;
        }

        private async Task<bool> DownLoadBlobFromStorage(string blobEntryFolder, CloudBlockBlob blockBlob)
        {
            bool successDownLoad = false;
            string errorMessage = String.Empty;

            _logger.LogInformation("Downloading '{0}'", blockBlob.Name);
            DirectoryInfo dirInfo = new DirectoryInfo(blobEntryFolder);
            DirectoryInfo parentDirectory = dirInfo.CreateSubdirectory(blockBlob.Parent.Prefix);
            string FullFolderName = parentDirectory.FullName;

            int LastIndex = blockBlob.Name.LastIndexOf('/');
            string fileNameToDownLoad = blockBlob.Name.Substring(LastIndex + 1);

            await blockBlob.DownloadToFileAsync(FullFolderName + "/" + fileNameToDownLoad, FileMode.Create);
            successDownLoad = true;
            return successDownLoad;
        }

        /// <summary>
        ///  This Method Download The Files from SubDirectory
        ///  The method is recursive until we have found the CloudBlockBlob
        /// </summary>
        /// <param name="blobEntry"></param>
        /// <param name="blobDirectory"></param>
        /// <returns></returns>
        private async Task<bool> DownLoadSubFoldersFromBlobStorage(BlobStorageEntry blobEntry,  CloudBlobDirectory blobDirectory)
        {
            bool successDownLoad = false;
            string errorMessage = String.Empty;

            BlobResultSegment resultSegment = null;
            BlobContinuationToken blobContinuationToken = null;
         
            try
            {                  
                do
                {
                    resultSegment = await blobDirectory.ListBlobsSegmentedAsync(blobContinuationToken);
                    blobContinuationToken = resultSegment.ContinuationToken;

                    blobContinuationToken = resultSegment.ContinuationToken;
                    foreach (var blobItem in resultSegment.Results)
                    {
                        Type blobItemType = blobItem.GetType();
                        if (blobItemType.Name == "CloudBlockBlob")
                        {
                            CloudBlockBlob blockBlob = (CloudBlockBlob)blobItem;
                            await DownLoadBlobFromStorage(blobEntry.FolderName, blockBlob);
                        }
                        else
                        {
                            CloudBlobDirectory blobSubDirectory = (CloudBlobDirectory)blobItem;
                            await DownLoadSubFoldersFromBlobStorage(blobEntry, blobSubDirectory);
                        }
                    }
                } while (blobContinuationToken != null); // Loop while the continuation token is not null. 

                successDownLoad = true;
            }
            catch (Exception ex)
            {
                errorMessage = ex.Message;
                _logger.LogError(errorMessage);
            }
 
            return successDownLoad;
        }

        /// <summary>
        ///  This Method Download The Objects from SubDirectory
        ///  The method is recursive until we have found the CloudBlockBlob
        /// </summary>
        /// <param name="blobEntry"></param>
        /// <param name="blobDirectory"></param>
        /// <returns></returns>
        private async Task<bool> DownLoadSubFoldersFromBlobStorage(BlobStorageEntry blobEntry, CloudBlobDirectory blobDirectory, List<MemoryStream> streamsToDownLoad)
        {
            bool successDownLoad = false;
            string errorMessage = String.Empty;

            BlobResultSegment resultSegment = null;
            BlobContinuationToken blobContinuationToken = null;
            MemoryStream currentStream = null;

            try
            {
                do
                {
                    resultSegment = await blobDirectory.ListBlobsSegmentedAsync(blobContinuationToken);
                    blobContinuationToken = resultSegment.ContinuationToken;

                    blobContinuationToken = resultSegment.ContinuationToken;
                    foreach (var blobItem in resultSegment.Results)
                    {
                        Type blobItemType = blobItem.GetType();
                        if (blobItemType.Name == "CloudBlockBlob")
                        {
                            CloudBlockBlob blockBlob = (CloudBlockBlob)blobItem;
                            currentStream = new MemoryStream();

                            await blockBlob.DownloadToStreamAsync(currentStream);
                            streamsToDownLoad.Add(currentStream);
                        }
                        else
                        {
                            CloudBlobDirectory blobSubDirectory = (CloudBlobDirectory)blobItem;
                            await DownLoadSubFoldersFromBlobStorage(blobEntry, blobSubDirectory, streamsToDownLoad);
                        }
                    }
                } while (blobContinuationToken != null); // Loop while the continuation token is not null. 

                successDownLoad = true;
            }
            catch (Exception ex)
            {
                errorMessage = ex.Message;
                _logger.LogError(errorMessage);
            }

            return successDownLoad;
        }

        /// <summary>
        /// Azure method for Upload Stream to Blob's Storage
        /// </summary>
        /// <param name="blobEntry">Record that includes Account Storage, Blob's Container, Stream to Upload  </param>
        /// <param name="blobName">String that represents Blob Name</param>
        /// <param name="RetryCount ">Number of Retries in case of error</param> 
        /// <returns>true in case of success</returns>
        public async Task<bool> UploadStreamToBlobStorage(BlobStorageEntry blobEntry, string blobName, string metaData = "", int RetryCount = 5)
        {
            bool successUpload = false;
            string errorMessage = String.Empty;
            string containerName = String.Empty;

            CloudBlobContainer cloudBlobContainer = null;
            CloudBlockBlob cloudBlockBlob = null;
            int errorCreatingBlobContainer = 0;
            string UriBlobName = String.Empty;
            bool bCreateContainer = false;
            AzureServiceTokenProvider azureServiceTokenProvider = null;

            BlobRequestOptions parallelThreadCountOptions = null;

            long bufferSizeForParallelOperations = 1024 * 1024 * 4; // 4 MB

            UriBlobName = String.Format("https://{0}.{1}/{2}/{3}", blobEntry.StorageAccountName, "blob.core.windows.net", blobEntry.ContainerName, blobName);

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

            // Get the i access token. This is refreshing automatically 
            string accessTokenAcquired = await azureServiceTokenProvider.GetAccessTokenAsync("https://storage.azure.com/");

            var tokenCredential = new TokenCredential(accessTokenAcquired);

            StorageCredentials storageCredentials = new StorageCredentials(tokenCredential);

            cloudBlockBlob = new CloudBlockBlob(new Uri(UriBlobName), storageCredentials);

            cloudBlobContainer = cloudBlockBlob.Container;

            while (errorCreatingBlobContainer < RetryCount)
            {
                try
                {
                    // Work Around for Possible Error 
                    // See https://github.com/Azure/azure-sdk-for-net/issues/109
                    bool bContainerExists = await cloudBlobContainer.ExistsAsync();
                    if (!bContainerExists)
                    {
                        bCreateContainer = await cloudBlobContainer.CreateIfNotExistsAsync();
                    }

                    if (bCreateContainer)
                    {
                        _logger.LogInformation("Create Container '{0}'", cloudBlobContainer.Name);
                    }
                    else
                    {
                        _logger.LogInformation("Extract container '{0}'", cloudBlobContainer.Name);
                    }

                    //metaData is attachment defined on: http://www.hl7.org/fhir/datatypes.html#Attachment
                    if (!string.IsNullOrEmpty(metaData))
                    {
                        JObject metaObj = JObject.Parse(metaData);
                        if (metaObj.GetValue("contentType") != null)
                        {
                            cloudBlockBlob.Properties.ContentType = metaObj["contentType"].ToString();
                        }
                        if (metaObj.GetValue("language") != null)
                        {
                            cloudBlockBlob.Properties.ContentLanguage = metaObj["language"].ToString();
                        }
                        if (metaObj.GetValue("hash") != null)
                        {
                            cloudBlockBlob.Metadata.Add("hash", metaObj["hash"].ToString());
                        }
                        if (metaObj.GetValue("title") != null)
                        {
                            cloudBlockBlob.Metadata.Add("title", metaObj["title"].ToString());
                        }
                        if (metaObj.GetValue("creation") != null)
                        {
                            cloudBlockBlob.Metadata.Add("creation", metaObj["creation"].ToString());
                        }
                    }

                    //  Examples
                    //  https://docs.microsoft.com/en-us/dotnet/api/microsoft.azure.storage.blob.blobrequestoptions.paralleloperationthreadcount?view=azure-dotnet-legacy#examples
                    //

                    //    Below the the important aricle for understanding Blob Storage
                    //    https://stackoverflow.com/questions/36714362/why-upload-to-azure-blob-so-slow


                    parallelThreadCountOptions = new BlobRequestOptions();

                    // We don't want t calculate CONTENT_MD5 in Client Side
                    parallelThreadCountOptions.StoreBlobContentMD5 = false;

                    // Set StreamWriteSizeInBytes for Big Objects
                    if (blobEntry.StreamObject.Length < bufferSizeForParallelOperations) // Small Files 
                    {
                        // For Small Files it is more effective perform SingleStream Operations with one thread
                        parallelThreadCountOptions.ParallelOperationThreadCount = 1;
                    } 
                    else
                    {
                        // Sets the size of Block
                        cloudBlockBlob.StreamWriteSizeInBytes += 1; // To Set Multiple Streams

                        // Sets the number of blocks that may be simultaneously uploaded.
                        parallelThreadCountOptions.ParallelOperationThreadCount = 20;
                    }

                    await cloudBlockBlob.UploadFromStreamAsync(blobEntry.StreamObject, null, parallelThreadCountOptions,  null);                    
                    _logger.LogInformation("Upload Stream blob '{0}' to Container '{1}'", blobName, cloudBlobContainer.Name);
                    successUpload = true;
                    break; // Exit From Loop
                }
                catch (StorageException storageex)
                {
                    errorCreatingBlobContainer++; // Retry 
                    errorMessage = storageex.Message + String.Format("   ,Retry Number {0}", errorCreatingBlobContainer);
                    _logger.LogError(errorMessage);
                    await Task.Delay(DelayRetry);
                }
                catch (Exception ex)
                {
                    errorMessage = ex.Message;
                    _logger.LogError(errorMessage);
                    break;
                }
            }

            return successUpload;
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
            string errorMessage = String.Empty;
            string containerName = String.Empty;

            CloudBlockBlob cloudBlockBlob = null;

            string UriBlobName = String.Empty;

            AzureServiceTokenProvider azureServiceTokenProvider = null;

            // Corresponding to modificatins of Microsoft Storage Team (RSNA - December 2019)
            // Kristof.Rennen@microsoft.com
            //
            // bufferSizeForParallelOperations was modified to 4 MB
            long bufferSizeForParallelOperations = 1024 * 1024 * 4; 

            UriBlobName = String.Format("https://{0}.{1}/{2}/{3}", blobEntry.StorageAccountName, "blob.core.windows.net", blobEntry.ContainerName, blobName);

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

            cloudBlockBlob = new CloudBlockBlob(new Uri(UriBlobName), storageCredentials);

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

                    JObject metaObject = new JObject();
                    metaObject.Add("contentType", cloudBlockBlob.Properties.ContentType);
                    metaObject.Add("language", cloudBlockBlob.Properties.ContentLanguage);
                    string metaValue;
                    cloudBlockBlob.Metadata.TryGetValue("hash", out metaValue);
                    if (!string.IsNullOrEmpty(metaValue))
                    {
                        metaObject.Add("hash", metaValue);
                    }
                    cloudBlockBlob.Metadata.TryGetValue("title", out metaValue);
                    if (!string.IsNullOrEmpty(metaValue))
                    {
                        metaObject.Add("title", metaValue);
                    }
                    cloudBlockBlob.Metadata.TryGetValue("creation", out metaValue);
                    if (!string.IsNullOrEmpty(metaValue))
                    {
                        metaObject.Add("creation", metaValue);
                    }

                    blobEntry.metaData = metaObject.ToString();

                    _logger.LogInformation("Successfull DownLoad Stream Blob '{0}' From Container '{1}' ", blobName, blobEntry.ContainerName);
                   
                    successDownLoad = true;
                }
            }
            catch (Exception ex)
            {
                errorMessage = ex.Message;
                _logger.LogError(errorMessage);
            }

            return successDownLoad;
        }

        /// <summary>
        /// Azure method for Generating SAS URL
        /// </summary>
        /// <param name="blobEntry">Record that includes Account Storage, Blob's Container, Location of Blob Storage</param>
        /// <param name="blobName">String that represents Blob Name</param>
        /// <returns>true in case of success</returns>
        public async Task<CloudBlockBlob> GetCloudBlockBlob(BlobStorageEntry blobEntry, string blobName)
        {
            string sasURL = String.Empty;
            string errorMessage = String.Empty;
            string containerName = String.Empty;
            CloudBlockBlob cloudBlockBlob = null;
            string UriBlobName = String.Empty;
            AzureServiceTokenProvider azureServiceTokenProvider = null;
            UriBlobName = String.Format("https://{0}.{1}/{2}/{3}", blobEntry.StorageAccountName, "blob.core.windows.net", blobEntry.ContainerName, blobName);

            string AzureProviderConnectionString = blobEntry.StorageProviderConnectionString;

            try
            { 
                if (String.IsNullOrEmpty(AzureProviderConnectionString))
                {
                    //  Azure Implementation
                    azureServiceTokenProvider = new AzureServiceTokenProvider();
                }
                else
                {
                    // No-Premize Implementation
                    azureServiceTokenProvider = new AzureServiceTokenProvider(AzureProviderConnectionString);
                }

                // Get the  access token. This is refreshing automatically 
                string accessTokenAcquired = await azureServiceTokenProvider.GetAccessTokenAsync("https://storage.azure.com/");

                var tokenCredential = new TokenCredential(accessTokenAcquired);

                StorageCredentials storageCredentials = new StorageCredentials(tokenCredential);

                cloudBlockBlob = new CloudBlockBlob(new Uri(UriBlobName), storageCredentials);
                                       
                bool blobExist = await cloudBlockBlob.ExistsAsync();
                if (!blobExist)
                {
                    cloudBlockBlob = null;
                }
            }
            catch (Exception ex)
            {
                errorMessage = ex.Message;
                _logger.LogError(errorMessage);
            }

            return cloudBlockBlob;
        }


        /// <summary>
        /// Azure method for DownLoad Multiple Streamss from Blob's Storage
        /// </summary>
        /// <param name="blobEntry">Record that includes Account Storage, Blob's Container, StreamObject to Download from Blob Storage</param>
        /// <param name="blobFolder">String that represents Folder Name</param>
        /// <param name=" streamsToDownLoad">Output DownLoad Streams that represents Folder Name</param>
        /// <returns>true in case of success</returns>
        public async Task<bool> DownLoadMultipleStreamsFromBlobStorage(BlobStorageEntry blobEntry,
             string blobFolder, List<MemoryStream> streamsToDownLoad)
        {
            bool successDownLoad = false;
            string errorMessage = String.Empty;
            string containerName = String.Empty;

            CloudBlobContainer cloudBlobContainer = null;
            CloudBlobDirectory cloudBlobDirectory = null;
            CloudBlockBlob blockBlob = null;
            BlobResultSegment resultSegment = null;
            MemoryStream currentStream = null;

            string UriBlobName = String.Empty;

            AzureServiceTokenProvider azureServiceTokenProvider = null;

            UriBlobName = String.Format("https://{0}.{1}/{2}/{3}", blobEntry.StorageAccountName, "blob.core.windows.net", blobEntry.ContainerName, blobFolder);

            string AzureProviderConnectionString = blobEntry.StorageProviderConnectionString;

            if (String.IsNullOrEmpty(AzureProviderConnectionString))
            {
                //  Azure Implementation
                azureServiceTokenProvider = new AzureServiceTokenProvider();
            }
            else
            {
                // No-Premize Implementation
                azureServiceTokenProvider = new AzureServiceTokenProvider(AzureProviderConnectionString);
            }

            //  Get the access token. This is refreshing automatically
            string accessTokenAcquired = await azureServiceTokenProvider.GetAccessTokenAsync("https://storage.azure.com/");

            var tokenCredential = new TokenCredential(accessTokenAcquired);

            StorageCredentials storageCredentials = new StorageCredentials(tokenCredential);

            blockBlob = new CloudBlockBlob(new Uri(UriBlobName), storageCredentials);

            cloudBlobContainer = blockBlob.Container;

            try
            {

                _logger.LogInformation("Extract container '{0}'", cloudBlobContainer.Name);

                bool bBlobExist = await blockBlob.ExistsAsync();
                if (bBlobExist)
                {
                    currentStream = new MemoryStream();

                    await blockBlob.DownloadToStreamAsync(currentStream);
                    streamsToDownLoad.Add(currentStream);
                }
                else
                {
                    BlobContinuationToken blobContinuationToken = null;
                    cloudBlobDirectory = cloudBlobContainer.GetDirectoryReference(blobFolder);
                    do
                    {
                        resultSegment = await cloudBlobDirectory.ListBlobsSegmentedAsync(blobContinuationToken);
                        blobContinuationToken = resultSegment.ContinuationToken;
                        {
                            foreach (var blobItem in resultSegment.Results)
                            {
                                Type blobItemType = blobItem.GetType();
                                if (blobItemType.Name == "CloudBlockBlob")
                                {
                                    blockBlob = (CloudBlockBlob)blobItem;
                                    currentStream = new MemoryStream();
                                    await blockBlob.DownloadToStreamAsync(currentStream);
                                    streamsToDownLoad.Add(currentStream);
                                }
                                else if (blobItemType.Name == "CloudBlobDirectory")
                                {
                                    CloudBlobDirectory blobSubDirectory = (CloudBlobDirectory)blobItem;
                                    await DownLoadSubFoldersFromBlobStorage(blobEntry, blobSubDirectory, streamsToDownLoad);
                                }
                            }
                        }
                    } while (blobContinuationToken != null); // Loop while the continuation token is not null. 

                }
                successDownLoad = true;
            }
            catch (Exception ex)
            {
                errorMessage = ex.Message;
                _logger.LogError(errorMessage);
            }

            return successDownLoad;
        }

        /// <summary>
        /// Azure method for Upload Stream to Blob's Storage
        /// </summary>
        /// <param name="blobEntry">Record that includes Azure Service connection string, Account Storage, Blob Container  </param>
        /// <param name="blobPathAndName"> that represents virtal Block Blob path and file name</param>
        /// <param name="RetryCount ">Number of Retries in case of error</param> 
        /// <returns>true in case of success</returns>
        public async Task<string> UploadJsonText(BlobStorageEntry blobEntry, string blobPathAndName, string json, int RetryCount = 5)
        {
            string errorMessage = String.Empty;
            string containerName = String.Empty;

            CloudBlobContainer cloudBlobContainer = null;
            CloudBlockBlob cloudBlockBlob = null;
            
            int errorCreatingBlobContainer = 0;
            string UriBlobName = String.Empty;
            bool successUpload = false;
            bool bCreateContainer = false;

            //AppAuthentication library manages authentication automatically
            AzureServiceTokenProvider azureServiceTokenProvider = null;

            UriBlobName = String.Format("https://{0}.{1}/{2}/{3}", blobEntry.StorageAccountName, "blob.core.windows.net", blobEntry.ContainerName, blobPathAndName);

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
            string accessToken = await azureServiceTokenProvider.GetAccessTokenAsync("https://storage.azure.com/");

            var tokenCredential = new TokenCredential(accessToken);
            StorageCredentials storageCredentials = new StorageCredentials(tokenCredential);

            cloudBlockBlob = new CloudBlockBlob(new Uri(UriBlobName), storageCredentials);
            cloudBlobContainer = cloudBlockBlob.Container;

            while (errorCreatingBlobContainer < RetryCount)
            {
                try
                {
                    // Work Around for Possible Error 
                    // See https://github.com/Azure/azure-sdk-for-net/issues/109
                    bool bContainerExists = await cloudBlobContainer.ExistsAsync();
                    if (!bContainerExists)
                    {
                        bCreateContainer = await cloudBlobContainer.CreateIfNotExistsAsync();
                    }

                    if (bCreateContainer)
                    {
                        _logger.LogInformation("Create Container '{0}'", cloudBlobContainer.Name);
                    }
                    else
                    {
                        _logger.LogInformation("Extract container '{0}'", cloudBlobContainer.Name);
                    }

                    await cloudBlockBlob.UploadTextAsync(json);
                    _logger.LogInformation("Upload text '{0}' to Container '{1}' '{2}'", json, cloudBlobContainer.Name, blobPathAndName);
                    successUpload = true;
                    break; // Exit From Loop
                }
                catch (StorageException storageex)
                {
                    errorCreatingBlobContainer++; // Retry 
                    errorMessage = storageex.Message + String.Format("   ,Retry Number {0}", errorCreatingBlobContainer);
                    _logger.LogError(errorMessage);
                    await Task.Delay(DelayRetry);
                }
                catch (Exception ex)
                {
                    errorMessage = ex.Message;
                    _logger.LogError(errorMessage);
                    break;
                }
            }
            if (successUpload)
            {
                return blobPathAndName;
            }
            else
            {
                return string.Empty;
            }
        }
    }
}
