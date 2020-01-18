using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using AzureWebApp.Libs;

namespace AzureWebApp.Controllers
{
    [Route("[controller]")]
    [ApiController]
    public class StorageController : ControllerBase
    {
        private IConfiguration Configuration = null;
        private ILogger Logger = null;

        public StorageController(IConfiguration configuration, ILogger<StorageController> logger)
        {
            Configuration = configuration;
            Logger = logger;
        }

        /// <summary>
        /// GET storage/async
        /// Get random files on storage
        /// </summary>
        /// <param name="name">name prefix of the blob</param>
        /// <returns></returns> 
        [HttpGet("async")]
        public async Task<IActionResult> GetAsync([FromQuery]string name)
        {
            string returnString = string.Empty;
            int? statusCode = Microsoft.AspNetCore.Http.StatusCodes.Status200OK;

            Random random = new Random();
            string fileName = string.Format("{0}-{1}.data", name, random.Next(1, 50));

            Stopwatch stopWatch = new Stopwatch();
            stopWatch.Start();
            try
            {
                MemoryStream msStream = new MemoryStream();

                if (await GetBlob(fileName, msStream))
                {
                    msStream.Position = 0;

                    // Enable the code below to read from the stream
                    //StreamReader reader = new StreamReader(msStream);
                    //var returnBlob = await reader.ReadToEndAsync();
                    
                    // Enable the code below to write the stream to file
                    //var tempPath = System.IO.Path.GetTempPath();
                    //var tempFile = string.Format(@"{0}{1}", tempPath, fileName);
                    //
                    //FileStream fileStream = new FileStream(tempFile, FileMode.OpenOrCreate);
                    //
                    //var buffer = new byte[msStream.Length];
                    //
                    //if (msStream.Read(buffer, 0, buffer.Length) > 0)
                    //{
                    //    await fileStream.WriteAsync(buffer, 0, buffer.Length);
                    //
                    //    returnString = string.Format("Downloading {0} success on temp file {1}", fileName, tempFile);
                    //}
                    //else
                    //{
                    //    Logger.LogError("GetAsync - Reading {0} failed", fileName);
                    //}
                    
                    returnString = string.Format("Downloading {0} success", fileName);
                }
                else
                {
                    Logger.LogError("GetAsync - Blob {0} wasn't downloaded successfully", fileName);
                }

                stopWatch.Stop();
                Logger.LogInformation("GetAsync call with {0} took {1} ms", fileName, stopWatch.ElapsedMilliseconds);
            }
            catch (Exception ex)
            {
                Logger.LogError("Excption on GetAsync call with {0} : {1}", fileName, ex.Message);
                statusCode = Microsoft.AspNetCore.Http.StatusCodes.Status500InternalServerError;
                returnString = ex.Message;
            }

            return new ContentResult()
            {
                Content = returnString,
                StatusCode = statusCode,
                ContentType = "application/fhir+json;charset=utf-8"
            };
        }

        /// <summary>
        /// GET storage/jsondata
        /// Access random Json data started with string defined on name parameter on blob storage
        /// </summary>
        /// <param name="name">prefix of the json file name</param>
        /// <returns></returns> 
        [HttpGet("jsondata")]
        public async Task<IActionResult> GetJsonData([FromQuery]string name)
        {
            string returnString = string.Empty;
            int? statusCode = Microsoft.AspNetCore.Http.StatusCodes.Status200OK;

            Random random = new Random();
            string fileName = string.Format("{0}-{1}.json", name, random.Next(1, 50));

            Stopwatch stopWatch = new Stopwatch();
            stopWatch.Start();
            try
            {
                MemoryStream msStream = new MemoryStream();

                if (await GetBlob(fileName, msStream))
                {
                    msStream.Position = 0;
                    StreamReader reader = new StreamReader(msStream);
                    var jsonBlob = await reader.ReadToEndAsync();
                    string valueOnKey = string.Empty;

                    using (var JsonDoc = JsonDocument.Parse(jsonBlob))
                    {
                        var root = JsonDoc.RootElement;
                        if (!root.Equals(default(JsonElement)))
                        {
                            valueOnKey = root.GetProperty("Key").GetProperty("Value")[0].ToString();
                        }
                    }

                    returnString = string.Format("FileName = {0}, Value = {1}", fileName, valueOnKey);
                }
                else
                {
                    Logger.LogError("GetDicomJson - Blob {0} wasn't downloaded successfully", fileName);
                }

                stopWatch.Stop();
                Logger.LogInformation("GetJsonData call with {0} took {1} ms", fileName, stopWatch.ElapsedMilliseconds);
            }
            catch (Exception ex)
            {
                Logger.LogError("Excption on GetJsonData call with {0} : {1}", fileName, ex.Message);
                statusCode = Microsoft.AspNetCore.Http.StatusCodes.Status500InternalServerError;
                returnString = ex.Message;
            }

            return new ContentResult()
            {
                Content = returnString,
                StatusCode = statusCode,
                ContentType = "application/fhir+json;charset=utf-8"
            };
        }

        private async Task<bool> GetBlob(string name, MemoryStream stream)
        {
            BlobStorageEntry blobEntry = new BlobStorageEntry();
            blobEntry.StreamObject = stream;
            blobEntry.StorageProviderConnectionString = Configuration["azureProviderConnectionString"]?.ToString();
            blobEntry.ContainerName = Configuration["ContainerName"]?.ToString();
            blobEntry.StorageAccountName = Configuration["StorageAccountName"]?.ToString();
            
            StorageAccessLayer azureStorage = new StorageAccessLayer(Logger);
            // Call Azure methods for DownLoad Stream 
            return await azureStorage.DownLoadStreamFromBlobStorage(blobEntry, name);
        }
    }
}