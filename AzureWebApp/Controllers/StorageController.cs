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
using AzureWebApp.Model;

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

        // GET storage/async
        [HttpGet("async")]
        public async Task<IActionResult> GetAsync([FromQuery]string name, [FromQuery]int minnumthread = 300)
        {
            if (!SetMinThreads(minnumthread, minnumthread))
            {
                throw new Exception("Failed to set min number of thread");
            }

            string returnString = string.Empty;
            int? statusCode = Microsoft.AspNetCore.Http.StatusCodes.Status200OK;

            Random random = new Random();
            string fileName = string.Format("{0}-{1}.DCM", name, random.Next(119, 321));

            Stopwatch stopWatch = new Stopwatch();
            stopWatch.Start();
            try
            {
                MemoryStream msStream = new MemoryStream();

                if (await GetBlob(fileName, msStream))
                {
                    msStream.Position = 0;
                    // Test reading from the stream //////////////////////////////////
                    //StreamReader reader = new StreamReader(msStream);
                    //var returnBlob = await reader.ReadToEndAsync();
                    //////////////////////////////////////////////////////////////////

                    // Test writing the stream to file ///////////////////////////////
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
                    //////////////////////////////////////////////////////////////////
                    
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

        // GET storage/async
        [HttpGet("dicomjson")]
        public async Task<IActionResult> GetDicomJson([FromQuery]string name, [FromQuery]int minnumthread = 300)
        {
            if (!SetMinThreads(minnumthread, minnumthread))
            {
                throw new Exception("Failed to set min number of thread");
            }

            string returnString = string.Empty;
            int? statusCode = Microsoft.AspNetCore.Http.StatusCodes.Status200OK;

            Random random = new Random();
            string fileName = string.Format("{0}{1}", name, random.Next(1, 50));

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
                    string sopInstanceUID = string.Empty;

                    using (var JsonDoc = JsonDocument.Parse(jsonBlob))
                    {
                        var root = JsonDoc.RootElement;
                        if (!root.Equals(default(JsonElement)))
                        {
                            sopInstanceUID = root.GetProperty("00080018").GetProperty("Value")[0].ToString();
                        }
                    }

                    returnString = string.Format("FileName = {0}, SOPInstanceUID = {1}", fileName, sopInstanceUID);
                }
                else
                {
                    Logger.LogError("GetDicomJson - Blob {0} wasn't downloaded successfully", fileName);
                }

                stopWatch.Stop();
                Logger.LogInformation("GetDicomJson call with {0} took {1} ms", fileName, stopWatch.ElapsedMilliseconds);
            }
            catch (Exception ex)
            {
                Logger.LogError("Excption on GetDicomJson call with {0} : {1}", fileName, ex.Message);
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
            blobEntry.UseAzureStorage = true;
            blobEntry.StorageAccountName = Configuration["StorageAccountName"]?.ToString();
            
            AzureStorage azureStorage = new AzureStorage(Logger);
            // Call Azure methods for DownLoad Stream 
            return await azureStorage.DownLoadStreamFromBlobStorage(blobEntry, name);
        }

        private bool SetMinThreads(int valueCPU, int valueIO)
        {
            return ThreadPool.SetMinThreads(valueCPU, valueIO);
        }
    }
}