using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Caching.Memory;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using System.Text.Json.Serialization;
using AzureWebApp.Libs;
using System.Text.Json;

namespace AzureWebApp.Controllers
{
    [Route("[controller]")]
    [ApiController]
    public class DatabaseController : ControllerBase
    {
        private const int defaultRecordCount = 100;
        private IConfiguration Configuration = null;
        private ILogger Logger = null;
        private IMemoryCache MemoryCache = null;
        private bool EnableCache = false;

        public DatabaseController(IConfiguration configuration, ILogger<DatabaseController> logger, IMemoryCache memoryCache)
        {
            Configuration = configuration;
            Logger = logger;
            MemoryCache = memoryCache;
            if(!Boolean.TryParse(Configuration["EnableCache"]?.ToString(), out EnableCache))
            {
                EnableCache = false;
            }
        }
        
        // GET database/async
        [HttpGet("async")]
        public async Task<IActionResult> GetAsync([FromQuery]string name, [FromQuery]int recordCount = defaultRecordCount, [FromQuery]int minnumthread = 300)
        {
            if (!SetMinThreads(minnumthread, minnumthread))
            {
                throw new Exception("Failed to set min number of thread");
            }

            string returnString = string.Empty;
            int? statusCode = Microsoft.AspNetCore.Http.StatusCodes.Status200OK;

            Stopwatch stopWatch = new Stopwatch();
            stopWatch.Start();

            try
            {
                string cachedString = string.Empty;

                if (EnableCache && MemoryCache.TryGetValue(name, out cachedString))
                {
                    returnString = cachedString;
                }
                else
                {
                    using (QueryExecutor queryExecutor = new QueryExecutor(Configuration["Data:DefaultConnection:ConnectionString"], Logger))
                    {
                        var returnList = await queryExecutor.QueryAsync<object>(
                            GetQuery(recordCount),
                            r =>
                            {
                                return new
                                {
                                    InternalValueSetEntryID = r.GetInt64(0),
                                    InternalValueSetID = r.GetInt64(1),
                                    ExtJson = r.GetString(2),
                                    ExtraColumn = r.GetString(3)
                                };
                            },
                            new { Name = name }
                        );
                        
                        returnString = JsonSerializer.Serialize(returnList);
                        if (EnableCache)
                        {
                            MemoryCache.Set<string>(name, returnString);
                        }                        
                    }
                }
                stopWatch.Stop();
                Logger.LogInformation("GetAsync call with {0} took {1} ms", name, stopWatch.ElapsedMilliseconds);
            }
            catch (Exception ex)
            {
                Logger.LogError("Excption on GetAsync call with {0} : {1}", name, ex.Message);
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

        [HttpGet("threadinfo")]
        public IActionResult Get()
        {
            int availableWorker, availableIO;
            int maxWorker, maxIO;
            int minWorker, minIOC;
            
            ThreadPool.GetAvailableThreads(out availableWorker, out availableIO);
            ThreadPool.GetMaxThreads(out maxWorker, out maxIO);
            ThreadPool.GetMinThreads(out minWorker, out minIOC);

            string returnString = JsonSerializer.Serialize(new 
            {
                AvailableWorkerThreads = availableWorker,
                MaxWorkerThreads = maxWorker,
                MaxIOThreads = maxIO,
                MinWorkerThreads = minWorker,
                MinIOC = minIOC,
                OccupiedThreads = maxWorker - availableWorker
            });

            return new ContentResult()
            {
                Content = returnString,
                StatusCode = Microsoft.AspNetCore.Http.StatusCodes.Status200OK,
                ContentType = "application/fhir+json;charset=utf-8"
            };
        }

        [HttpPost("minnumthread")]
        public void PostMinNumThread([FromBody] int value)
        {
            if (!SetMinThreads(value, value))
            {
                throw new Exception("Failed to set min number of thread");
            }
        }

        private bool SetMinThreads(int valueCPU, int valueIO)
        {
            //int minWorker, minIOC;
            //ThreadPool.GetMinThreads(out minWorker, out minIOC);

            return ThreadPool.SetMinThreads(valueCPU, valueIO);
        }

        private IQuery GetQuery(int count)
        {
            IQuery query = new SelectQuery()
            {
                select = new ArrayExpression<string>()
                {
                    arrayValue = new string[] { "InternalValueSetEntryID", "InternalValueSetID", "ExtJson", "ExtraColumn" }
                },
                orderBy = new OrderByExpression()
                {
                    column = "InternalValueSetEntryID"
                },
                skip = 0,
                take = count,
                from = "[fhir].[valueset_view]",
                where = new LogicalExpression()
                {
                    expression = new EqualExpression<string>()
                    {
                        useParentheses = false,
                        useQuotesOnValue = false,
                        key = new KeyValuePair<string, string>("Name", "@Name"),
                    }.ToSQL()
                }
            };

            return query;
        }        
    }
}
