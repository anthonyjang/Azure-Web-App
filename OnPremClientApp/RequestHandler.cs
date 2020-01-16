using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Net.Http;
using System.Threading.Tasks;
using Serilog;

namespace OnPremClientApp
{
    public class RequestHandler
    {
        private const int defaultInstances = 10;
        private const int defaultIterations = 10;
        private int Instances = defaultInstances;
        private int Iterations = defaultIterations;
        private Uri EndpointUri;

        public RequestHandler(string InstancesStr, string IterationsStr, Uri EndpointUri)
        {
            if (!int.TryParse(InstancesStr, out Instances))
            {
                Instances = defaultInstances;
            }
            if (!int.TryParse(IterationsStr, out Iterations))
            {
                Iterations = defaultIterations;
            }

            this.EndpointUri = EndpointUri;
        }

        public async Task<bool> Request()
        {
            bool requestSuccess = false;
            HttpClientHandler httpClientHandler = new HttpClientHandler()
            {
                ServerCertificateCustomValidationCallback = (m, c, ch, e) => { return true; }
            };

            HttpClient httpClient = new HttpClient(httpClientHandler);
            try
            {
                Stopwatch stopwatch = new Stopwatch();
                stopwatch.Start();
                foreach (var iteration in Enumerable.Range(0, Iterations))
                {
                    IList<Task> allTasks = new List<Task>();

                    foreach (var instance in Enumerable.Range(0, Instances))
                    {
                        allTasks.Add(PerformRequest(httpClient));
                    }

                    await Task.WhenAll(allTasks);
                }
                requestSuccess = true;
                Log.Information("Request - Success and took {0} ms for overall {1} requests", stopwatch.ElapsedMilliseconds, Iterations*Instances);
            }
            catch (Exception ex)
            {
                Console.WriteLine("Request - except {0}", ex.Message);
                Log.Error("Request - except {0}", ex.Message);
                requestSuccess = false;
            }
            return requestSuccess;
        }

        private async Task<string> PerformRequest(HttpClient httpClient)
        {
            string responseJson = string.Empty;

            Stopwatch stopwatch = new Stopwatch();

            stopwatch.Start();
            var getResponse = await httpClient.GetAsync(EndpointUri);
            if (getResponse.IsSuccessStatusCode)
            {
                responseJson = await getResponse.Content.ReadAsStringAsync();
                //Console.WriteLine("PerformRequest - Success {0}", responseJson);
                Log.Information("PerformRequest - Success and took {0} ms", stopwatch.ElapsedMilliseconds);
            }
            else
            {
                //Console.WriteLine("PerformRequest - failed {0}", getResponse.StatusCode);
                Log.Information("PerformRequest - failed {0}", getResponse.StatusCode);
            }

            return responseJson;
        }
    }
}
