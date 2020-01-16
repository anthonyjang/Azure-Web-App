using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.PlatformAbstractions;
using System;
using Serilog;

namespace OnPremClientApp
{
    class Program
    {
        public static IConfigurationRoot Configuration { get; set; }

        static void Main(string[] args)
        {
            var app_environment = PlatformServices.Default.Application;
            string applicationBasePath = app_environment.ApplicationBasePath;

            var builder = new ConfigurationBuilder().SetBasePath(applicationBasePath)
                .AddJsonFile("appsettings.json", optional: true, reloadOnChange: true);

            Configuration = builder.Build();

            // Initialisation of Log Services
            var cfg = new LoggerConfiguration();
            cfg.MinimumLevel.Information();
            cfg.WriteTo.Async(a => a.File("../logs/AzureWebAppClinetApp.log",
                outputTemplate: "{Timestamp:yyyy-MM-dd HH:mm:ss.fff} [{Level}] {Message}{NewLine:l}",
                fileSizeLimitBytes: 20*1024*1024, rollOnFileSizeLimit: true, rollingInterval: RollingInterval.Day));
            Log.Logger = cfg.CreateLogger();

            try
            {
                RequestHandler requester = new RequestHandler(Configuration["Instances"]?.ToString(), Configuration["Iterations"]?.ToString(), new Uri(Configuration["Endpoint"].ToString()));

                if (requester.Request().Result)
                {
                    LogInfo("RequestHandler - Success");
                }
                else
                {
                    LogInfo("RequestHandler - Failed");                    
                }
            }
            catch (Exception ex)
            {
                LogException("RequestHandler - exception {0}", ex.Message);
            }

            Log.CloseAndFlush();
        }

        private static void LogInfo(string msg)
        {
            Console.WriteLine(msg);
            Log.Information(msg);
        }

        private static void LogInfo(string msg, params object[] arg)
        {
            Console.WriteLine(msg, arg); 
            Log.Information(msg, arg);
        }

        private static void LogException(string msg, params object[] arg) => Console.WriteLine(msg, arg);
    }
}
