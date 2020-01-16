using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Hosting;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Serilog;

namespace AzureWebApp
{
    public class Program
    {
        public static void Main(string[] args)
        {            
            var cfg = new LoggerConfiguration();
            cfg.MinimumLevel.Information();
            cfg.WriteTo.Async(a => a.File("../logs/AzureWebApp.log", 
                outputTemplate: "{Timestamp:yyyy-MM-dd HH:mm:ss.fff} [{Level}:{RequestId}] {Message}{NewLine:l}", 
                fileSizeLimitBytes: 20971520, rollOnFileSizeLimit: true, rollingInterval: RollingInterval.Day));
            Log.Logger = cfg.CreateLogger();

            try
            {
                Log.Information("Starting up");
                CreateHostBuilder(args).Build().Run();
            }
            catch (Exception ex)
            {
                Log.Fatal(ex, "Application start-up failed");
            }
            finally
            {
                Log.CloseAndFlush();
            }
        }

        public static IHostBuilder CreateHostBuilder(string[] args) =>
            Host.CreateDefaultBuilder(args)
                .UseSerilog()
                .ConfigureWebHostDefaults(webBuilder =>
                {
                    webBuilder.UseStartup<Startup>();
                });
    }
}
