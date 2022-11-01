using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using NLog.Extensions.Logging;
using Zeebe.Client;
using Zeebe.Client.Api.Responses;
using Zeebe.Client.Api.Worker;

namespace Client.Examples
{
    internal class Program
    {
        private static readonly string ZeebeUrl = "zeebe:26500";
        private static readonly string JobType = "weather-service";
        private static readonly string WorkerName = Environment.MachineName;
        private static readonly long WorkCount = 2L;

        private static long processDefinitionKey = 1337;

        public static async Task Main(string[] args)
        {
            // zeebe client
            var client = ZeebeClient.Builder()
                .UseLoggerFactory(new NLogLoggerFactory())
                .UseGatewayAddress(ZeebeUrl)
                .UsePlainText()
                .Build();

            var topology = await client.TopologyRequest()
                .Send();

            Console.WriteLine(topology);

            await client.NewPublishMessageCommand()
                .MessageName("csharp")
                .CorrelationKey("wow")
                .Variables("{\"realValue\":2}")
                .Send();

            // open job worker
            using (var signal = new EventWaitHandle(false, EventResetMode.AutoReset))
            {
                client.NewWorker()
                      .JobType(JobType)
                      .Handler(HandleJob)
                      .MaxJobsActive(5)
                      .Name(WorkerName)
                      .AutoCompletion()
                      .PollInterval(TimeSpan.FromSeconds(1))
                      .Timeout(TimeSpan.FromSeconds(10))
                      .FetchVariables(new[] { "weather", "status" })
                      .Open();

                // blocks main thread, so that worker can run
                signal.WaitOne();
            }
        }

        private static void HandleJob(IJobClient jobClient, IJob job)
        {
            // business logic
            var jobKey = job.Key;
            Console.WriteLine("Handling job: " + job);
            var variables = System.Text.Json.JsonSerializer.Deserialize<Root>(job.Variables);
            var rainyDaysIndexes = Enumerable.Range(0, variables.weather.hourly.rain.Count())
             .Where(i => variables.weather.hourly.rain[i] != 0)
             .ToList();

            if (rainyDaysIndexes.Count > 0)
            {
                var rainyDays = new List<Rainy>();
                foreach (var index in rainyDaysIndexes)
                {
                    rainyDays.Add(new Rainy(variables.weather.hourly.time[index], variables.weather.hourly.rain[index]));
                }
                Console.WriteLine();
                Console.WriteLine("Upcoming days and times with rain");
                foreach (var dayWithRain in rainyDays)
                {
                    Console.WriteLine($"{dayWithRain.time.ToString("yyyy-MM-dd HH:mm:ss")} - {dayWithRain.amount}mm");
                }
            }

        }
    }
}

public class Rainy
{
    public Rainy(DateTime time, double amount)
    {
        this.time = time;
        this.amount = amount;
    }

    public DateTime time { get; set; }
    public double amount { get; set; }
}

public class Hourly
{
    public List<DateTime> time { get; set; }
    public List<double> rain { get; set; }
}

public class HourlyUnits
{
    public string time { get; set; }
    public string rain { get; set; }
}

public class Root
{
    public int status { get; set; }
    public Weather weather { get; set; }
}

public class Weather
{
    public Hourly hourly { get; set; }
    public double latitude { get; set; }
    public double generationtime_ms { get; set; }
    public double longitude { get; set; }
    public double elevation { get; set; }
    public int utc_offset_seconds { get; set; }
    public HourlyUnits hourly_units { get; set; }
    public string timezone_abbreviation { get; set; }
    public string timezone { get; set; }
}
