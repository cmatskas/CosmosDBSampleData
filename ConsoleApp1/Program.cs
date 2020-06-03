using System;
using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;
using Bogus;
using Microsoft.Azure.Cosmos;
using Microsoft.Extensions.Configuration;
using Newtonsoft.Json;

namespace ConsoleApp1
{
    class Program
    {
        private static string connectionString = "cosmosDBConnection";
        private static string databaseName = "VolcanoList";
        private static string containerName = "Volcano";
        private static string fileName = "sampleData.json";

        static async Task Main(string[] args)
        {
            var config = BootstrapConfiguration();

            Console.WriteLine("Initializing Cosmos DB connection");
            var cosmosClient = new CosmosClient(config[connectionString]);
            var db = await cosmosClient.CreateDatabaseIfNotExistsAsync(databaseName);
            var container = await db.Database.CreateContainerIfNotExistsAsync(containerName, "/id");

            Console.WriteLine("Loading data from json file");
            var data = await GetJsonDataAsync();

            Console.WriteLine("Enriching volcano data with measurements");
            PopulateMeasurementData(ref data);

            Console.WriteLine("Bulk uploading data to Cosmos DB");
            await BulkUploadDataToCosmosDB(data, container.Container);
       
            Console.WriteLine("Execution completed");
        }

        private static async Task BulkUploadDataToCosmosDB(List<Volcano> data, Container container)
        {
            if (data == null || data.Count == 0)
            {
                return;
            }
            
            List<Task> tasks = new List<Task>(data.Count);
            foreach (var volcano in data)
            {
                tasks.Add(container.CreateItemAsync(volcano));
            }
            await Task.WhenAll(tasks);
        }

        private static async Task<List<Volcano>> GetJsonDataAsync()
        {
            var data = await File.ReadAllTextAsync(fileName);
            return JsonConvert.DeserializeObject<List<Volcano>>(data);
        }

        private static void PopulateMeasurementData(ref List<Volcano> volcanoes)
        {
            foreach(var volcano in volcanoes)
            {
                var faker = new Faker<Measurements>()
                    .StrictMode(false)
                    .RuleFor(m => m.CO2, f => f.Random.Float(0.0f, 10.0f).ToString())
                    .RuleFor(m => m.H2S, f => f.Random.Int(100, 1000).ToString())
                    .RuleFor(m => m.HCL, f => f.Random.Float(0.1f, 5f).ToString())
                    .RuleFor(m => m.HF, f => f.Random.Int(1, 100).ToString())
                    .RuleFor(m => m.NaOH, f => f.Random.Float(1.0f, 9.0f).ToString())
                    .RuleFor(m => m.SClratio, f => f.Random.Float(0.1f, 10f).ToString())
                    .RuleFor(m => m.SO2, f => f.Random.Int(1000, 50000).ToString());

                volcano.Measurements = faker.Generate();
            }
        }

        private static IConfiguration BootstrapConfiguration()
        {
            string env = Environment.GetEnvironmentVariable("ASPNETCORE_ENVIRONMENT");

            if (string.IsNullOrWhiteSpace(env))
            {
                env = "Development";
            }

            var builder = new ConfigurationBuilder();

            if (env == "Development")
            {
                builder.AddUserSecrets<Program>();
            }

            return builder.Build();
        }
    }

}
