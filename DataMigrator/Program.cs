using DataMigrator;
using Microsoft.Extensions.Configuration;

namespace DataMigrationApp
{
    class Program
    {
        static async Task Main(string[] args)
        {
            Console.WriteLine("Program is starting");
            //var config = new ConfigurationBuilder()
            //    .SetBasePath(AppContext.BaseDirectory)
            //    .AddJsonFile("appSettings.json", optional: false, reloadOnChange: true)
            //    .Build();
            //Console.WriteLine("Reading Connection String");
            //string sourceConnectionString = config.GetConnectionString("SourceDB");
            //string destinationConnectionString = config.GetConnectionString("DestinationDB");
            string sourceConnectionString = "";
            string destinationConnectionString = "";
            Console.WriteLine("Connection String Read Successfully");
            var tableNames = new List<string> { "cust", "DimAccount", "DimCurrency" }; // Add your table names here

            var migrationService = new DataMigrationService(sourceConnectionString, destinationConnectionString);
            await migrationService.MigrateDataAsync(tableNames);
        }
    }
}
