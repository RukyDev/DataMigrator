using System;
using System.Data;
using System.Data.SqlClient;

namespace DataMigrator
{
    public class DataMigrationService
    {
        private readonly string sourceConnectionString;
        private readonly string destinationConnectionString;

        public DataMigrationService(string sourceConnectionString, string destinationConnectionString)
        {
            this.sourceConnectionString = sourceConnectionString;
            this.destinationConnectionString = destinationConnectionString;
        }

        public async Task MigrateDataAsync(List<string> tableNames)
        {
            var tasks = new List<Task>();

            foreach (var tableName in tableNames)
            {
                tasks.Add(Task.Run(() => MigrateTableAsync(tableName)));
            }

            await Task.WhenAll(tasks);
        }

        private async Task MigrateTableAsync(string tableName)
        {
            Console.WriteLine("Starting Extration Process");
            var dataTable = await ExtractDataAsync(tableName);
            await CreateTableIfNotExistsAsync(tableName, dataTable);
            await LoadDataAsync(tableName, dataTable);
        }

        private async Task<DataTable> ExtractDataAsync(string tableName)
        {
            var dataTable = new DataTable();

            using (var sourceConnection = new SqlConnection(sourceConnectionString))
            using (var command = new SqlCommand($"SELECT * FROM {tableName}", sourceConnection))
            using (var adapter = new SqlDataAdapter(command))
            {
                await sourceConnection.OpenAsync();
                adapter.Fill(dataTable);
            }
            Console.WriteLine("Extraction Completed");
            return dataTable;
        }

        private async Task CreateTableIfNotExistsAsync(string tableName, DataTable dataTable)
        {
            using (var destinationConnection = new SqlConnection(destinationConnectionString))
            {
                await destinationConnection.OpenAsync();

                var columnDefinitions = string.Join(", ", dataTable.Columns.Cast<DataColumn>().Select(column => $"{column.ColumnName} {GetSqlDataType(column.DataType)}"));
                var createTableQuery = $"IF NOT EXISTS (SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = '{tableName}') CREATE TABLE {tableName} ({columnDefinitions})";

                using (var command = new SqlCommand(createTableQuery, destinationConnection))
                {
                    await command.ExecuteNonQueryAsync();
                }
            }
        }

        private async Task LoadDataAsync(string tableName, DataTable dataTable)
        {
            Console.WriteLine("Loading Data to Destination Table");
            using (var destinationConnection = new SqlConnection(destinationConnectionString))
            {
                await destinationConnection.OpenAsync();

                foreach (DataRow row in dataTable.Rows)
                {
                    var columns = string.Join(", ", dataTable.Columns.Cast<DataColumn>().Select(column => column.ColumnName));
                    var parameters = string.Join(", ", dataTable.Columns.Cast<DataColumn>().Select(column => $"@{column.ColumnName}"));
                    var insertQuery = $"INSERT INTO {tableName} ({columns}) VALUES ({parameters})";

                    using (var command = new SqlCommand(insertQuery, destinationConnection))
                    {
                        foreach (DataColumn column in dataTable.Columns)
                        {
                            command.Parameters.AddWithValue($"@{column.ColumnName}", row[column]);
                        }

                        await command.ExecuteNonQueryAsync();
                        Console.WriteLine("Load Completed");
                    }
                }
            }
        }

        private string GetSqlDataType(Type type)
        {
            switch (Type.GetTypeCode(type))
            {
                case TypeCode.Int32:
                    return "INT";
                case TypeCode.String:
                    return "NVARCHAR(MAX)";
                case TypeCode.DateTime:
                    return "DATETIME";
                case TypeCode.Boolean:
                    return "BIT";
                case TypeCode.Decimal:
                    return "DECIMAL(18, 2)";
                case TypeCode.Double:
                    return "FLOAT";
                case TypeCode.Single:
                    return "REAL";
                case TypeCode.Byte:
                    return "TINYINT";
                case TypeCode.Int16:
                    return "SMALLINT";
                case TypeCode.Int64:
                    return "BIGINT";
                case TypeCode.Char:
                    return "NCHAR(1)";
                default:
                    return "NVARCHAR(MAX)";
            }
        }

    }
}

