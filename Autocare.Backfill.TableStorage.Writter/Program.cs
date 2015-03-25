using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Configuration;
using System.Diagnostics;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.WindowsAzure;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Table;

namespace Autocare.Backfill.TableStorage.Writter
{
    class Program
    {
        static void Main(string[] args)
        {
            string path = ConfigurationManager.AppSettings.Get("path");
            string tablename = ConfigurationManager.AppSettings.Get("table");

            CloudStorageAccount storageAccount = CloudStorageAccount.Parse(CloudConfigurationManager.GetSetting("StorageConnectionString"));
            CloudTableClient tableClient = storageAccount.CreateCloudTableClient();
            CloudTable table = tableClient.GetTableReference(tablename);
            table.CreateIfNotExists();
            
            StreamReader file = new StreamReader(path);
            
            string line;
            int counter = 0;
            Console.WriteLine("Reading lines. (update every 1k.)");

            var annonList = new List<Model>(1000000);
            var sw = new Stopwatch();
            sw.Start();
            while ((line = file.ReadLine()) != null)
            {
                counter++;
                var values = line.Split(',');
                annonList.Add(
                    new Model
                    {
                        userGuid = values[0],
                        charityId = values[1],
                        careReasonType = values[2],
                        careReasonTypeId = values[3],
                    });


                if ((counter % 1000) == 0)
                {
                    ParallelInserts(table, annonList);
                    annonList = new List<Model>(1000000);
                    Console.WriteLine("Completed {1} lines in {0} minutes", sw.Elapsed.TotalMinutes, counter);
                }
            }

            // write leftovers to the storage too
            InsertBatchIntoTableStorage(table, annonList);

            file.Close();
            sw.Stop();
            Console.WriteLine("Moved {0} lines to Azure in {1} minutes", counter, sw.Elapsed.TotalMinutes);
            Console.ReadLine();

        }

        private static void ParallelInserts(CloudTable table, List<Model> cares)
        {
            var partitioner = Partitioner.Create(0, cares.Count);
            var options = new ParallelOptions {MaxDegreeOfParallelism = 8};

            Parallel.ForEach(partitioner, options, range =>
            {
                for (int i = range.Item1; i < range.Item2; i++)
                {

                    if (cares[i].charityId.Equals("2050"))
                    {
                        Console.WriteLine("Excluded demo charity.");
                        continue;
                    }

                    var entity = new DynamicTableEntity
                    {
                        PartitionKey = cares[i].userGuid,
                        RowKey = cares[i].charityId,
                        Timestamp = DateTime.UtcNow,
                        ETag = "*",
                    };

                    entity["CareReasonType"] = new EntityProperty(cares[i].careReasonType);
                    entity["CareReasonId"] = new EntityProperty(cares[i].careReasonTypeId);

                    var insertOperation = TableOperation.Insert(entity);

                    try
                    {
                        table.Execute(insertOperation);
                    }
                    catch (StorageException ex)
                    {
                        Console.WriteLine("Failed to insert entity to table storage {0} {1} {2}", ex.ToString(), cares[i].userGuid, cares[i].charityId);
                    }
                }
            });

        }

        private static void InsertBatchIntoTableStorage(CloudTable table, List<Model> cares)
        {
            foreach (var care in cares)
            {
                if (care.charityId.Equals("2050"))
                {
                    Console.WriteLine("Excluded demo charity.");
                    continue;
                }

                var entity = new DynamicTableEntity
                {
                    PartitionKey = care.userGuid,
                    RowKey = care.charityId,
                    Timestamp = DateTime.UtcNow,
                    ETag = "*",
                };

                entity["CareReasonType"] = new EntityProperty(care.careReasonType);
                entity["CareReasonId"] = new EntityProperty(care.careReasonTypeId);

                var insertOperation = TableOperation.Insert(entity);

                try
                {

                    table.Execute(insertOperation);
                }
                catch (StorageException ex)
                {
                    Console.WriteLine("Failed to insert entity to table storage", ex.ToString(), care.userGuid, care.charityId);
                }
            }


        }
    }

    public class Model
    {
        public string userGuid { get; set; }
        public string charityId { get; set; }
        public string careReasonType { get; set; }
        public string careReasonTypeId { get; set; }

    }


}
