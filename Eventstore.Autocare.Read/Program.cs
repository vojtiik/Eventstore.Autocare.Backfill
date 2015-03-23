using System;
using System.Collections.Generic;
using System.Configuration;
using System.IO;
using System.Linq;
using System.Net;
using System.Text;
using EventStore.ClientAPI;
using EventStore.ClientAPI.SystemData;
using GG.Care.WriteConcern.Messages.V3;
using Newtonsoft.Json;

namespace Eventstore.Autocare.Read
{
    public class Program
    {
        private static void Main(string[] args)
        {
            string esIP = ConfigurationManager.AppSettings.Get("eventstoreIP"); // 1113
            var esPort = int.Parse(ConfigurationManager.AppSettings.Get("eventStorePort")); //IPAddress.Loopback;
            var ip = IPAddress.Parse(esIP);
            var settings = ConnectionSettings.Create();
            var ep = new IPEndPoint(ip, esPort);

            settings
                .UseConsoleLogger()
                .SetDefaultUserCredentials(new UserCredentials(
                    ConfigurationManager.AppSettings.Get("eventstoreUsr"),
                    ConfigurationManager.AppSettings.Get("eventstorePass")));
            //   var connection = EventStoreConnection.Create(settings.Build(), new IPEndPoint(ip, esPort));


            string streamname = ConfigurationManager.AppSettings.Get("stream"); // "backfillauto6";
            string sourcePath = ConfigurationManager.AppSettings.Get("path"); // @"d:\autocare_backfill\";
            string sourceFileName = ConfigurationManager.AppSettings.Get("filename"); // "autocare-out.txt";
            string filePathAndName = sourcePath + sourceFileName;

            var start = 0;

            var resolvedEvents = new List<ResolvedEvent>(5000000);
            var resolvedEvents2 = new List<ResolvedEvent>(5000000);
            
            while (true)
            {

                StreamEventsSlice slice = ReadNextEventsFromEventstore(settings, ep, streamname, start);

                if (start > 3000000)
                {
                    resolvedEvents.AddRange(slice.Events);
                }
                else
                {
                    resolvedEvents2.AddRange(slice.Events);
                }
                start = slice.NextEventNumber;
                Console.ForegroundColor = ConsoleColor.Green;
                Console.WriteLine("Reading ({0})events from {2} stream. Position {1} ", slice.Events.Count(), start, streamname);
                Console.ResetColor();

                if (slice.IsEndOfStream)
                {
                    Console.WriteLine("EndOfTheStream reached.");
                    break;
                }
            }

            resolvedEvents.AddRange(resolvedEvents2);

            var result = TranslateEventsToStorageFormat(resolvedEvents);
            AppendToFile(filePathAndName, result);
            Console.WriteLine("Append to {0} completed.", filePathAndName);

            Console.ReadLine();

        }

        private static string plainCareStringName = "care";

        private static List<AzureTableStorageFormat> TranslateEventsToStorageFormat(List<ResolvedEvent> events)
        {
            var output = new Dictionary<string, AzureTableStorageFormat>(1000000);
            var v1v2counter = 0;
            Console.WriteLine("Deserializing events. ( . = 100k)");
            int progressCounter = 0;
            foreach (var ev in events)
            {
                progressCounter++;
                if ((progressCounter % 10000) == 0)
                {
                    Console.Write(".");
                }

                var stringobject = Encoding.UTF8.GetString(ev.Event.Data);

                switch (ev.Event.EventType)
                {
                    case "GG.Care.WriteConcern.Messages.V1.UserStartedCaring":
                    case "GG.Care.WriteConcern.Messages.V1.UserStoppedCaring":
                    case "GG.Care.WriteConcern.Messages.V2.UserStartedCaring":
                    case "GG.Care.WriteConcern.Messages.V2.UserStoppedCaring":
                        v1v2counter++;
                        break;

                    case "GG.Care.WriteConcern.Messages.V3.UserStartedCaring":
                        var careV3 = JsonConvert.DeserializeObject<UserStartedCaring>(stringobject);
                        if (careV3.EntityType != "charity")
                        {
                            continue;
                        }

                        // care is always more important, remove and re-add 
                        output.Remove(careV3.UserId + careV3.EntityId);
                        output.Add(careV3.UserId + careV3.EntityId, new AzureTableStorageFormat()
                        {
                            UserGuid = careV3.UserId.ToString(),
                            CharityId = careV3.EntityId,
                            ReasonId = careV3.UserId.ToString(),
                            ReasonType = plainCareStringName
                        });
                        break;

                    case "GG.Care.WriteConcern.Messages.V3.UserStoppedCaring":
                        var uncare = JsonConvert.DeserializeObject<UserStoppedCaring>(stringobject);
                        if (uncare.EntityType != "charity")
                        {
                            continue;
                        }

                        output.Remove(uncare.UserId + uncare.EntityId);
                        break;

                    case "GG.Care.WriteConcern.Messages.V3.UserAutoCared":
                        var autocareV3 = JsonConvert.DeserializeObject<UserAutoCared>(stringobject);
                        if (autocareV3.EntityType != "charity")
                        {
                            continue;
                        }

                        var userGuid = autocareV3.UserId.ToString();

                        if (output.ContainsKey(autocareV3.UserId + autocareV3.EntityId))
                        {
                            // if there is already a care dont overwrite
                            if (output[autocareV3.UserId + autocareV3.EntityId].ReasonType == plainCareStringName)
                            {
                                continue;
                            }

                            // remove and replace with fresher autocare
                            output.Remove(autocareV3.UserId + autocareV3.EntityId);
                        }

                        output.Add(autocareV3.UserId + autocareV3.EntityId, new AzureTableStorageFormat
                            {
                                UserGuid = userGuid,
                                CharityId = autocareV3.EntityId,
                                ReasonId = autocareV3.SourceEntityId,
                                ReasonType = autocareV3.SourceEntityType
                            });

                        break;
                    default:
                        Console.WriteLine("Unsuporrted message type " + ev.Event.EventType);
                        break;
                }
            }

            Console.WriteLine("V1V2 messages found: " + v1v2counter);

            return output.Values.ToList();
        }

        public static StreamEventsSlice ReadNextEventsFromEventstore(ConnectionSettingsBuilder settings, IPEndPoint ep,string streamName, int start)
        {
           var connection = EventStoreConnection.Create(settings.Build(), ep);
            connection.ConnectAsync().Wait();
            
            var result = connection.ReadStreamEventsForwardAsync(streamName, start, 200000, false).Result;
         
            connection.Close();
            return result;
        }

        public static void AppendToFile(string filePathAndName, List<AzureTableStorageFormat> events)
        {

            if (!File.Exists(filePathAndName))
            {
                using (StreamWriter sw = File.CreateText(filePathAndName))
                {
                }
            }

            // Create a file to write to. 
            using (StreamWriter sw = File.AppendText(filePathAndName))
            {
                foreach (var ev in events)
                {
                    sw.WriteLine("{0},{1},{2},{3}", ev.UserGuid, ev.CharityId, ev.ReasonType, ev.ReasonId);
                }
            }
        }
    }

    public class AzureTableStorageFormat
    {
        public string UserGuid { get; set; }
        public string CharityId { get; set; }
        public string ReasonType { get; set; }
        public string ReasonId { get; set; }
    }
}
