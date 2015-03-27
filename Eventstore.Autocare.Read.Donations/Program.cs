using System;
using System.Collections.Generic;
using System.Configuration;
using System.IO;
using System.Linq;
using System.Net;
using System.Text;
using System.Threading;
using EventStore.ClientAPI;
using EventStore.ClientAPI.SystemData;
using GG.Care.WriteConcern.Messages.V3;
using Newtonsoft.Json;

namespace Eventstore.Autocare.Read.Donations
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


            string streamname = ConfigurationManager.AppSettings.Get("stream");
            string sourcePath = ConfigurationManager.AppSettings.Get("path");
            string sourceFileName = ConfigurationManager.AppSettings.Get("filename");
            int start = int.Parse(ConfigurationManager.AppSettings.Get("start"));
            int end = int.Parse(ConfigurationManager.AppSettings.Get("end"));
            string filePathAndName = sourcePath + sourceFileName;

            var resolvedEvents = new List<ResolvedEvent>(5000000);
            var resolvedEvents2 = new List<ResolvedEvent>(5000000);

            while (true)
            {
                Console.ForegroundColor = ConsoleColor.Green;
                Console.WriteLine("Reading from {0} stream. Position {1}", streamname, start);
                Console.ResetColor();
                
                StreamEventsSlice slice = null;
                int retry = 0;
                while (retry < 10)
                {
                    try
                    {
                        slice = ReadNextEventsFromEventstore(settings, ep, streamname, start, end);
                        break;
                    }
                    catch (Exception e)
                    {
                        Console.ForegroundColor = ConsoleColor.Red;
                        Console.WriteLine("read slice failed,  retry in 1s , retry #: {0}", retry);
                        Console.ResetColor();
                        retry++;
                        Thread.Sleep(1000);
                    }
                }

                if (slice == null)
                {
                    throw new Exception("failed to read from ES");
                }


                // split them into two memory allocation 
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

                if (slice.IsEndOfStream || slice.NextEventNumber >= end)
                {
                    Console.WriteLine("EndOfTheStream reached.");
                    break;
                }
            }

            resolvedEvents2.AddRange(resolvedEvents);

            var result = Deserialize(resolvedEvents2);
            AppendToFile(filePathAndName, result);
            Console.WriteLine("Append to {0} completed.", filePathAndName);

            Console.ReadLine();

        }

        private static List<DonationEvent> Deserialize(List<ResolvedEvent> events)
        {
            var donationsToUncare = new Dictionary<string, DonationEvent>(1000000);
            var otherEventscounter = 0;
            var democharitycounter = 0;
            Console.WriteLine("Deserializing events. ( . = 10k)");
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
                    case "GG.Care.WriteConcern.Messages.V3.UserStartedCaring":
                        var careV3 = JsonConvert.DeserializeObject<UserStartedCaring>(stringobject);
                        if (careV3.EntityType != "charity")
                        {
                            continue;
                        }
                        if (careV3.EntityId.Equals("2050"))
                        {
                            democharitycounter++;
                            continue;
                        }

                        // care is always more important, remove and re-add 
                        donationsToUncare.Remove(careV3.UserId + careV3.EntityId);
                        break;


                    case "GG.Care.WriteConcern.Messages.V3.UserAutoCared":
                        var autocareV3 = JsonConvert.DeserializeObject<UserAutoCared>(stringobject);
                        if (autocareV3.EntityType != "charity")
                        {
                            continue;
                        }

                        if (autocareV3.EntityId.Equals("2050"))
                        {
                            democharitycounter++;
                            continue;
                        }

                        var userGuid = autocareV3.UserId.ToString();

                        if (donationsToUncare.ContainsKey(autocareV3.UserId + autocareV3.EntityId))
                        {
                            continue;
                        }

                        if (autocareV3.SourceEntityType == "Donation")
                        {
                            donationsToUncare.Add(autocareV3.UserId + autocareV3.EntityId, new DonationEvent
                            {
                                UserGuid = userGuid,
                                CharityId = autocareV3.EntityId,
                                DonationId = autocareV3.SourceEntityId,
                            });
                        }

                        break;
                    default:
                        otherEventscounter++;
                        break;
                }
            }

            Console.WriteLine("Demo charities found: " + democharitycounter);
            Console.WriteLine("Donation events read: " + donationsToUncare.Count());
            return donationsToUncare.Values.ToList();
        }

        private static StreamEventsSlice ReadNextEventsFromEventstore(ConnectionSettingsBuilder settings, IPEndPoint ep, string streamName, int start, int end)
        {
            var connection = EventStoreConnection.Create(settings.Build(), ep);
            connection.ConnectAsync().Wait();

            int readcount = 30000;

            if ((start + readcount) > end)
            {
                readcount = end - start;
            }

            var result = connection.ReadStreamEventsForwardAsync(streamName, start, readcount, false).Result;

            connection.Close();

            return result;
        }

        public static void AppendToFile(string filePathAndName, List<DonationEvent> events)
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
                    sw.WriteLine("{0},{1},{2}", ev.UserGuid, ev.CharityId, ev.DonationId);
                }
            }
        }
    }

    public class DonationEvent
    {
        public string UserGuid { get; set; }
        public string CharityId { get; set; }
        public string DonationId { get; set; }
    }
}
