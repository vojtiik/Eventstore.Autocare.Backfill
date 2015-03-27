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

            settings.UseConsoleLogger().SetDefaultUserCredentials(new UserCredentials(ConfigurationManager.AppSettings.Get("eventstoreUsr"),ConfigurationManager.AppSettings.Get("eventstorePass")));

            string streamname = ConfigurationManager.AppSettings.Get("stream"); 
            string sourcePath = ConfigurationManager.AppSettings.Get("path"); 
            string sourceFileName = ConfigurationManager.AppSettings.Get("filename"); 
            string filePathAndName = sourcePath + sourceFileName;

            var start = int.Parse(ConfigurationManager.AppSettings.Get("start"));

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
                        slice = ReadNextEventsFromEventstore(settings, ep, streamname, start);
                        break;
                    }
                    catch (Exception e)
                    {
                        Console.ForegroundColor = ConsoleColor.Red;
                        Console.WriteLine("read slice failed,  retry in 1s , retry #: {0}", retry  );
                        Console.ResetColor();
                        retry++;
                        Thread.Sleep(1000);
                    }
                }

                if (slice == null)
                {
                    throw new Exception("failed to read from ES");
                }


                // split them into two 
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

            resolvedEvents2.AddRange(resolvedEvents);

            var result = TranslateEventsToStorageFormat(resolvedEvents2);
            AppendToFile(filePathAndName, result);
            Console.WriteLine("Append to {0} completed.", filePathAndName);

            Console.ReadLine();

        }

        private static string plainCareStringName = "Care";

        private static List<Model> TranslateEventsToStorageFormat(List<ResolvedEvent> events)
        {
            var output = new Dictionary<string, Model>(1000000);
            var ignoredEvents = 0;
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
                    case "GG.Care.WriteConcern.Messages.V1.UserStartedCaring":
                    case "GG.Care.WriteConcern.Messages.V1.UserStoppedCaring":
                    case "GG.Care.WriteConcern.Messages.V2.UserStartedCaring":
                    case "GG.Care.WriteConcern.Messages.V2.UserStoppedCaring":
                    case "GG.Care.WriteConcern.Messages.V3.UserAutoCared":
                        ignoredEvents++;
                        break;

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
                        output.Remove(careV3.UserId + careV3.EntityId);
                        output.Add(careV3.UserId + careV3.EntityId, new Model()
                        {
                            UserGuid = careV3.UserId.ToString(),
                            CharityId = careV3.EntityId,
                            CareDatetime = careV3.EventDate.ToString("yyyy-MM-ddTHH:mm:ss")////2009-06-15T13:45:30{}
                        });
                        break;

                    case "GG.Care.WriteConcern.Messages.V3.UserStoppedCaring":
                        var uncare = JsonConvert.DeserializeObject<UserStoppedCaring>(stringobject);
                        if (uncare.EntityType != "charity")
                        {
                            continue;
                        }

                        if (uncare.EntityId.Equals("2050"))
                        {
                            democharitycounter++;
                            continue;
                        }

                        output.Remove(uncare.UserId + uncare.EntityId);
                        break;

                 
                    default:
                        Console.WriteLine("Unsuporrted message type " + ev.Event.EventType);
                        break;
                }
            }

            Console.WriteLine("V1V2 + autocares messages found: " + ignoredEvents);
            Console.WriteLine("Demo charities found: " + democharitycounter);
            Console.WriteLine("Usercares prepared: " + output.Count());
            return output.Values.ToList();
        }

        public static StreamEventsSlice ReadNextEventsFromEventstore(ConnectionSettingsBuilder settings, IPEndPoint ep, string streamName, int start)
        {
            var connection = EventStoreConnection.Create(settings.Build(), ep);
            connection.ConnectAsync().Wait();

            var result = connection.ReadStreamEventsForwardAsync(streamName, start, 30000, false).Result;

            connection.Close();

            return result;
        }

        public static void AppendToFile(string filePathAndName, List<Model> events)
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
                    sw.WriteLine("{0},{1},{2}", ev.UserGuid, ev.CharityId, ev.CareDatetime);
                }
            }
        }
    }

    public class Model
    {
        public string UserGuid { get; set; }
        public string CharityId { get; set; }
        public string CareDatetime { get; set; }
       
    }
}
