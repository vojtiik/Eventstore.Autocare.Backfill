using System;
using System.Collections.Generic;
using System.Configuration;
using System.IO;
using System.Linq;
using System.Net;
using System.Text;
using System.Threading.Tasks;
using Eventstore.Autocare.Backfill.GG.Care.WriteConcern.Messages.V3;
using EventStore.ClientAPI;
using EventStore.ClientAPI.SystemData;
using Newtonsoft.Json;


namespace Eventstore.Autocare.Backfill
{
    public class Program
    {
        static void Main(string[] args)
        {
            string esIP = ConfigurationManager.AppSettings.Get("eventstoreIP"); // 1113
            var esPort = int.Parse(ConfigurationManager.AppSettings.Get("eventStorePort")); //IPAddress.Loopback;
            var ip = IPAddress.Parse(esIP);
            var settings = ConnectionSettings.Create();
            settings
                .UseConsoleLogger()
                .SetDefaultUserCredentials(new UserCredentials(
                    ConfigurationManager.AppSettings.Get("eventstoreUsr"),
                    ConfigurationManager.AppSettings.Get("eventstorePass")));
            var connection = EventStoreConnection.Create(settings.Build(), new IPEndPoint(ip, esPort));

            connection.ConnectAsync().Wait();
            string streamname = ConfigurationManager.AppSettings.Get("stream"); // "backfillauto6";
            string sourcePath = ConfigurationManager.AppSettings.Get("path"); // @"d:\autocare_backfill\";
            string sourceFileName = ConfigurationManager.AppSettings.Get("filename"); // "autocare_";
            string filePathAndName = sourcePath + sourceFileName;

            var fileIndex = 0;

            while (true)
            {
                List<UserAutoCared> autocareData;
                try
                {
                    autocareData = ReadEventsFromFile(fileIndex, filePathAndName);
                    Console.WriteLine("Completed read from sourcefile with  index : " + fileIndex);
                    Console.WriteLine(autocareData.Count + " autocared events found.");
                    fileIndex++;

                }
                catch (FileNotFoundException fnf)
                {
                    Console.WriteLine("File not found / backfill completed. Last processed index: " + fileIndex);
                    Console.WriteLine(fnf.ToString());
                    Console.ReadLine();
                    break;
                }

                catch (Exception e)
                {
                    Console.WriteLine(e.ToString());
                    Console.ReadLine();
                    // quit
                    break;
                }

                var events = BuildEventData(autocareData);

                AppendToEventStore(connection, streamname, events).Wait();

                Console.WriteLine("{0} events appended to the {1} stream.", events.Count(), streamname);
            }

        }

        public static async Task AppendToEventStore(IEventStoreConnection connection, string streamName, List<EventData> eventData)
        {
            await connection.AppendToStreamAsync(streamName, ExpectedVersion.Any, eventData);
        }


        public static List<UserAutoCared> ReadEventsFromFile(int fileIndex, string filePathAndName)
        {
            string text;
            using (StreamReader streamReader = new StreamReader(filePathAndName + fileIndex + ".json", Encoding.UTF8))
            {
                text = streamReader.ReadToEnd();
            }

            var autocares = JsonConvert.DeserializeObject<List<UserAutoCared>>(text);

            return autocares;
        }

        public static List<EventData> BuildEventData(List<UserAutoCared> autocareDataList)
        {
            var events = new List<EventData>();

            var serializerSettings = new JsonSerializerSettings { TypeNameHandling = TypeNameHandling.None };
            foreach (var autocaredata in autocareDataList)
            {
                var data = JsonConvert.SerializeObject(autocaredata, serializerSettings);

                var myEvent = new EventData(
                         Guid.NewGuid(),
                         "GG.Care.WriteConcern.Messages.V3.UserAutoCared",
                         true,
                         Encoding.UTF8.GetBytes(data),
                         null);

                events.Add(myEvent);
            }

            return events;
        }
    }

    namespace GG.Care.WriteConcern.Messages.V3
    {
        public class UserStartedCaring
        {
            public UserStartedCaring()
            {
                EventDate = DateTime.Now;
            }

            public Guid UserId { get; set; }
            public string EntityId { get; set; }
            public string EntityType { get; set; }
            public DateTime EventDate { get; set; }
        }
        public class UserAutoCared
        {
            public UserAutoCared()
            {
                EventDate = DateTime.UtcNow;
            }

            public Guid UserId { get; set; }
            public string EntityId { get; set; }
            public string EntityType { get; set; }
            public DateTime EventDate { get; set; }
            public string SourceEntityId { get; set; }
            public string SourceEntityType { get; set; }
        }

        public class UserStoppedCaring
        {
            public UserStoppedCaring()
            {
                EventDate = DateTime.Now;
            }

            public Guid UserId { get; set; }
            public string EntityId { get; set; }
            public string EntityType { get; set; }
            public DateTime EventDate { get; set; }
        }

    }



}
