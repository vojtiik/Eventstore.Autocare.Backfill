using System;
using System.Collections.Generic;
using System.Configuration;
using System.IO;
using System.Linq;
using System.Net;
using System.Text;
using System.Threading.Tasks;
using Eventstore.Autocare.EventsGenerator.GG.Care.WriteConcern.Messages.V3;
using EventStore.ClientAPI;
using EventStore.ClientAPI.SystemData;
using Newtonsoft.Json;

namespace Eventstore.Autocare.EventsGenerator
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

            // LoadEvForStressTest(connection, streamname);

            var users = new[] { Guid.NewGuid(), Guid.NewGuid() };


            var events = BuildAutoCareForUser(users, "2");
            AppendToEventStore(connection, streamname, events).Wait();
            Console.WriteLine("{0} events appended to the {1} stream.", events.Count(), streamname);

            events = BuildAutoCareForUser(users, "1");
            AppendToEventStore(connection, streamname, events).Wait();
            Console.WriteLine("{0} events appended to the {1} stream.", events.Count(), streamname);

            events = BuildUnCareForUser(users, "1");
            AppendToEventStore(connection, streamname, events).Wait();
            Console.WriteLine("{0} events appended to the {1} stream.", events.Count(), streamname);

            events = BuildCareForUser(users, "2");
            AppendToEventStore(connection, streamname, events).Wait();
            Console.WriteLine("{0} events appended to the {1} stream.", events.Count(), streamname);


            //events = BuildCareForUser(users, "1");
            //AppendToEventStore(connection, streamname, events).Wait();
            //Console.WriteLine("{0} events appended to the {1} stream.", events.Count(), streamname);

            Console.WriteLine("Done.");
            Console.ReadLine();

        }

        private static void LoadEvForStressTest(IEventStoreConnection connection, string streamname)
        {
            var events = BuildAutocares(10);
            AppendToEventStore(connection, streamname, events).Wait();
            Console.WriteLine("{0} events appended to the {1} stream.", events.Count(), streamname);

            events = BuildCares(10);
            AppendToEventStore(connection, streamname, events).Wait();
            Console.WriteLine("{0} events appended to the {1} stream.", events.Count(), streamname);

            events = BuildUnCares(10);
            AppendToEventStore(connection, streamname, events).Wait();
            Console.WriteLine("{0} events appended to the {1} stream.", events.Count(), streamname);
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

        public static List<EventData> BuildAutocares(int count)
        {
            var events = new List<EventData>();

            var serializerSettings = new JsonSerializerSettings { TypeNameHandling = TypeNameHandling.None };
            for (int i = 0; i < count; i++)
            {
                var autocareevent = new UserAutoCared
                {
                    EntityId = Guid.NewGuid().ToString(),
                    EntityType = "charity",
                    SourceEntityType = Guid.NewGuid().ToString(),
                    SourceEntityId = Guid.NewGuid().ToString(),
                    UserId = Guid.NewGuid(),
                };

                var data = JsonConvert.SerializeObject(autocareevent, serializerSettings);

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

        private static List<EventData> BuildUnCares(int count)
        {
            var events = new List<EventData>();

            var serializerSettings = new JsonSerializerSettings { TypeNameHandling = TypeNameHandling.None };
            for (int i = 0; i < count; i++)
            {
                var autocareevent = new UserStoppedCaring()
                {
                    EntityId = Guid.NewGuid().ToString(),
                    EntityType = "charity",
                    UserId = Guid.NewGuid(),
                };

                var data = JsonConvert.SerializeObject(autocareevent, serializerSettings);

                var myEvent = new EventData(
                         Guid.NewGuid(),
                         "GG.Care.WriteConcern.Messages.V3.UserStoppedCaring",
                         true,
                         Encoding.UTF8.GetBytes(data),
                         null);

                events.Add(myEvent);
            }

            return events;
        }

        private static List<EventData> BuildCares(int count)
        {
            var events = new List<EventData>();

            var serializerSettings = new JsonSerializerSettings { TypeNameHandling = TypeNameHandling.None };
            for (int i = 0; i < count; i++)
            {
                var autocareevent = new UserStartedCaring
                {
                    EntityId = Guid.NewGuid().ToString(),
                    EntityType = "charity",
                    UserId = Guid.NewGuid(),
                };

                var data = JsonConvert.SerializeObject(autocareevent, serializerSettings);

                var myEvent = new EventData(
                         Guid.NewGuid(),
                         "GG.Care.WriteConcern.Messages.V3.UserStartedCaring",
                         true,
                         Encoding.UTF8.GetBytes(data),
                         null);

                events.Add(myEvent);
            }

            return events;
        }

        private static List<EventData> BuildCareForUser(Guid[] userids, string charityId)
        {
            var events = new List<EventData>();

            var serializerSettings = new JsonSerializerSettings { TypeNameHandling = TypeNameHandling.None };
            for (int i = 0; i < userids.Length; i++)
            {
                var autocareevent = new UserStartedCaring
                {
                    EntityId = charityId,
                    EntityType = "charity",
                    UserId = userids[i],
                };

                var data = JsonConvert.SerializeObject(autocareevent, serializerSettings);

                var myEvent = new EventData(
                         Guid.NewGuid(),
                         "GG.Care.WriteConcern.Messages.V3.UserStartedCaring",
                         true,
                         Encoding.UTF8.GetBytes(data),
                         null);

                events.Add(myEvent);
            }

            return events;
        }

        private static List<EventData> BuildUnCareForUser(Guid[] userids, string charityId)
        {
            var events = new List<EventData>();

            var serializerSettings = new JsonSerializerSettings { TypeNameHandling = TypeNameHandling.None };
            for (int i = 0; i < userids.Length; i++)
            {
                var autocareevent = new UserStoppedCaring
                {
                    EntityId = charityId,
                    EntityType = "charity",
                    UserId = userids[i],
                };

                var data = JsonConvert.SerializeObject(autocareevent, serializerSettings);

                var myEvent = new EventData(
                         Guid.NewGuid(),
                         "GG.Care.WriteConcern.Messages.V3.UserStoppedCaring",
                         true,
                         Encoding.UTF8.GetBytes(data),
                         null);

                events.Add(myEvent);
            }

            return events;
        }

        private static List<EventData> BuildAutoCareForUser(Guid[] userids, string charityId)
        {
            var events = new List<EventData>();

            var serializerSettings = new JsonSerializerSettings { TypeNameHandling = TypeNameHandling.None };
            for (int i = 0; i < userids.Length; i++)
            {
                var autocareevent = new UserAutoCared()
                {
                    EntityId = charityId,
                    EntityType = "charity",
                    UserId = userids[i],
                    SourceEntityType = "FRP-created",
                    SourceEntityId = Guid.NewGuid().ToString(),
                };

                var data = JsonConvert.SerializeObject(autocareevent, serializerSettings);

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
