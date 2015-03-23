using System;

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