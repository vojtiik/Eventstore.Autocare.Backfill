using System;

namespace GG.Care.WriteConcern.Messages.V2
{
    public class UserStartedCaring
    {
        public UserStartedCaring()
        {
            EventDate = DateTime.Now;
        }

        public int UserId { get; set; }
        public int EntityId { get; set; }
        public string EntityType { get; set; }
        public DateTime EventDate { get; set; }
    }

    public class UserStoppedCaring
    {
        public UserStoppedCaring()
        {
            EventDate = DateTime.Now;
        }

        public int UserId { get; set; }
        public int EntityId { get; set; }
        public string EntityType { get; set; }
        public DateTime EventDate { get; set; }
    }
}