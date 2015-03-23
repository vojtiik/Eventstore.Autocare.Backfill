namespace GG.Care.WriteConcern.Messages.V1
{
    public class UserStartedCaring
    {
        public int UserId { get; set; }
        public int EntityId { get; set; }
        public string EntityType { get; set; }
    }

    public class UserStoppedCaring
    {
        public int UserId { get; set; }
        public int EntityId { get; set; }
        public string EntityType { get; set; }
    }
}