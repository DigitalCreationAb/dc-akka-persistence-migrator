using Akka.Persistence;

namespace DC.Akka.PersistenceMigrator;

public class DeduplicateEventsUsingSequenceNumber : IDeduplicateEvents
{
    public string GetDeduplicationKey(IPersistentRepresentation evnt)
    {
        return evnt.SequenceNr.ToString();
    }
}