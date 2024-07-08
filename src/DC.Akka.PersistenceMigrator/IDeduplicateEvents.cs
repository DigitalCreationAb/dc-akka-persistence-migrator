using Akka.Persistence;

namespace DC.Akka.PersistenceMigrator;

public interface IDeduplicateEvents
{
    string GetDeduplicationKey(IPersistentRepresentation evnt);
}