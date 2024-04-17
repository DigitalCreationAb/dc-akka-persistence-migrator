using Akka.Persistence.Query;

namespace DC.Akka.PersistenceMigrator;

public interface IStoreEventMigrationPosition
{
    Task<Offset> LoadLatestOffset(CancellationToken cancellationToken = default);
    Task SaveLatestOffset(Offset offset, CancellationToken cancellationToken = default);
}