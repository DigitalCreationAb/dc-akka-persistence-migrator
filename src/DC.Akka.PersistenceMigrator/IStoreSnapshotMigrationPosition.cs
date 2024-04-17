using System.Collections.Immutable;

namespace DC.Akka.PersistenceMigrator;

public interface IStoreSnapshotMigrationPosition
{
    Task<IImmutableDictionary<string, long>> LoadLatestSnapshotsForPersistenceIds(
        IImmutableList<string> persistenceIds,
        CancellationToken cancellationToken = default);
    
    Task StoreLatestSnapshotForPersistenceIds(
        IImmutableDictionary<string, long> persistenceIdsSequenceNumbers,
        CancellationToken cancellationToken = default);
}