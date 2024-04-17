using System.Collections.Immutable;
using Akka;
using Akka.Actor;
using Akka.Persistence;
using Akka.Persistence.Query;
using Akka.Streams;
using Akka.Streams.Dsl;

namespace DC.Akka.PersistenceMigrator;

public class SnapshotMigrator
{
    private readonly ActorSystem _actorSystem;
    private readonly IPersistenceIdsQuery _query;
    private readonly string _destinationPersistence;
    private readonly IStoreSnapshotMigrationPosition _storeSnapshotMigrationPosition;

    private SnapshotMigrator(
        ActorSystem actorSystem,
        IPersistenceIdsQuery query,
        string destinationPersistence,
        IStoreSnapshotMigrationPosition storeSnapshotMigrationPosition)
    {
        _actorSystem = actorSystem;
        _query = query;
        _destinationPersistence = destinationPersistence;
        _storeSnapshotMigrationPosition = storeSnapshotMigrationPosition;
    }

    private async Task Run(CancellationToken cancellationToken)
    {
        var extension = Persistence.Instance.Apply(_actorSystem);
        var destination = extension.SnapshotStoreFor(_destinationPersistence);

        while (!cancellationToken.IsCancellationRequested)
        {
            var (killSwitch, task) = _query
                .PersistenceIds()
                .GroupedWithin(100, TimeSpan.FromSeconds(2))
                .SelectAsync(1, async ids =>
                {
                    var idsList = ids.ToImmutableList();

                    var latestPositions = await _storeSnapshotMigrationPosition
                        .LoadLatestSnapshotsForPersistenceIds(idsList, cancellationToken);

                    var storedSnapshots = new Dictionary<string, long>();

                    foreach (var persistenceId in idsList)
                    {
                        var response = await destination
                            .Ask<LoadSnapshotResult>(new LoadSnapshot(
                                persistenceId,
                                new SnapshotSelectionCriteria(long.MaxValue),
                                long.MaxValue));

                        if (latestPositions.ContainsKey(persistenceId)
                            && latestPositions[persistenceId] >= response.Snapshot.Metadata.SequenceNr)
                        {
                            continue;
                        }

                        if (response.Snapshot is null)
                            continue;

                        await destination
                            .Ask<SaveSnapshotSuccess>(new SaveSnapshot(
                                response.Snapshot.Metadata,
                                response.Snapshot.Snapshot));

                        storedSnapshots[persistenceId] = response.Snapshot.Metadata.SequenceNr;
                    }

                    return storedSnapshots.ToImmutableDictionary();
                })
                .SelectAsync(1, async storedSnapshots =>
                {
                    await _storeSnapshotMigrationPosition.StoreLatestSnapshotForPersistenceIds(
                        storedSnapshots,
                        cancellationToken);

                    return NotUsed.Instance;
                })
                .ViaMaterialized(KillSwitches.Single<NotUsed>(), Keep.Right)
                .ToMaterialized(Sink.Ignore<NotUsed>(), Keep.Both)
                .Run(_actorSystem.Materializer());

            try
            {
                await Task.WhenAny(task, Task.Delay(Timeout.Infinite, cancellationToken));
            }
            finally
            {
                killSwitch.Shutdown();
            }

            await Task.Delay(TimeSpan.FromMinutes(10), cancellationToken);
        }
    }

    public static Task RunFor<TReadJournal>(
        ActorSystem actorSystem,
        string journalId,
        string destinationPersistence,
        IStoreSnapshotMigrationPosition storeSnapshotMigrationPosition,
        CancellationToken cancellationToken = default) where TReadJournal : IPersistenceIdsQuery
    {
        var journal = (IPersistenceIdsQuery)PersistenceQuery
            .Get(actorSystem)
            .ReadJournalFor<TReadJournal>(journalId);

        return new SnapshotMigrator(
                actorSystem,
                journal,
                destinationPersistence,
                storeSnapshotMigrationPosition)
            .Run(cancellationToken);
    }
}