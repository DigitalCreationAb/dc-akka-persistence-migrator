using System.Collections.Immutable;
using Akka;
using Akka.Actor;
using Akka.Persistence;
using Akka.Persistence.Query;
using Akka.Streams;
using Akka.Streams.Dsl;

namespace DC.Akka.PersistenceMigrator;

public class EventMigrator
{
    private readonly ActorSystem _actorSystem;
    private readonly Func<Offset, Source<EventEnvelope, NotUsed>> _startSource;
    private readonly string _destinationId;
    private readonly IStoreEventMigrationPosition _storeMigrationPosition;
    private readonly RestartSettings? _restartSettings;
    private readonly Func<WriteMessagesFailed, FailureStrategy> _failureStrategy;

    private EventMigrator(
        ActorSystem actorSystem,
        Func<Offset, Source<EventEnvelope, NotUsed>> startSource,
        string destinationId,
        IStoreEventMigrationPosition storeMigrationPosition,
        RestartSettings? restartSettings,
        Func<WriteMessagesFailed, FailureStrategy> failureStrategy)
    {
        _actorSystem = actorSystem;
        _startSource = startSource;
        _destinationId = destinationId;
        _storeMigrationPosition = storeMigrationPosition;
        _restartSettings = restartSettings;
        _failureStrategy = failureStrategy;
    }

    private async Task Run(CancellationToken cancellationToken)
    {
        var extension = Persistence.Instance.Apply(_actorSystem);

        var destinationFullPath = $"akka.persistence.journal.{_destinationId}";
        
        var destination = _actorSystem.ActorOf(
            Props.Create(() => new PersistenceEventWriter(
                extension.JournalFor(destinationFullPath),
                extension.AdaptersFor(destinationFullPath))));

        var startFrom = await _storeMigrationPosition.LoadLatestOffset(cancellationToken);

        var (killSwitch, task) = RestartSource
            .OnFailuresWithBackoff(() =>
            {
                return _startSource(startFrom)
                    .SelectAsync(1, async evnt =>
                    {
                        var response = await destination
                            .Ask<IJournalResponse>(new PersistenceEventWriter.Commands.WriteEvent(evnt));

                        return response switch
                        {
                            WriteMessagesSuccessful => evnt.Offset,
                            WriteMessagesFailed failed => _failureStrategy(failed) switch
                            {
                                FailureStrategy.Ignore => evnt.Offset,
                                FailureStrategy.Throw => throw failed.Cause,
                                _ => throw new ArgumentOutOfRangeException()
                            },
                            _ => throw new Exception($"Unknown response: {response.GetType()}")
                        };
                    })
                    .GroupedWithin(1000, TimeSpan.FromSeconds(5))
                    .SelectAsync(1, async x =>
                    {
                        var listedOffset = x.ToImmutableList();

                        if (listedOffset.IsEmpty)
                            return NotUsed.Instance;

                        var highestOffset = listedOffset
                            .OrderByDescending(y => y)
                            .First();

                        await _storeMigrationPosition.SaveLatestOffset(highestOffset, cancellationToken);

                        startFrom = highestOffset;

                        return NotUsed.Instance;
                    });
            }, _restartSettings ?? RestartSettings
                .Create(TimeSpan.FromSeconds(3), TimeSpan.FromMinutes(1), 0.2)
                .WithMaxRestarts(5, TimeSpan.FromMinutes(10)))
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
    }

    public static Task RunFor<TReadJournal, TQuery>(
        ActorSystem actorSystem,
        Func<Offset, TQuery, Source<EventEnvelope, NotUsed>> createSource,
        string sourceId,
        string destinationId,
        IStoreEventMigrationPosition storeMigrationPosition,
        Func<WriteMessagesFailed, FailureStrategy> failureStrategy,
        Func<EventEnvelope, bool>? filter = null,
        RestartSettings? restartSettings = null,
        CancellationToken cancellationToken = default)
        where TReadJournal : TQuery
        where TQuery : IReadJournal
    {
        var journal = (TQuery)PersistenceQuery
            .Get(actorSystem)
            .ReadJournalFor<TReadJournal>($"akka.persistence.query.journal.{sourceId}");

        return new EventMigrator(
            actorSystem,
            x =>
            {
                return createSource(x, journal)
                    .Where(evnt => filter?.Invoke(evnt) ?? true);
            },
            destinationId,
            storeMigrationPosition,
            restartSettings,
            failureStrategy)
            .Run(cancellationToken);
    }

    public static Task RunForAll<TReadJournal>(
        ActorSystem actorSystem,
        string sourceId,
        string destinationId,
        IStoreEventMigrationPosition storeMigrationPosition,
        Func<WriteMessagesFailed, FailureStrategy> failureStrategy,
        Func<EventEnvelope, bool>? filter = null,
        RestartSettings? restartSettings = null,
        CancellationToken cancellationToken = default)
        where TReadJournal : IAllEventsQuery
    {
        return RunFor<TReadJournal, IAllEventsQuery>(
            actorSystem,
            (offset, journal) => journal.AllEvents(offset),
            sourceId,
            destinationId,
            storeMigrationPosition,
            failureStrategy,
            filter,
            restartSettings,
            cancellationToken);
    }

    public static Task RunForAllCurrent<TReadJournal>(
        ActorSystem actorSystem,
        string sourceId,
        string destinationId,
        IStoreEventMigrationPosition storeMigrationPosition,
        Func<WriteMessagesFailed, FailureStrategy> failureStrategy,
        Func<EventEnvelope, bool>? filter = null,
        RestartSettings? restartSettings = null,
        CancellationToken cancellationToken = default)
        where TReadJournal : ICurrentAllEventsQuery
    {
        return RunFor<TReadJournal, ICurrentAllEventsQuery>(
            actorSystem,
            (offset, journal) => journal.CurrentAllEvents(offset),
            sourceId,
            destinationId,
            storeMigrationPosition,
            failureStrategy,
            filter,
            restartSettings,
            cancellationToken);
    }

    public static Task RunForTag<TReadJournal>(
        string tag,
        ActorSystem actorSystem,
        string sourceId,
        string destinationId,
        IStoreEventMigrationPosition storeMigrationPosition,
        Func<WriteMessagesFailed, FailureStrategy> failureStrategy,
        Func<EventEnvelope, bool>? filter = null,
        RestartSettings? restartSettings = null,
        CancellationToken cancellationToken = default)
        where TReadJournal : IEventsByTagQuery
    {
        return RunFor<TReadJournal, IEventsByTagQuery>(
            actorSystem,
            (offset, journal) => journal.EventsByTag(tag, offset),
            sourceId,
            destinationId,
            storeMigrationPosition,
            failureStrategy,
            filter,
            restartSettings,
            cancellationToken);
    }

    public static Task RunForTagCurrent<TReadJournal>(
        string tag,
        ActorSystem actorSystem,
        string sourceId,
        string destinationId,
        IStoreEventMigrationPosition storeMigrationPosition,
        Func<WriteMessagesFailed, FailureStrategy> failureStrategy,
        Func<EventEnvelope, bool>? filter = null,
        RestartSettings? restartSettings = null,
        CancellationToken cancellationToken = default)
        where TReadJournal : ICurrentEventsByTagQuery
    {
        return RunFor<TReadJournal, ICurrentEventsByTagQuery>(
            actorSystem,
            (offset, journal) => journal.CurrentEventsByTag(tag, offset),
            sourceId,
            destinationId,
            storeMigrationPosition,
            failureStrategy,
            filter,
            restartSettings,
            cancellationToken);
    }
}