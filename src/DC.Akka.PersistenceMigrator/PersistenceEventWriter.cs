using System.Collections.Immutable;
using System.Diagnostics.CodeAnalysis;
using Akka.Actor;
using Akka.Persistence;
using Akka.Persistence.Journal;
using Akka.Persistence.Query;

namespace DC.Akka.PersistenceMigrator;

internal class PersistenceEventWriter : ReceiveActor
{
    private readonly IActorRef _journalRef;
    private readonly EventAdapters _eventAdapters;
    private readonly IDeduplicateEvents _deduplicateEvents;

    public static class Commands
    {
        public record WriteEvents(string PersistenceId, IImmutableList<EventEnvelope> Events);
    }
    
    public static class Responses
    {
        public record WriteEventsResponse(Exception? Error = null)
        {
            [MemberNotNullWhen(false, nameof(Error))]
            public bool IsSuccessful => Error == null;
        }
    }

    public PersistenceEventWriter(
        IActorRef journalRef,
        EventAdapters eventAdapters,
        IDeduplicateEvents deduplicateEvents)
    {
        _journalRef = journalRef;
        _eventAdapters = eventAdapters;
        _deduplicateEvents = deduplicateEvents;

        Receive<Commands.WriteEvents>(cmd =>
        {
            var writer = GetWriter(cmd.PersistenceId);
            
            writer.Tell(cmd, Sender);
        });
    }

    private IActorRef GetWriter(string persistenceId)
    {
        var writer = Context.Child(persistenceId);

        return writer.IsNobody()
            ? Context.ActorOf(Props.Create(() => new EventWriter(
                persistenceId,
                _journalRef,
                _eventAdapters,
                _deduplicateEvents)))
            : writer;
    }

    private class EventWriter : ReceiveActor, IWithTimers
    {
        private static class WriterCommands
        {
            public record Stop;
        }
        
        private readonly string _persistenceId;
        private readonly IActorRef _journalRef;
        private readonly EventAdapters _eventAdapters;
        private readonly IDeduplicateEvents _deduplicateEvents;

        public EventWriter(
            string persistenceId,
            IActorRef journalRef,
            EventAdapters eventAdapters,
            IDeduplicateEvents deduplicateEvents)
        {
            _persistenceId = persistenceId;
            _journalRef = journalRef;
            _eventAdapters = eventAdapters;
            _deduplicateEvents = deduplicateEvents;

            Become(NotLoaded);
            
            ScheduleStop();
        }

        public ITimerScheduler Timers { get; set; } = null!;

        private void NotLoaded()
        {
            Receive<Commands.WriteEvents>(cmd =>
            {
                _journalRef.Tell(new ReplayMessages(0, long.MaxValue, long.MaxValue, _persistenceId, Self));

                var sender = Sender;

                Become(() => Replaying(cmd, sender));
                
                ScheduleStop();
            });

            Receive<WriterCommands.Stop>(_ =>
            {
                Context.Stop(Self);
            });
        }

        private void Replaying(Commands.WriteEvents writing, IActorRef replyTo)
        {
            var replayedEventNumbers = new List<string>();

            Receive<ReplayedMessage>(cmd =>
            {
                replayedEventNumbers.Add(_deduplicateEvents.GetDeduplicationKey(cmd.Persistent));
                
                ScheduleStop();
            });

            Receive<RecoverySuccess>(cmd =>
            {
                Self.Tell(writing, replyTo);

                Become(() => Loaded(replayedEventNumbers.ToImmutableList(), cmd.HighestSequenceNr));
                
                ScheduleStop();
            });
            
            Receive<WriterCommands.Stop>(_ =>
            {
                Context.Stop(Self);
            });
        }

        private void Loaded(IImmutableList<string> migratedEvents, long currentSequenceNumber)
        {
            ReceiveAsync<Commands.WriteEvents>(async cmd =>
            {
                try
                {
                    var eventsToWrite = cmd
                        .Events
                        .Select((x, index) =>
                        {
                            var adapter = _eventAdapters.Get(x.Event.GetType());
                            
                            var evnt = new Persistent(
                                adapter.ToJournal(x.Event),
                                currentSequenceNumber + index + 1,
                                _persistenceId,
                                timestamp: x.Timestamp);
                            
                            return new
                            {
                                Event = (IPersistentRepresentation)evnt,
                                DedupKey = _deduplicateEvents.GetDeduplicationKey(evnt)
                            };
                        })
                        .Where(x => !migratedEvents.Contains(x.DedupKey))
                        .ToImmutableList();

                    if (eventsToWrite.IsEmpty)
                    {
                        Sender.Tell(new Responses.WriteEventsResponse());

                        return;
                    }

                    var atomicWrite = new AtomicWrite(eventsToWrite
                        .Select(x => x.Event)
                        .ToImmutableList());
                    
                    var writeResponse = await _journalRef.Ask<IJournalResponse>(
                        new WriteMessages(new List<IPersistentEnvelope>
                            {
                                atomicWrite
                            },
                            Self,
                            1),
                        TimeSpan.FromSeconds(5));

                    switch (writeResponse)
                    {
                        case WriteMessagesSuccessful:
                            Sender.Tell(new Responses.WriteEventsResponse());

                            Become(() =>
                                Loaded(
                                    migratedEvents.AddRange(eventsToWrite.Select(x => x.DedupKey)),
                                    atomicWrite.HighestSequenceNr));
                            break;
                        case WriteMessagesFailed failure:
                            Sender.Tell(new Responses.WriteEventsResponse(failure.Cause));
                            break;
                        default:
                            Sender.Tell(new Responses.WriteEventsResponse(new Exception("Unknown response")));
                            break;
                    }
                }
                catch (Exception e)
                {
                    Sender.Tell(new Responses.WriteEventsResponse(e));
                }
                finally
                {
                    ScheduleStop();
                }
            });
            
            Receive<WriterCommands.Stop>(_ =>
            {
                Context.Stop(Self);
            });
        }

        private void ScheduleStop()
        {
            Timers.StartSingleTimer(
                "stop",
                new WriterCommands.Stop(),
                TimeSpan.FromMinutes(2));
        }
    }
}