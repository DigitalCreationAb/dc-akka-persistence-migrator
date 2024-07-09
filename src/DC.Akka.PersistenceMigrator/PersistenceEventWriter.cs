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
                _deduplicateEvents)),
                persistenceId)
            : writer;
    }

    private class EventWriter : ReceiveActor, IWithTimers, IWithStash
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
        public IStash Stash { get; set; } = null!;

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
                
                Stash.UnstashAll();
                
                ScheduleStop();
            });
            
            Receive<WriterCommands.Stop>(_ =>
            {
                Context.Stop(Self);
            });

            Receive<Commands.WriteEvents>(_ =>
            {
                Stash.Stash();
            });
        }

        private void Loaded(IImmutableList<string> migratedEvents, long currentSequenceNumber)
        {
            Receive<Commands.WriteEvents>(cmd =>
            {
                try
                {
                    var eventsToWrite = cmd
                        .Events
                        .OrderBy(x => x.SequenceNr)
                        .Select(x =>
                        {
                            var adapter = _eventAdapters.Get(x.Event.GetType());
                            
                            var evnt = new Persistent(
                                adapter.ToJournal(x.Event),
                                x.SequenceNr,
                                _persistenceId,
                                timestamp: x.Timestamp);
                            
                            return new
                            {
                                Event = evnt,
                                DedupKey = _deduplicateEvents.GetDeduplicationKey(evnt)
                            };
                        })
                        .Where(x => !migratedEvents.Contains(x.DedupKey))
                        .Select((x, index) => new
                        {
                            x.DedupKey,
                            Event = x.Event.Update(
                                currentSequenceNumber + index + 1,
                                x.Event.PersistenceId,
                                x.Event.IsDeleted,
                                x.Event.Sender,
                                x.Event.WriterGuid)
                        })
                        .ToImmutableList();

                    if (eventsToWrite.IsEmpty)
                    {
                        Sender.Tell(new Responses.WriteEventsResponse());

                        return;
                    }

                    var atomicWrite = new AtomicWrite(eventsToWrite
                        .Select(x => x.Event)
                        .ToImmutableList());
                    
                    _journalRef.Tell(
                        new WriteMessages(new List<IPersistentEnvelope>
                            {
                                atomicWrite
                            },
                            Self,
                            1),
                        Self);
                    
                    Become(() => Writing(
                        migratedEvents,
                        currentSequenceNumber,
                        eventsToWrite.Select(x => x.DedupKey).ToImmutableList(),
                        atomicWrite.HighestSequenceNr,
                        Sender));
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

        private void Writing(
            IImmutableList<string> migratedEvents,
            long currentSequenceNumber,
            IImmutableList<string> waitingForEvents,
            long newSequenceNumber,
            IActorRef replyTo)
        {
            Receive<IJournalResponse>(writeResponse =>
            {
                switch (writeResponse)
                {
                    case WriteMessagesSuccessful:
                        replyTo.Tell(new Responses.WriteEventsResponse());

                        Become(() =>
                            Loaded(
                                migratedEvents.AddRange(waitingForEvents),
                                newSequenceNumber));
                        break;
                    case WriteMessagesFailed failure:
                        replyTo.Tell(new Responses.WriteEventsResponse(failure.Cause));
                        
                        Become(() => Loaded(migratedEvents, currentSequenceNumber));
                        break;
                    default:
                        replyTo.Tell(new Responses.WriteEventsResponse(new Exception("Unknown response")));
                        
                        Become(() => Loaded(migratedEvents, currentSequenceNumber));
                        break;
                }
                
                Stash.UnstashAll();
            });
            
            Receive<WriterCommands.Stop>(_ =>
            {
                Context.Stop(Self);
            });
            
            Receive<Commands.WriteEvents>(_ =>
            {
                Stash.Stash();
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