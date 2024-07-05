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

    public PersistenceEventWriter(IActorRef journalRef, EventAdapters eventAdapters)
    {
        _journalRef = journalRef;
        _eventAdapters = eventAdapters;
        
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
                _eventAdapters)))
            : writer;
    }

    private class EventWriter : ReceiveActor, IWithTimers
    {
        public static class WriterCommands
        {
            public record Stop;
        }
        
        private readonly string _persistenceId;
        private readonly IActorRef _journalRef;
        private readonly EventAdapters _eventAdapters;

        public EventWriter(string persistenceId, IActorRef journalRef, EventAdapters eventAdapters)
        {
            _persistenceId = persistenceId;
            _journalRef = journalRef;
            _eventAdapters = eventAdapters;

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
            var replayedEventNumbers = new List<long>();

            Receive<ReplayedMessage>(cmd =>
            {
                replayedEventNumbers.Add(cmd.Persistent.SequenceNr);
                
                ScheduleStop();
            });

            Receive<RecoverySuccess>(_ =>
            {
                Self.Tell(writing, replyTo);

                Become(() => Loaded(replayedEventNumbers.ToImmutableList()));
                
                ScheduleStop();
            });
            
            Receive<WriterCommands.Stop>(_ =>
            {
                Context.Stop(Self);
            });
        }

        private void Loaded(IImmutableList<long> replayedEventNumbers)
        {
            ReceiveAsync<Commands.WriteEvents>(async cmd =>
            {
                try
                {
                    var eventsToWrite = cmd
                        .Events
                        .Where(x => !replayedEventNumbers.Contains(x.SequenceNr))
                        .ToImmutableList();

                    if (eventsToWrite.IsEmpty)
                    {
                        Sender.Tell(new Responses.WriteEventsResponse());

                        return;
                    }

                    var writeResponse = await _journalRef.Ask<IJournalResponse>(
                        new WriteMessages(new List<IPersistentEnvelope>
                            {
                                new AtomicWrite(cmd
                                    .Events
                                    .Select(x =>
                                    {
                                        var adapter = _eventAdapters.Get(x.Event.GetType());

                                        return (IPersistentRepresentation)new Persistent(
                                            adapter.ToJournal(x.Event),
                                            x.SequenceNr,
                                            _persistenceId,
                                            timestamp: x.Timestamp);
                                    })
                                    .ToImmutableList())
                            },
                            Sender,
                            1),
                        TimeSpan.FromSeconds(5));

                    switch (writeResponse)
                    {
                        case WriteMessagesSuccessful:
                            Sender.Tell(new Responses.WriteEventsResponse());

                            Become(() =>
                                Loaded(replayedEventNumbers.AddRange(eventsToWrite.Select(x => x.SequenceNr))));
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