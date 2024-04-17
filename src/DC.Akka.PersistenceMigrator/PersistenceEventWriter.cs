using Akka.Actor;
using Akka.Persistence;
using Akka.Persistence.Journal;
using Akka.Persistence.Query;

namespace DC.Akka.PersistenceMigrator;

internal class PersistenceEventWriter : ReceiveActor
{
    public static class Commands
    {
        public record WriteEvent(EventEnvelope Event);
    }

    public PersistenceEventWriter(ICanTell journalRef, EventAdapters eventAdapters)
    {
        Receive<Commands.WriteEvent>(cmd =>
        {
            var adapter = eventAdapters.Get(cmd.Event.GetType());

            journalRef.Tell(new WriteMessages(new List<IPersistentEnvelope>
                    {
                        new AtomicWrite(new Persistent(
                            adapter.ToJournal(cmd.Event.Event),
                            cmd.Event.SequenceNr,
                            cmd.Event.PersistenceId,
                            timestamp: cmd.Event.Timestamp))
                    },
                    Sender,
                    1),
                Sender);
        });
    }
}