using Akka.Persistence;
using Akka.Actor;

namespace DC.Akka.PersistenceMigrator.Tests;

public class TestActor : ReceivePersistentActor
{
    public static class Commands
    {
        public record WriteEvents(params object[] Events);
    }
    
    public static class Responses
    {
        public record Ack;
    }
    
    private readonly string _id;

    public TestActor(string id)
    {
        _id = id;

        Command<Commands.WriteEvents>(cmd =>
        {
            PersistAll(cmd.Events, _ => {});
            
            DeferAsync("saved", _ =>
            {
                Sender.Tell(new Responses.Ack());
            });
        });
    }

    public override string PersistenceId => $"test-actor-{_id}";
}