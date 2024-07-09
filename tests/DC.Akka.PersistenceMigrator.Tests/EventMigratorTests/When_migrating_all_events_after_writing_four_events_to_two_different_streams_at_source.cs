using System.Collections.Immutable;
using Akka.Actor;
using Akka.Persistence.Query;
using Akka.Persistence.Query.InMemory;
using Akka.Streams;
using Akka.TestKit.Xunit2;
using FluentAssertions;
using Xunit;

namespace DC.Akka.PersistenceMigrator.Tests.EventMigratorTests;

public class When_migrating_all_events_after_writing_four_events_to_two_different_streams_at_source(
    When_migrating_all_events_after_writing_four_events_to_two_different_streams_at_source.Setup setup)
    : IClassFixture<When_migrating_all_events_after_writing_four_events_to_two_different_streams_at_source.Setup>
{
    [Fact]
    public void Then_two_persistence_ids_should_have_been_migrated()
    {
        setup.Results.Should().HaveCount(2);
    }

    [Fact]
    public void Then_first_event_should_be_correct()
    {
        setup.Results["test-actor-1"][0].Event.Should().Be("event-1");
    }
    
    [Fact]
    public void Then_second_event_should_be_correct()
    {
        setup.Results["test-actor-1"][1].Event.Should().Be("event-2");
    }
    
    [Fact]
    public void Then_third_event_should_be_correct()
    {
        setup.Results["test-actor-2"][0].Event.Should().Be("event-3");
    }
    
    [Fact]
    public void Then_fourth_event_should_be_correct()
    {
        setup.Results["test-actor-2"][1].Event.Should().Be("event-4");
    }
    
    [Fact]
    public void Then_first_event_should_have_correct_sequence_number()
    {
        setup.Results["test-actor-1"][0].SequenceNr.Should().Be(1);
    }
    
    [Fact]
    public void Then_second_event_should_have_correct_sequence_number()
    {
        setup.Results["test-actor-1"][1].SequenceNr.Should().Be(2);
    }
    
    [Fact]
    public void Then_third_event_should_have_correct_sequence_number()
    {
        setup.Results["test-actor-2"][0].SequenceNr.Should().Be(1);
    }
    
    [Fact]
    public void Then_fourth_event_should_have_correct_sequence_number()
    {
        setup.Results["test-actor-2"][1].SequenceNr.Should().Be(2);
    }
    
    public class Setup() : TestKit($$"""
                                    akka.persistence {
                                        journal {
                                            plugin = "akka.persistence.journal.{{SourcePluginName}}"
                                        
                                            {{SourcePluginName}} {
                                                class = "Akka.Persistence.Journal.MemoryJournal, Akka.Persistence"
                                                plugin-dispatcher = "akka.actor.default-dispatcher"
                                            }
                                            
                                            {{DestinationPluginName}} {
                                                class = "Akka.Persistence.Journal.MemoryJournal, Akka.Persistence"
                                                plugin-dispatcher = "akka.actor.default-dispatcher"
                                            }
                                        }
                                        
                                        query.journal {
                                            {{SourcePluginName}} {
                                                class = "Akka.Persistence.Query.InMemory.InMemoryReadJournalProvider, Akka.Persistence.Query.InMemory"
                                                write-plugin = "akka.persistence.journal.{{SourcePluginName}}"
                                                max-buffer-size = 100
                                            }
                                            
                                            {{DestinationPluginName}} {
                                                class = "Akka.Persistence.Query.InMemory.InMemoryReadJournalProvider, Akka.Persistence.Query.InMemory"
                                                write-plugin = "akka.persistence.journal.{{DestinationPluginName}}"
                                                max-buffer-size = 100
                                            }
                                        }
                                    }
                                    """), IAsyncLifetime
    {
        private const string SourcePluginName = "source";
        private const string DestinationPluginName = "destination";
        
        public IImmutableDictionary<string, ImmutableList<EventEnvelope>> Results { get; private set; } =
            ImmutableDictionary<string, ImmutableList<EventEnvelope>>.Empty;
        
        private readonly CancellationTokenSource _cts = new();

        public async Task InitializeAsync()
        {
            var testActor1 = Sys.ActorOf(Props.Create(() => new TestActor("1")));
            var testActor2 = Sys.ActorOf(Props.Create(() => new TestActor("2")));

            await testActor1.Ask<TestActor.Responses.Ack>(
                new TestActor.Commands.WriteEvents("event-1", "event-2"));
            
            await testActor2.Ask<TestActor.Responses.Ack>(
                new TestActor.Commands.WriteEvents("event-3", "event-4"));
            
            _cts.CancelAfter(TimeSpan.FromSeconds(10));

            await EventMigrator
                .RunForAllCurrent<InMemoryReadJournal>(
                    Sys,
                    SourcePluginName,
                    DestinationPluginName,
                    new StoreEventMigrationPositionInMemory(),
                    new DeduplicateEventsUsingSequenceNumber(),
                    restartSettings: RestartSettings.Create(
                            TimeSpan.Zero,
                            TimeSpan.Zero,
                            0)
                        .WithMaxRestarts(0, TimeSpan.FromMinutes(1)),
                    cancellationToken: _cts.Token);

            var journal = PersistenceQuery
                .Get(Sys)
                .ReadJournalFor<InMemoryReadJournal>($"akka.persistence.query.journal.{DestinationPluginName}");

            var results = await journal
                .CurrentAllEvents(Offset.NoOffset())
                .RunAggregate(
                    ImmutableList<EventEnvelope>.Empty,
                    (list, item) => list.Add(item),
                    Sys.Materializer());

            Results = results
                .GroupBy(x => x.PersistenceId)
                .ToImmutableDictionary(
                    x => x.Key,
                    x => x.ToImmutableList());
        }

        public Task DisposeAsync()
        {
            return _cts.CancelAsync();
        }
    }
}