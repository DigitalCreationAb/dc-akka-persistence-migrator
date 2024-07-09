using System.Collections.Immutable;
using Akka.Actor;
using Akka.Persistence.Query;
using Akka.Persistence.Query.InMemory;
using Akka.Streams;
using Akka.TestKit.Xunit2;
using FluentAssertions;
using Xunit;

namespace DC.Akka.PersistenceMigrator.Tests.EventMigratorTests;

public class When_migrating_same_events_twice(When_migrating_same_events_twice.Setup setup)
    : IClassFixture<When_migrating_same_events_twice.Setup>
{
    [Fact]
    public void Then_three_events_should_have_been_migrated()
    {
        setup.Results.Should().HaveCount(3);
    }

    [Fact]
    public void Then_first_event_should_be_correct()
    {
        setup.Results[0].Event.Should().Be("event-1");
    }
    
    [Fact]
    public void Then_second_event_should_be_correct()
    {
        setup.Results[1].Event.Should().Be("event-2");
    }
    
    [Fact]
    public void Then_third_event_should_be_correct()
    {
        setup.Results[2].Event.Should().Be("event-3");
    }
    
    [Fact]
    public void Then_first_event_should_have_correct_sequence_number()
    {
        setup.Results[0].SequenceNr.Should().Be(1);
    }
    
    [Fact]
    public void Then_second_event_should_have_correct_sequence_number()
    {
        setup.Results[1].SequenceNr.Should().Be(2);
    }
    
    [Fact]
    public void Then_third_event_should_have_correct_sequence_number()
    {
        setup.Results[2].SequenceNr.Should().Be(3);
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

        public IImmutableList<EventEnvelope> Results { get; private set; } = ImmutableList<EventEnvelope>.Empty;

        private readonly CancellationTokenSource _cts = new();

        public async Task InitializeAsync()
        {
            var testActor = Sys.ActorOf(Props.Create(() => new TestActor("1")));

            await testActor.Ask<TestActor.Responses.Ack>(
                new TestActor.Commands.WriteEvents("event-1", "event-2", "event-3"));
            
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

            Results = await journal
                .CurrentAllEvents(Offset.NoOffset())
                .RunAggregate(
                    ImmutableList<EventEnvelope>.Empty,
                    (list, item) => list.Add(item),
                    Sys.Materializer());
        }

        public Task DisposeAsync()
        {
            return _cts.CancelAsync();
        }
    }
}