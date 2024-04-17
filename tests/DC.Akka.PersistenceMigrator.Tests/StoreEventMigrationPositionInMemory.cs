using Akka.Persistence.Query;

namespace DC.Akka.PersistenceMigrator.Tests;

public class StoreEventMigrationPositionInMemory : IStoreEventMigrationPosition
{
    private Offset _currentOffset = Offset.NoOffset();
    
    public Task<Offset> LoadLatestOffset(CancellationToken cancellationToken = default)
    {
        return Task.FromResult(_currentOffset);
    }

    public Task SaveLatestOffset(Offset offset, CancellationToken cancellationToken = default)
    {
        _currentOffset = offset;
        
        return Task.CompletedTask;
    }
}