namespace OpenFTTH.AddressImport.Dawa;

public interface ITransactionStore
{
    Task Init(bool enableMigration);
    Task<DateTime?> LastCompletedUtc(CancellationToken cancellationToken = default);
    Task<DateTime> NewestUtc(CancellationToken cancellationToken = default);
    Task<bool> Store(DateTime timestamp);
}
