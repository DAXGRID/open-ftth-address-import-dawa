namespace OpenFTTH.AddressImport.Dawa;

public interface ITransactionStore
{
    Task Init();
    Task<DateTime?> LastCompleted(CancellationToken cancellationToken = default);
    Task<DateTime> Newest(CancellationToken cancellationToken = default);
    Task<bool> Store(DateTime timestamp);
}
