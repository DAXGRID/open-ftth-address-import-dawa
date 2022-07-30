namespace OpenFTTH.AddressIndexer.Dawa;

public interface ITransactionStore
{
    Task Init();
    Task<ulong?> LastCompleted(CancellationToken cancellationToken = default);
    Task<ulong> Newest(CancellationToken cancellationToken = default);
    Task<bool> Store(ulong transactionId);
}
