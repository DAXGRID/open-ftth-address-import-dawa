namespace OpenFTTH.AddressIndexer.Dawa;

public interface ITransactionStore
{
    Task<ulong?> GetLastCompletedTransactionId(CancellationToken cancellationToken = default);
    Task<ulong> GetNewestTransactionId(CancellationToken cancellationToken = default);
    Task StoreTransactionId(ulong transactionId);
}
