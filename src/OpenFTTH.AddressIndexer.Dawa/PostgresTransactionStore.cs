namespace OpenFTTH.AddressIndexer.Dawa;

internal class PostgresTransactionStore : ITransactionStore
{
    public Task<ulong?> GetLastCompletedTransactionId(
        CancellationToken cancellationToken = default)
    {
        throw new NotImplementedException();
    }

    public Task<ulong> GetNewestTransactionId(
        CancellationToken cancellationToken = default)
    {
        throw new NotImplementedException();
    }

    public Task StoreTransactionId(ulong transactionId)
    {
        throw new NotImplementedException();
    }
}
