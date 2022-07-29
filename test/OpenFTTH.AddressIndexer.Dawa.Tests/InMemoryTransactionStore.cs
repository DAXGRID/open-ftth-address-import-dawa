namespace OpenFTTH.AddressIndexer.Dawa.Tests;

internal class InMemoryTransactionStore : ITransactionStore
{
    private readonly List<ulong> _transactions = new();

    public Task<ulong?> GetLastCompletedTransactionId(
        CancellationToken cancellationToken = default)
    {
        return Task.FromResult(_transactions.Count > 0 ? (ulong?)_transactions.First() : null);
    }

    public Task StoreTransactionId(ulong transactionId)
    {
        _transactions.Add(transactionId);
        return Task.CompletedTask;
    }

    // We just use a specific transaction id to simplify it.
    public Task<ulong> GetNewestTransactionId(CancellationToken cancellationToken = default)
    {
        return Task.FromResult<ulong>(3905212UL - 20000);
    }
}
