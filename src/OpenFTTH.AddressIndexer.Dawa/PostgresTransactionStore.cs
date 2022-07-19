namespace OpenFTTH.AddressIndexer.Dawa;

internal class PostgresTransactionStore : ITransactionStore
{
    public async Task<ulong?> GetLastId()
    {
        return await Task.FromResult<ulong?>(null).ConfigureAwait(false);
    }
}
