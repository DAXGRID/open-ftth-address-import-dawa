namespace OpenFTTH.AddressIndexer.Dawa;

internal static class TransactionStore
{
    public static Task<ulong?> GetLastRunTransactionId()
    {
        return Task.FromResult<ulong?>(null);
    }
}
