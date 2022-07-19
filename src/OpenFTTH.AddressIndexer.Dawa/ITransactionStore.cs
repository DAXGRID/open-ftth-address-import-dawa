namespace OpenFTTH.AddressIndexer.Dawa;

internal interface ITransactionStore
{
    Task<ulong?> GetLastId();
}
