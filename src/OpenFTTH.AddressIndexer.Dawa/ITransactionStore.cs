namespace OpenFTTH.AddressIndexer.Dawa;

public interface ITransactionStore
{
    Task<ulong?> GetLastId();
}
