namespace OpenFTTH.AddressIndexer.Dawa;

public interface IAddressImport
{
    Task Full(CancellationToken cancellation = default);
    Task Changes(ulong lastTransactionId, CancellationToken cancellation = default);
}
