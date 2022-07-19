namespace OpenFTTH.AddressIndexer.Dawa;

internal interface IAddressImport
{
    Task Full(CancellationToken cancellation = default);
    Task Changes(ulong lastTransactionId, CancellationToken cancellation = default);
}
