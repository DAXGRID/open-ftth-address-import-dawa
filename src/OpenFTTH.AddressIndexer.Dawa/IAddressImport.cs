namespace OpenFTTH.AddressIndexer.Dawa;

internal interface IAddressImport
{
    Task Full(CancellationToken cancellation = default);
}
