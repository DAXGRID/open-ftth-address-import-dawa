namespace OpenFTTH.AddressIndexer.Dawa;

public interface IAddressFullImport
{
    Task Start(CancellationToken cancellation = default);
}
