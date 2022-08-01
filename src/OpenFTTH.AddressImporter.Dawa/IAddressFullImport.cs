namespace OpenFTTH.AddressImporter.Dawa;

public interface IAddressFullImport
{
    Task Start(ulong transactionId, CancellationToken cancellation = default);
}
