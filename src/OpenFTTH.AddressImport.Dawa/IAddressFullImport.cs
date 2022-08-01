namespace OpenFTTH.AddressImport.Dawa;

public interface IAddressFullImport
{
    Task Start(ulong transactionId, CancellationToken cancellation = default);
}
