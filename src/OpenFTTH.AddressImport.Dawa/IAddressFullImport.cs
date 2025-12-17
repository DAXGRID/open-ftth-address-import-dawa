namespace OpenFTTH.AddressImport.Dawa;

public interface IAddressFullImport
{
    Task Start(DateTime timestamp, CancellationToken cancellation = default);
}
