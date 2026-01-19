namespace OpenFTTH.AddressImport.Dawa;

public interface IAddressFullImport
{
    Task<DateTime> Start(CancellationToken cancellation = default);
}
