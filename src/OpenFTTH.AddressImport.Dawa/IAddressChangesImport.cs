namespace OpenFTTH.AddressImport.Dawa;

public interface IAddressChangesImport
{
    Task Start(
        DateTime fromTimeStamp,
        DateTime toTimeStamp,
        CancellationToken cancellation = default);
}
