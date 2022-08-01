namespace OpenFTTH.AddressImport.Dawa;

public interface IAddressChangesImport
{
    Task Start(
        ulong lastTransactionId,
        ulong newestTransactionId,
        CancellationToken cancellation = default);
}
