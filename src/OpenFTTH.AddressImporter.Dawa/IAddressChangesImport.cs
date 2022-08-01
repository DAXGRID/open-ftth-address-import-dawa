namespace OpenFTTH.AddressImporter.Dawa;

public interface IAddressChangesImport
{
    Task Start(
        ulong lastTransactionId,
        ulong newestTransactionId,
        CancellationToken cancellation = default);
}
