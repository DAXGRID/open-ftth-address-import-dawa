using DawaAddress;
using OpenFTTH.Core.Address;

namespace OpenFTTH.AddressImport.Dawa;

internal static class DawaStatusMapper
{
    public static AccessAddressStatus MapAccessAddressStatus(DawaStatus status)
        => status switch
        {
            DawaStatus.Active => AccessAddressStatus.Active,
            DawaStatus.Canceled => AccessAddressStatus.Canceled,
            DawaStatus.Discontinued => AccessAddressStatus.Discontinued,
            DawaStatus.Pending => AccessAddressStatus.Pending,
            _ => throw new ArgumentException(
                $"{status} cannot be converted.", nameof(status))
        };

    public static UnitAddressStatus MapUnitAddressStatus(DawaStatus status)
        => status switch
        {
            DawaStatus.Active => UnitAddressStatus.Active,
            DawaStatus.Canceled => UnitAddressStatus.Canceled,
            DawaStatus.Discontinued => UnitAddressStatus.Discontinued,
            DawaStatus.Pending => UnitAddressStatus.Pending,
            _ => throw new ArgumentException(
                $"{status} cannot be converted.", nameof(status))
        };

    public static RoadStatus MapRoadStatus(DawaRoadStatus status)
        => status switch
        {
            DawaRoadStatus.Effective => RoadStatus.Effective,
            DawaRoadStatus.Temporary => RoadStatus.Temporary,
            _ => throw new ArgumentException(
                $"{status} cannot be converted.", nameof(status))
        };
}
