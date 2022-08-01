FROM mcr.microsoft.com/dotnet/sdk:6.0 AS build-env
WORKDIR /app

COPY ./*sln ./

COPY ./src/**/*.csproj ./src/OpenFTTH.AddressImporter.Dawa/
COPY ./test/**/*.csproj ./test/OpenFTTH.AddressImporter.Dawa.Tests/

RUN dotnet restore --packages ./packages

COPY . ./
WORKDIR /app/src/OpenFTTH.AddressImporter.Dawa
RUN dotnet publish -c Release -o out --packages ./packages

# Build runtime image
FROM mcr.microsoft.com/dotnet/runtime:6.0
WORKDIR /app

COPY --from=build-env /app/src/OpenFTTH.AddressImporter.Dawa/out .
ENTRYPOINT ["dotnet", "OpenFTTH.AddressImporter.Dawa.dll"]