FROM mcr.microsoft.com/dotnet/sdk:6.0 AS build-env
WORKDIR /app

COPY ./*sln ./

COPY ./src/**/*.csproj ./src/OpenFTTH.AddressIndexer.Dawa/
COPY ./test/**/*.csproj ./test/OpenFTTH.AddressIndexer.Dawa.Tests/

RUN dotnet restore --packages ./packages

COPY . ./
WORKDIR /app/src/OpenFTTH.AddressIndexer.Dawa
RUN dotnet publish -c Release -o out --packages ./packages

# Build runtime image
FROM mcr.microsoft.com/dotnet/runtime:6.0
WORKDIR /app

COPY --from=build-env /app/src/OpenFTTH.AddressIndexer.Dawa/out .
ENTRYPOINT ["dotnet", "OpenFTTH.AddressIndexer.Dawa.dll"]