FROM mcr.microsoft.com/dotnet/sdk:6.0 AS build-env
WORKDIR /app

COPY ./*sln ./

COPY ./src/**/*.csproj ./src/OpenFTTH.AddressImport.Dawa/
COPY ./test/**/*.csproj ./test/OpenFTTH.AddressImport.Dawa.Tests/

RUN dotnet restore --packages ./packages

COPY . ./
WORKDIR /app/src/OpenFTTH.AddressImport.Dawa
RUN dotnet publish -c Release -o out --packages ./packages

# Build runtime image
FROM mcr.microsoft.com/dotnet/runtime:6.0
WORKDIR /app

COPY --from=build-env /app/src/OpenFTTH.AddressImport.Dawa/out .
ENTRYPOINT ["dotnet", "OpenFTTH.AddressImport.Dawa.dll"]