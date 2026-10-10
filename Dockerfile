FROM mcr.microsoft.com/dotnet/sdk:10.0-alpine AS build-env
WORKDIR /app

COPY ./*sln ./

COPY ./src/**/*.csproj ./src/OpenFTTH.AddressImport.Dawa/
COPY ./test/**/*.csproj ./test/OpenFTTH.AddressImport.Dawa.Tests/

RUN dotnet restore --packages ./packages

COPY . ./
WORKDIR /app/src/OpenFTTH.AddressImport.Dawa
RUN dotnet publish -c Release -o out --packages ./packages

# Build runtime image
FROM mcr.microsoft.com/dotnet/runtime:10.0-alpine
WORKDIR /app

RUN apk add --no-cache icu-libs krb5-libs

COPY --from=build-env --chown=app:app /app/src/OpenFTTH.AddressImport.Dawa/out .
USER app
ENTRYPOINT ["dotnet", "OpenFTTH.AddressImport.Dawa.dll"]
