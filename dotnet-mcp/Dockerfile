# dotnet-mcp：多阶段构建（sdk 编译 → aspnet 运行时）
FROM mcr.microsoft.com/dotnet/sdk:10.0 AS build
WORKDIR /src
COPY src/StockData.Mcp/StockData.Mcp.csproj src/StockData.Mcp/
RUN dotnet restore src/StockData.Mcp
COPY src/ src/
RUN dotnet publish src/StockData.Mcp -c Release -o /app --no-restore

FROM mcr.microsoft.com/dotnet/aspnet:10.0
ENV TZ=Asia/Shanghai
WORKDIR /app
COPY --from=build /app .
# StockData:ApiBase 由 compose 环境变量 StockData__ApiBase 覆盖
ENTRYPOINT ["dotnet", "StockData.Mcp.dll"]
