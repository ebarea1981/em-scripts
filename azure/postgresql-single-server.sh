# Core settings
resourceGroup="EpiMethyl-Resource-1"
serverName="em-pgserver"  # Unique name with timestamp
location="eastus"
sku="GP_Gen5_2"  # 2 vCores, General Purpose
adminUser="pgadmin"  # Fallback local admin (required for initial setup)
adminPassword="MonteCarlo001*"  # Strong password (8-128 chars)

# Entra ID settings (replace with your details)
entraAdmin="ebarea@epimethyl.com"
entraObjectId=$(az ad user show --id "$entraAdmin" --query id -o tsv)  # Fetch your Entra Object ID
tenantId=$(az account show --query tenantId -o tsv)  # Your Azure tenant ID


az postgres server create \
    --resource-group "$resourceGroup" \
    --name "$serverName" \
    --location "$location" \
    --admin-user "$adminUser" \
    --admin-password "$adminPassword" \
    --sku-name "$sku" \
    --version 11 \
    --public-network-access Enabled
 

az postgres server ad-admin create \
    --resource-group "$resourceGroup" \
    --server-name "$serverName" \
    --display-name "$entraAdmin" \
    --object-id "$entraObjectId"
 

 # Allow your local IP (for initial setup)
myIp=$(curl -s ifconfig.me)
az postgres server firewall-rule create \
    --resource-group "$resourceGroup" \
    --server-name "$serverName" \
    --name AllowMyIP \
    --start-ip-address "$myIp" \
    --end-ip-address "$myIp"

# Allow Azure services (e.g., Prefect Server VM and workers)
az postgres server firewall-rule create \
    --resource-group "$resourceGroup" \
    --server-name "$serverName" \
    --name AllowAzureServices \
    --start-ip-address 0.0.0.0 \
    --end-ip-address 0.0.0.0


psql -h em-pgserver.postgres.database.azure.com -U pgadmin@em-pgserver -d postgres

```
CREATE DATABASE prefectdb;
\c prefectdb
CREATE EXTENSION pg_trgm;  # Required by Prefect

#ADDED BY EduardoBareaBermudez
CREATE EXTENSION pgcrypto;
ALTER TYPE state_type ADD VALUE IF NOT EXISTS 'CRASHED';

```

export PREFECT_API_DATABASE_CONNECTION_URL="postgresql+asyncpg://pgadmin@em-pgserver.postgres.database.azure.com:MonteCarlo001*@em-pgserver.postgres.database.azure.com:5432/prefectdb"















token=$(az account get-access-token --resource-type oss-rdbms --query accessToken -o tsv)
psql "host=$serverName.postgres.database.azure.com port=5432 dbname=postgres user=$entraAdmin@$tenantId sslmode=require password=$token"

# For Entra ID Managed Identity (like your MSSQL setup)
export PREFECT_API_DATABASE_CONNECTION_URL="postgresql+asyncpg://$serverName.postgres.database.azure.com:5432/prefectdb?sslmode=require&user=$entraAdmin@$tenantId&password=<token>"

token=$(az account get-access-token --resource-type oss-rdbms --query accessToken -o tsv)
psql --host=$serverName.postgres.database.azure.com --port=5432 --username=$entraAdmin --dbname=postgres --password=$token


# For Entra ID Managed Identity (like your MSSQL setup)
export PREFECT_API_DATABASE_CONNECTION_URL="postgresql+asyncpg://pgadmin@em-pgserver.postgres.database.azure.com:MonteCarlo001*@em-pgserver.postgres.database.azure.com:5432/prefectdb"


CREATE EXTENSION IF NOT EXISTS pgcrypto;