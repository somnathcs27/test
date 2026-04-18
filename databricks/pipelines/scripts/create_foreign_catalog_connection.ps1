param([String]$repository_path, [String]$environment_shortname, [String]$package_directory, [String]$common_libs_version, [String]$oauth_tenant_id, [String]$oauth_client_id, [String]$oauth_client_secret, [String]$db_host, [String]$db_name)

$auth_endpoint = "$($Env:DATABRICKS_HOST)/oidc/v1/token"
$postParams = @{
    client_id="$($Env:DATABRICKS_CLIENT_ID)";
    client_secret="$($Env:DATABRICKS_CLIENT_SECRET)";
    grant_type="client_credentials";
    scope="all-apis"
}
$res = Invoke-WebRequest -Uri $auth_endpoint -Method POST -Body $postParams -UseBasicParsing

if([int]$res.StatusCode -ne 200) {
    throw [System.Web.HttpUnhandledException] "Unable to obtain bearer token from Databricks."
}

$bearer_token = ($res | ConvertFrom-Json).access_token

$headers = @{
    Authorization="Bearer $($bearer_token)";
    "Content-Type"="application/json";
}

# Create catalog connection
Write-Host "Creating catalog connection to MDP Workflow Control Database..."

$api_url = "$($Env:DATABRICKS_HOST)/api/2.1/unity-catalog/connections";

$body = @{
    name="MDP_Workflow_Control_Connection";
    type="sql_server";
    connection_type="SQLSERVER";
    read_only=$false;
    options=@{
        host=$db_host;
        port="1433";
        database=$db_name;
        authentication="OAuth";
        client_id=$oauth_client_id;
        client_secret=$oauth_client_secret;
        tenant_id=$oauth_tenant_id;
        authorization_endpoint="https://login.microsoftonline.com/$($tenant_id)/oauth2/v2.0/authorize";
        resource="https://database.windows.net/";
        oauth_scope="https://database.windows.net/.default offline_access";
    };
    comment="Connection to MDP Workflow Control Database";
} | ConvertTo-Json

$res = Invoke-WebRequest -Uri $api_url -Method POST -Body $body -Headers $headers -UseBasicParsing | ConvertFrom-Json

if([int]$res.StatusCode -ne 200) {
    throw [System.Web.HttpUnhandledException] "Unable to obtain bearer token from Databricks."
}

Write-Host "Catalog Connection Created."

# Create catalog

Write-Host "Creating foreign catalog for MDP Workflow Control Database..."

$api_url = "$($Env:DATABRICKS_HOST)/api/2.1/unity-catalog/catalogs";

$body = @{
    name="MDP_Workflow_Control_Catalog";
    type="foreign";
    connection_name="MDP_Workflow_Control_Connection";
    comment="MDP Workflow Control Database Foreign Catalog";
}

$res = Invoke-WebRequest -Uri $api_url -Method POST -Body $body -Headers $headers -UseBasicParsing | ConvertFrom-Json

if([int]$res.StatusCode -ne 200) {
    throw [System.Web.HttpUnhandledException] "Unable to obtain bearer token from Databricks."
}

Write-Host "Foreign Catalog Created."