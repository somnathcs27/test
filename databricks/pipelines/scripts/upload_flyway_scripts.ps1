# upload flyway scripts
param([String]$environment_shortname, [String]$scripts_directory, [String]$uc_schema_name, [String]$uc_volume_name)

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


# ****************************************************************************************
# Look for the schema migration schema ($uc_schema_name). If it doesn't exist, create it.
# ****************************************************************************************
$api_url = "$($Env:DATABRICKS_HOST)/api/2.1/unity-catalog/schemas?catalog_name=$($environment_shortname)_catalog"
$headers = @{
    Authorization="Bearer $($bearer_token)"
}
$schemas = Invoke-WebRequest -Uri $api_url -Method GET -Headers $headers -UseBasicParsing | ConvertFrom-Json

$schema_exists = $false;
foreach($schema in $schemas.schemas) {
    if ($schema.name -eq $uc_schema_name) {
        $schema_exists = $true;
    }
}

if ($schema_exists -eq $true) {
    Write-Output "Found schema $($uc_schema_name)"
} else {
    Write-Output "Creating schema $($uc_schema_name)..."
    
    $api_url = "$($Env:DATABRICKS_HOST)/api/2.1/unity-catalog/schemas"
    
    $headers = @{
        Authorization="Bearer $($bearer_token)"
    }
    
    $body = @{
        catalog_name="$($environment_shortname)_catalog";
        name=$uc_schema_name;
    } | ConvertTo-Json
    
    $schemas = Invoke-WebRequest -Uri $api_url -Method POST -Body $body -Headers $headers -UseBasicParsing | ConvertFrom-Json
    
    Write-Output "Created schema $($uc_schema_name)"
}


# ****************************************************************************************
# Look for the $uc_volume_name volume. If don't find it, create it.
# ****************************************************************************************
Write-Output "Looking for existing unity-catalog volume matching $($uc_volume_name) ..."
$api_url = "$($Env:DATABRICKS_HOST)/api/2.1/unity-catalog/volumes?catalog_name=$($environment_shortname)_catalog&schema_name=$uc_schema_name"
Write-Output $api_url
$headers = @{
    Authorization="Bearer $($bearer_token)"
}

$volumes = Invoke-WebRequest -Uri $api_url -Method GET -Headers $headers -UseBasicParsing

$volumes = $volumes | ConvertFrom-Json

Write-Output "Volumes currently in $($uc_schema_name) schema:"
foreach($volume in $volumes.volumes) {
    Write-Output "- $($volume.name)"
}

$check_volume_exists = $false

foreach($volume in $volumes.volumes) {
    if($volume.name -eq $uc_volume_name) {
        Write-Output "Volume $($uc_volume_name) exists."
        $check_volume_exists = $true;
        break;
    }
}

if($check_volume_exists -eq $true){
    # Found target volume. Don't try and create it.
    Write-Output "Found volume: $($uc_volume_name)"
}
else{
    Write-Output "Didn't find volume: $($uc_volume_name); attempt to create it..."

    $api_url = "$($Env:DATABRICKS_HOST)/api/2.1/unity-catalog/volumes"
    
    $headers = @{
        Authorization="Bearer $($bearer_token)"
    }
    
    $body = @{
        catalog_name = "$($environment_shortname)_catalog";
        schema_name = $uc_schema_name;
        name = $uc_volume_name;
        volume_type = "MANAGED";
    } | ConvertTo-Json
    
    $create_volume_command_response = Invoke-WebRequest -Uri $api_url -Method POST -Body $body -Headers $headers -UseBasicParsing | ConvertFrom-Json
    
    Write-Output "Volume create command response (see error embedded in this string if the volume didn't create properly): $create_volume_command_response"
}


# ****************************************************************************************
# Look for all script files, and upload them to the volume
# ****************************************************************************************

$script_sql_files = Get-ChildItem -Path $scripts_directory -Filter *.sql -File

if ($script_sql_files.Count -eq 0){
    Write-Output "Informational: No SQL script files found in the downloaded artifact."
}

foreach ($sql_file in $script_sql_files){
    $filename = $sql_file.Name
    Write-Output "Found .sql file: $($filename)"

    # If got here without exception - the volume should exist.. proceed to upload.
    $unity_catalog_target_path = "/Volumes/$($environment_shortname)_catalog/$($uc_schema_name)/$($uc_volume_name)/$($filename)"
    Write-Output "Uploading: $($unity_catalog_target_path)"

    # This is in public preview but it's the only way it works
    $api_url = "$($Env:DATABRICKS_HOST)/api/2.0/fs/files$($unity_catalog_target_path)?overwrite=true"
    $headers = @{
        Authorization="Bearer $($bearer_token)"
    }
    $body = [IO.File]::ReadAllBytes($sql_file.FullName)
    $web_req_output = Invoke-WebRequest -Uri $api_url -Method PUT -Body $body -Headers $headers -UseBasicParsing | ConvertFrom-Json
}
