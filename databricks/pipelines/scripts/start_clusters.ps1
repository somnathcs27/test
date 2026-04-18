# Run schema maintenance job
param([String]$cluster_id)

$failure_states = @("ERROR", "UNKNOWN")

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

# Get state
$api_url = "$($Env:DATABRICKS_HOST)/api/2.1/clusters/get?cluster_id=$($cluster_id)"
$headers = @{
    Authorization = "Bearer $($bearer_token)";
}
$cluster = Invoke-WebRequest -Uri $api_url -Method GET -Headers $headers -UseBasicParsing | ConvertFrom-Json
$cluster_state = $cluster.state
Write-Output "Current cluster state $($cluster_state)";

# Start the cluster
if($cluster_state -ne "RUNNING" -and $cluster_state -ne "PENDING") {
    Write-Output "Starting cluster $($cluster_name)..."
    # Start cluster
    $api_url = "$($Env:DATABRICKS_HOST)/api/2.1/clusters/start"
    $headers = @{
        Authorization = "Bearer $($bearer_token)";
    }
    $body = @{
        cluster_id=$cluster_id;
    } | ConvertTo-Json
    $cluster = Invoke-WebRequest -Uri $api_url -Method POST -Headers $headers -Body $body -UseBasicParsing | ConvertFrom-Json
}

# Wait for it to report running
while($cluster_state -ne "RUNNING") {
    $api_url = "$($Env:DATABRICKS_HOST)/api/2.1/clusters/get?cluster_id=$($cluster_id)"
    $headers = @{
        Authorization = "Bearer $($bearer_token)";
    }
    $cluster = Invoke-WebRequest -Uri $api_url -Method GET -Headers $headers -UseBasicParsing | ConvertFrom-Json
    $cluster_state = $cluster.state
    Write-Output "Cluster state: $($cluster_state)"

    if($failure_states.Contains($cluster_state)) {
        throw [System.Exception] "Unable to start cluster. Current cluster state = $($cluster_state)."
    }

    if($cluster_state -ne "RUNNING") {
        Start-Sleep -Seconds 30
    }
}
Write-Output "Started cluster $($cluster_name)."