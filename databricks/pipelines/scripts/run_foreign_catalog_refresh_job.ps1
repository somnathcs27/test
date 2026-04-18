# Run schema maintenance job
param([String]$repository_path, [String]$environment_shortname)

$auth_endpoint = "$($Env:DATABRICKS_HOST)/oidc/v1/token"
$postParams = @{
    client_id="$($Env:DATABRICKS_CLIENT_ID)";
    client_secret="$($Env:DATABRICKS_CLIENT_SECRET)";
    grant_type="client_credentials";
    scope="all-apis"
}
$job_name = "$($environment_shortname)_run_foreign_catalog_refresh_job"

$res = Invoke-WebRequest -Uri $auth_endpoint -Method POST -Body $postParams -UseBasicParsing

if([int]$res.StatusCode -ne 200) {
    throw [System.Web.HttpUnhandledException] "Unable to obtain bearer token from Databricks."
}

$bearer_token = ($res | ConvertFrom-Json).access_token

Write-Output "DATABRICKS_HOST: $Env:DATABRICKS_HOST"
$api_url = "$($Env:DATABRICKS_HOST)/api/2.1/jobs/list?name=$($job_name)"
$headers = @{
    Authorization="Bearer $($bearer_token)"
}

$jobs = Invoke-WebRequest -Uri $api_url -Method GET -Headers $headers -UseBasicParsing | ConvertFrom-Json

$job_id = ""
foreach($job in $jobs.jobs) {
    if($job.settings.name -eq $job_name) {
        $job_id = $job.job_id;
        break;
    }
}

if ($job_id -eq ""){
    throw "Job search object is null. Cannot find job in databricks workspace"
}

# Submit job...

$json_job_config = @{
    job_id = $job_id;
    job_parameters = @{
        env_var = $environment_shortname;
    };
} | ConvertTo-Json

Write-Output "json_job_config: $json_job_config"

$api_url = "$($Env:DATABRICKS_HOST)/api/2.1/jobs/run-now"
$headers = @{
    Authorization="Bearer $($bearer_token)"
}

$cli_rtn = Invoke-WebRequest -Uri $api_url -Method POST -Body $json_job_config -Headers $headers -UseBasicParsing | ConvertFrom-Json

Write-Output "value of databricks cli return obj: $cli_rtn"
