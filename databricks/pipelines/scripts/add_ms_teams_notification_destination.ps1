param(
    [parameter(Mandatory = $true)] [String] $DisplayName,
    [parameter(Mandatory = $true)] [String] $WebhookUrl
)

Write-Host "Adding MS Teams notification destination..."

$exists = $false;

# Get Bearer Token
try {
    $auth_endpoint = "$($Env:DATABRICKS_HOST)/oidc/v1/token"
    $postParams = @{
        client_id="$($Env:DATABRICKS_CLIENT_ID)";
        client_secret="$($Env:DATABRICKS_CLIENT_SECRET)";
        grant_type="client_credentials";
        scope="all-apis";
    }
    $res = Invoke-WebRequest -Uri $auth_endpoint -Method POST -Body $postParams -UseBasicParsing

    if([int]$res.StatusCode -ne 200) {
        throw "Failure obtaining bearer token: $($response.StatusCode)";
    }

    $bearerToken = ($res | ConvertFrom-Json).access_token;
} catch {
    Write-Error "An error occurred: $($_)";
}

# Set auth headers variable
$headers = @{
    Authorization="Bearer $($bearerToken)";
};

# List existing notification destinations, check if what we're creating already exists
try {
    $uri = "$($Env:DATABRICKS_HOST)/api/2.0/notification-destinations";
    $res = Invoke-WebRequest -Uri $uri -Method GET -Headers $headers -UseBasicParsing
    
    if([int]$res.StatusCode -ne 200) {
        throw "Unable to list notification destinations. $($res.StatusCode)";
    }

    foreach($nd in ($res | ConvertFrom-Json).results) {
        if($nd.display_name -eq $DisplayName) {
            $exists = $true;
            Write-Host "Notification Destination $($DisplayName) already exists."
            Write-Host "##vso[task.setvariable variable=databricks_job_failure_notification_destination_id;]$($nd.id)"
            break;
        }
    }    
} catch {
    Write-Error "An error occurred: $($_)";
}

# Create the notification destination
if($exists -eq $false) {
    try{
        $uri = "$($Env:DATABRICKS_HOST)/api/2.0/notification-destinations";
        $body = @{
            display_name=$DisplayName;
            config=@{
                microsoft_teams=@{
                    url=$WebhookUrl;
                }
            }
        };

        $res = Invoke-WebRequest -Uri $uri -Method POST -Headers $headers -Body ($body | ConvertTo-Json -Depth 10) -UseBasicParsing

        if([int]$res.StatusCode -ne 200) {
            throw "Failure creating notification destination. $($res.StatusCode)";
        }

        $destinationId = ($res | ConvertFrom-Json).id;

        Write-Host "##vso[task.setvariable variable=databricks_job_failure_notification_destination_id;]$($destinationId)"
    } catch {
        Write-Error "An error occurred: $($_)";
    }
    
    Write-Host "Added Notification Destination '$($DisplayName)'.";
}
