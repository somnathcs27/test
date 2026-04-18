# Tries to upload a given init script to the init_scripts volume in common_libraries schema.
param([String]$environment_shortname, [String]$package_directory, [String]$init_script_name, [String]$target_volume = 'default')

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

# Look for all .sh files, and generate folder names for upload
$sh_files = Get-ChildItem -Recurse -Path $package_directory -Filter *.sh -File

if ($sh_files.Count -eq 0){
    throw "FATAL: No sh files found in the downloaded artifact."
}

foreach ($sh_file in $sh_files){
    $filename = $sh_file.Name
    Write-Output "Found .sh file: $filename"

    if ($filename -eq $init_script_name) {

        # Look for the schema. If it doesn't exist, create it.
        $api_url = "$($Env:DATABRICKS_HOST)/api/2.1/unity-catalog/schemas?catalog_name=$($environment_shortname)_catalog"
        $headers = @{
            Authorization="Bearer $($bearer_token)"
        }
        $schemas = Invoke-WebRequest -Uri $api_url -Method GET -Headers $headers -UseBasicParsing | ConvertFrom-Json

        $schema_exists = $false;
        foreach($schema in $schemas.schemas) {
            if ($schema.name -eq "init_scripts") {
                $schema_exists = $true;
            }
        }

        if ($schema_exists -eq $true) {
            Write-Output "Found schema init_scripts."
        } else {
            # Create it
            Write-Output "Creating schema init_scripts..."
            $api_url = "$($Env:DATABRICKS_HOST)/api/2.1/unity-catalog/schemas"
            $headers = @{
                Authorization="Bearer $($bearer_token)"
            }
            $body = @{
                catalog_name="$($environment_shortname)_catalog";
                name="init_scripts";
            } | ConvertTo-Json
            $schemas = Invoke-WebRequest -Uri $api_url -Method POST -Body $body -Headers $headers -UseBasicParsing | ConvertFrom-Json
            Write-Output "Created schema init_scripts."
        }

        # Look for the volume. If don't find it, create it.
        Write-Output "Looking for existing volumes on DBFS..."
        $api_url = "$($Env:DATABRICKS_HOST)/api/2.1/unity-catalog/volumes?catalog_name=$($environment_shortname)_catalog&schema_name=init_scripts"
        Write-Output $api_url
        $headers = @{
            Authorization="Bearer $($bearer_token)"
        }

        $volumes = Invoke-WebRequest -Uri $api_url -Method GET -Headers $headers -UseBasicParsing

        $volumes = $volumes | ConvertFrom-Json

        Write-Output "Volumes currently in init_scripts schema:"
        foreach($volume in $volumes.volumes) {
            Write-Output "- $($volume.name)"
        }

        $check_volume_exists = $false
        foreach($volume in $volumes.volumes) {
            if($volume.name -eq $target_volume) {
                Write-Output "Volume $($target_volume) exists."
                $check_volume_exists = $true;
                break;
            }
        }

        if($check_volume_exists -eq $true){
            # Found target volume. Don't try and create it.
            Write-Output "Found volume: $target_volume"
        }
        else{
            Write-Output "Didn't find volume: $target_volume; attempt to create it..."

            $api_url = "$($Env:DATABRICKS_HOST)/api/2.1/unity-catalog/volumes"
            $headers = @{
                Authorization="Bearer $($bearer_token)"
            }
            $body = @{
                catalog_name = "$($environment_shortname)_catalog";
                schema_name = "init_scripts";
                name = $target_volume;
                volume_type = "MANAGED";
            } | ConvertTo-Json
            $create_volume_command_response = Invoke-WebRequest -Uri $api_url -Method POST -Body $body -Headers $headers -UseBasicParsing | ConvertFrom-Json
            Write-Output "Volume create command response (see error embedded in this string if the volume didn't create properly): $create_volume_command_response"
        }

        # If got here without exception - the volume should exist.. proceed to upload.
        $unity_catalog_target_path = "/Volumes/$($environment_shortname)_catalog/init_scripts/$($target_volume)/$($filename)"
        Write-Output $unity_catalog_target_path

        # This is in public preview but it's the only way it works
        $api_url = "$($Env:DATABRICKS_HOST)/api/2.0/fs/files$($unity_catalog_target_path)?overwrite=true"
        $headers = @{
            Authorization="Bearer $($bearer_token)"
        }
        $body = [IO.File]::ReadAllBytes($sh_file.FullName)
        $stream = Invoke-WebRequest -Uri $api_url -Method PUT -Body $body -Headers $headers -UseBasicParsing | ConvertFrom-Json

        # Install on cluster

        # Assign to cluster
        # FYI documentation of the clusters API doesn't list:
        #   spark_version
        #   num_workers
        #   data_security_mode
        #   node_type_id
        #   driver_node_type_id
        # as required. But they are - this appears to be the minimal set to satisfy
        # the edit endpoint at time of writing. Nothing in release notes about clusters api,
        # but there is a version bump to 2.1.
        # COMMENTED: appears to scrub cluster config
        # $api_url = "$($Env:DATABRICKS_HOST)/api/2.0/clusters/edit"
        # $headers = @{
        #     Authorization="Bearer $($bearer_token)"
        # }
        # $body = @{
        #     cluster_id = $cluster_id
        #     num_workers = 1;
        #     spark_version = "13.3.x-scala2.12";
        #     data_security_mode = "USER_ISOLATION";
        #     node_type_id = "Standard_DS3_v2";
        #     driver_node_type_id = "Standard_DS3_v2";
        #     init_scripts = @(
        #         @{
        #             volumes = @{
        #                 destination = $unity_catalog_target_path
        #             }
        #         }
        #     )
        # } | ConvertTo-Json -Depth 3
        # Write-Output "cluster/edit endpoint body: $($body)"
        # $install = Invoke-WebRequest -Uri $api_url -Method POST -Body $body -Headers $headers -UseBasicParsing | ConvertFrom-Json
    } else {
        Write-Output " Found $($filename), but ignoring (targeting $($init_script_name))."
    }
}
