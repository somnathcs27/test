#!/usr/bin/env sh

# Databricks cluster init (shell) script which attempts to setup msodbc drivers on clusters.
# Runs on 13.3LTS.

log() {
  # Make clear this is a script log line...
  echo '[Init Script]' $@
}

log 'attempting to download msodbc 18 driver'
curl -sSL -O https://packages.microsoft.com/ubuntu/22.04/prod/pool/main/m/msodbcsql18/msodbcsql18_18.4.1.1-1_amd64.deb

log 'attempting to install msodbc 18 driver'
sudo ACCEPT_EULA=Y dpkg -i --ignore-depends=odbcinst msodbcsql18_18.4.1.1-1_amd64.deb > /dev/null 2>&1

log 'removing .deb'
rm msodbcsql18_18.4.1.1-1_amd64.deb

log 'installing unixodbc-dev'
sudo apt-get install -y unixodbc-dev

log 'Populating odbcinst.ini'
echo \
"""[ODBC Driver 18 for SQL Server]
Driver=/usr/lib/libmsodbcsql-18.so
UsageCount=1""" > /etc/odbcinst.ini