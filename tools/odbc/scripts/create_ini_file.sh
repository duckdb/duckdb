#!/bin/bash

echo "Creating odbc.ini in the home directory..."

current_directory=$(pwd)
ini_file_path="$HOME/.odbc.ini"

# Create a simple odbc.ini file with some content
echo "[DuckDB]" > "$ini_file_path"
echo "Driver = DuckDB Driver" >> "$ini_file_path"
echo "database = $current_directory/test/sql/storage_version/storage_version.db" >> "$ini_file_path"
echo "access_mode = read_only" >> "$ini_file_path"
echo "allow_unsigned_extensions = true" >> "$ini_file_path"

if [ ! -f "$ini_file_path" ]; then
    echo "Failed to create $ini_file_path"
    exit 1
fi

cat "$ini_file_path"
echo "$ini_file_path created successfully!"
