#!/bin/bash

echo "Checking if odbc.ini exists in the home directory..."

current_directory=$(pwd)
ini_file_path="$HOME/.odbc.ini"

if [ ! -f "$ini_file_path" ]; then
    echo "$ini_file_path does not exist"
    exit 1
fi

echo "Found $ini_file_path"
echo "Are you sure you want to delete it? (y/n)"

read -r answer

if [ "$answer" != "${answer#[Yy]}" ]; then
    echo "Deleting $ini_file_path..."
    rm "$ini_file_path"
    echo "$ini_file_path deleted successfully!"
    exit 0
fi
