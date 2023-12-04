import os
import configparser

# Fetch DRIVER_PATH from the environment
DRIVER_PATH = os.environ.get("DRIVER_PATH")

if not DRIVER_PATH:
    raise ValueError("Environment variable DRIVER_PATH is not set.")

# The path to the .odbcinst.ini file
config_file_path = os.path.expanduser("~/.odbcinst.ini")

# Create a ConfigParser object and read the existing .odbcinst.ini file
config = configparser.ConfigParser()
config.read(config_file_path)

# Update the 'DuckDB Driver' section with the new DRIVER_PATH
if "DuckDB Driver" in config:
    config["DuckDB Driver"]["Driver"] = DRIVER_PATH
else:
    config.add_section("DuckDB Driver")
    config["DuckDB Driver"]["Driver"] = DRIVER_PATH

# Write the modified configuration back to the .odbcinst.ini file
with open(config_file_path, "w") as configfile:
    config.write(configfile)
