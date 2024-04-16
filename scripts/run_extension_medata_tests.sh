#!/bin/bash

# Please consider your energy footprint by only running this script with ccache.
# note that subsequent runs used cached artifacts, use `make clean` or rm -rf build/debug to clean

set -x
set -e

DUCKDB_BUILD_DIR="./build/debug"

TEST_DIR="./build/extension_metadata_test_data"
TEST_DIR_COPY="./build/extension_metadata_test_data_copy"

### Directories to use
# Used as the extension installation directory for DuckDB
export LOCAL_EXTENSION_DIR="$TEST_DIR/extension_dir"
# Repository for testing successfully updating extensions
export LOCAL_EXTENSION_REPO_UPDATED="$TEST_DIR/repository"
# Repository for testing incorrect platform
export LOCAL_EXTENSION_REPO_INCORRECT_PLATFORM="$TEST_DIR/repository_incorrect_platform"
# Repository for testing incorrect version
export LOCAL_EXTENSION_REPO_INCORRECT_DUCKDB_VERSION="$TEST_DIR/repository_incorrect_version"
# Repository where both platform and version mismatch
export LOCAL_EXTENSION_REPO_VERSION_AND_PLATFORM_INCORRECT="$TEST_DIR/repository_incorrect_version_and_platform"
# Directory containing the extensions for direct installing
export DIRECT_INSTALL_DIR="$TEST_DIR/direct_install"

if [ -d "$TEST_DIR_COPY" ]; then
  # REUSE PREVIOUSLY GENERATED DATA
  rm -r $TEST_DIR
  cp -R $TEST_DIR_COPY $TEST_DIR
else
  # GENERATE FRESH DATA
  mkdir -p $TEST_DIR
  mkdir -p $DIRECT_INSTALL_DIR
  mkdir -p $LOCAL_EXTENSION_DIR
  mkdir -p $LOCAL_EXTENSION_REPO_UPDATED
  mkdir -p $LOCAL_EXTENSION_REPO_INCORRECT_PLATFORM
  mkdir -p $LOCAL_EXTENSION_REPO_INCORRECT_DUCKDB_VERSION

  #################################################
  ### First repo: successfully updating extensions.
  #################################################
  # Set extension config
  cat > $TEST_DIR/extension_config_before.cmake <<EOL
  duckdb_extension_load(json DONT_LINK EXTENSION_VERSION v0.0.1)
  duckdb_extension_load(tpch DONT_LINK EXTENSION_VERSION v0.0.1)
  duckdb_extension_load(tpcds DONT_LINK EXTENSION_VERSION v0.0.1)
EOL

  # Build the extensions using the first config
  LOCAL_EXTENSION_REPO=$LOCAL_EXTENSION_REPO_UPDATED EXTENSION_CONFIGS=$TEST_DIR/extension_config_before.cmake make debug

  # Install the extension from the initial config
  $DUCKDB_BUILD_DIR/duckdb -unsigned -c "set extension_directory='$LOCAL_EXTENSION_DIR'; set custom_extension_repository='$LOCAL_EXTENSION_REPO_UPDATED'; install tpch; install json;"

  # Install tpcds directly
  cp $DUCKDB_BUILD_DIR/extension/tpcds/tpcds.duckdb_extension $DIRECT_INSTALL_DIR/tpcds.duckdb_extension
  $DUCKDB_BUILD_DIR/duckdb -unsigned -c "set extension_directory='$LOCAL_EXTENSION_DIR'; install '$DIRECT_INSTALL_DIR/tpcds.duckdb_extension';"

  # Set updated extension config where we update the tpch extension but not the json extension
  cat > $TEST_DIR/extension_config_after.cmake <<EOL
  duckdb_extension_load(json DONT_LINK EXTENSION_VERSION v0.0.1)
  duckdb_extension_load(tpch DONT_LINK EXTENSION_VERSION v0.0.2)
EOL

  # Build the extensions using the second config
  LOCAL_EXTENSION_REPO=$LOCAL_EXTENSION_REPO_UPDATED EXTENSION_CONFIGS=$TEST_DIR/extension_config_after.cmake BUILD_EXTENSIONS_ONLY=1 make debug

  ##########################################
  ### Second repo: Incorrect DuckDB platform
  ##########################################
  rm -rf $DUCKDB_BUILD_DIR
  # Set extension config
  cat > $TEST_DIR/extension_config_incorrect_platform.cmake <<EOL
  duckdb_extension_load(json DONT_LINK EXTENSION_VERSION v0.0.3)
EOL

  # Build the extensions using the incorrect platform
  DUCKDB_PLATFORM=test_platform EXTENSION_CONFIGS=$TEST_DIR/extension_config_incorrect_platform.cmake BUILD_EXTENSIONS_ONLY=1 make debug

  cp $DUCKDB_BUILD_DIR/extension/json/json.duckdb_extension $DIRECT_INSTALL_DIR/json_incorrect_platform.duckdb_extension

  ########################################
  ### Third repo: Incorrect DuckDB version
  ########################################
  rm -rf $DUCKDB_BUILD_DIR
  # Set extension config
  cat > $TEST_DIR/extension_config_incorrect_version.cmake <<EOL
  duckdb_extension_load(json DONT_LINK EXTENSION_VERSION v0.0.4)
EOL

  # Build the extensions using the incorrect platform
  EXTRA_CMAKE_VARIABLES=-DDUCKDB_EXPLICIT_VERSION=v1337 EXTENSION_CONFIGS=$TEST_DIR/extension_config_before.cmake BUILD_EXTENSIONS_ONLY=1 make debug

  cp $DUCKDB_BUILD_DIR/extension/json/json.duckdb_extension $DIRECT_INSTALL_DIR/json_incorrect_version.duckdb_extension

  ####################################################
  ### Fourth repo: Both platform and version incorrect
  ####################################################
  rm -rf $DUCKDB_BUILD_DIR
  # Set extension config
  cat > $TEST_DIR/extension_config_incorrect_version.cmake <<EOL
  duckdb_extension_load(json DONT_LINK EXTENSION_VERSION v0.0.4)
EOL

  # Build the extensions using the incorrect platform
  DUCKDB_PLATFORM=test_platform EXTRA_CMAKE_VARIABLES=-DDUCKDB_EXPLICIT_VERSION=v1337 EXTENSION_CONFIGS=$TEST_DIR/extension_config_before.cmake BUILD_EXTENSIONS_ONLY=1 make debug

  cp $DUCKDB_BUILD_DIR/extension/json/json.duckdb_extension $DIRECT_INSTALL_DIR/json_incorrect_version_and_platform.duckdb_extension

  # Note that we set the "double wrong" extension to have the proper name, so we can actually load it during testing with
  # SET allow_extensions_metadata_mismatch=true;
  cp $DUCKDB_BUILD_DIR/extension/json/json.duckdb_extension $DIRECT_INSTALL_DIR/json.duckdb_extension

  ###########################
  ### Prepare malformed repos
  ###########################
  # Build clean duckdb
  rm -rf $DUCKDB_BUILD_DIR
  make debug

  # Use duckdb to install the extensions into the repositories (note that we are doing a trick here by setting the extension_directory to the local repo dir)
  $DUCKDB_BUILD_DIR/duckdb -unsigned -c "set allow_extensions_metadata_mismatch=true; set extension_directory='$LOCAL_EXTENSION_REPO_INCORRECT_PLATFORM'; install '$DIRECT_INSTALL_DIR/json_incorrect_platform.duckdb_extension'"
  $DUCKDB_BUILD_DIR/duckdb -unsigned -c "set allow_extensions_metadata_mismatch=true; set extension_directory='$LOCAL_EXTENSION_REPO_INCORRECT_DUCKDB_VERSION'; install '$DIRECT_INSTALL_DIR/json_incorrect_version.duckdb_extension'"
  $DUCKDB_BUILD_DIR/duckdb -unsigned -c "set allow_extensions_metadata_mismatch=true; set extension_directory='$LOCAL_EXTENSION_REPO_VERSION_AND_PLATFORM_INCORRECT'; install '$DIRECT_INSTALL_DIR/json_incorrect_version_and_platform.duckdb_extension'"

  ###################################################################
  ### Allow using copy instead of regenerating test data on every run
  ###################################################################
  cp -R $TEST_DIR $TEST_DIR_COPY
fi

################
### Run test
################
RUN_EXTENSION_UPDATE_TEST=1 $DUCKDB_BUILD_DIR/test/unittest test/extension/update_extensions_ci.test
