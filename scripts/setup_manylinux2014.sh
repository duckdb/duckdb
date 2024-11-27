#!/bin/bash

# This script is used to setup the additional dependencies required by duckdb
# for the manylinux2014 image. The reason for this to exist in a script is firstly to be able to
# easily reproduce the same environment locally. Secondly, these steps can be now used
# both in the x86 jobs as the aarch64 jobs on GH actions (aarch64 jobs need a special env).

# Examples
#
### To install just basics:
#
# > scripts/setup_manylinux2014.sh general
#
### Install openssl (through vcpkg, which will be in /tmp/vcpkg), ccache and jdk
#
# > VCPKG_TARGET_DIR=/tmp scripts/setup_manylinux2014.sh general vcpkg openssl ccache jdk
#

# Installs deps for a specific required dependency
install_deps() {
  if [ "$1" = "general" ]; then
    git config --global --add safe.directory '*'
    yum install -y curl zip unzip tar
    yum install -y ninja-build

  elif [ "$1" = "aws-cli" ]; then
    curl "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -o "awscliv2.zip"
    unzip awscliv2.zip
    ./aws/install
    aws --version

  elif [ "$1" = "vcpkg" ]; then
    # Note: it is preferred to use the lukka/run-vcpkg@v11 over this when running this
    # in a Github Actions job. When running locally or in the aarch64, these can be used instead
    # Also note that this action install.
    # Note2: this installs vcpkg to $VCPKG_TARGET_DIR
    (
      cd $VCPKG_TARGET_DIR ;
      git clone https://github.com/Microsoft/vcpkg.git ;
      git checkout a1a1cbc975abf909a6c8985a6a2b8fe20bbd9bd6 ;
      cd vcpkg ;
      ./bootstrap-vcpkg.sh
    )
    export VCPKG_ROOT=$VCPKG_TARGET_DIR/vcpkg

  elif [ "$1" = "openssl" ]; then
    yum install -y perl-IPC-Cmd

  elif [ "$1" = "ccache" ]; then
    yum -y install ccache

  elif [ "$1" = "python_alias" ]; then
    ln -fs /usr/local/bin/python3.9 /usr/local/bin/python3

  elif [ "$1" = "jdk" ]; then
    yum install -y java-11-openjdk-devel maven

  elif [ "$1" = "ssh" ]; then
    yum install -y openssh-clients

  elif [ "$1" = "glibc32" ]; then
    yum install -y libgcc*i686 libstdc++*i686 glibc*i686 libgfortran*i686

  elif [ "$1" = "gcc_4_8" ]; then
    yum install -y gcc-c++

  elif [ "$1" = "nodejs" ]; then
    yum install -y nodejs

  else
      >&2 echo "unknown input for setup_manylinux2014.sh: '$1'"
      exit $exit_code
  fi
}

for var in "$@"
do
    install_deps $var
done


