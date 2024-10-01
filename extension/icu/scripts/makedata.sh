#! /bin/sh

# ICU File Structure
icu=https://github.com/unicode-org/icu/archive/refs/tags/release-version.zip
zip_file=release-version.zip
source_path=icu-release-version/icu4c/source
data_path=$source_path"/data"

#rm -rf build
set -e
mkdir -p build
pushd build

# download ICU 66
code_version=66-1
wget -nc ${icu/version/$code_version}
unzip -o ${zip_file/version/$code_version}

# download ICU 72 (replace with latest version)
data_version=72-1
wget -nc ${icu/version/$data_version}
unzip -o ${zip_file/version/$data_version}

# copy over the collation data
find ${data_path/version/$data_version} -type f ! -iname "*.txt" -delete
cp -r ${data_path/version/$data_version} ${source_path/version/$code_version}

# download IANA and copy the latest Time Zone Data
tz_version=2024b
rm -rf icu-data
git clone git@github.com:unicode-org/icu-data.git
cp icu-data/tzdata/icunew/${tz_version}/44/*.txt ${data_path/version/$code_version}/misc

# build the data, make sure to create "filters.json" first, see above
cp ../filters.json ${source_path/version/$code_version}
pushd ${source_path/version/$code_version}
ICU_DATA_FILTER_FILE=filters.json ./runConfigureICU Linux --with-data-packaging=archive
make
popd

# the data file will be located in icu-release-66-1/icu4c/source/data/out/icudt66l.dat
# copy over the data to the minimal-icu-collation data repository
# then run the following two commands:
popd

icudt=icudt${code_version/-[[:digit:]]/}l.dat
python3 scripts/inline-data.py < build/${data_path/version/$code_version}/out/${icudt} > third_party/icu/stubdata/stubdata.cpp
