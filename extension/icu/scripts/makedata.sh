#rm -rf build
set -e
mkdir -p build
pushd build

# download ICU 66
wget -nc https://github.com/unicode-org/icu/archive/refs/tags/release-66-1.zip
unzip -o release-66-1.zip

# download ICU 71 (replace with latest version)
wget -nc https://github.com/unicode-org/icu/archive/refs/tags/release-71-1.zip
unzip -o release-71-1.zip

# copy over the data
find icu-release-71-1/icu4c/source/data -type f ! -iname "*.txt" -delete
cp -r icu-release-71-1/icu4c/source/data icu-release-66-1/icu4c/source

# build the data, make sure to create "filters.json" first, see above
cp ../filters.json icu-release-66-1/icu4c/source
pushd icu-release-66-1/icu4c/source
ICU_DATA_FILTER_FILE=filters.json ./runConfigureICU Linux --with-data-packaging=archive
make
popd

# the data file will be located in icu-release-66-1/icu4c/source/data/out/icudt66l.dat
# copy over the data to the minimal-icu-collation data repository
# then run the following two commands:
popd
python3 scripts/inline-data.py < build/icu-release-66-1/icu4c/source/data/out/icudt66l.dat > third_party/icu/stubdata/stubdata.cpp
