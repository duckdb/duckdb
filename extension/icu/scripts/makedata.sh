#! /bin/bash
#
# Regenerate third_party/icu/stubdata/stubdata.cpp -- the ICU data blob that is
# inlined (compiled) into the binary.
#
# serenedb's ICU must carry break-iteration AND normalization data (iresearch's
# text tokenizers use ICU BreakIterator + Normalizer2). That makes the data
# build different from upstream duckdb's:
#
#   ICU break rules (brkitr/*.brk) are compiled by `genbrk`, which fully
#   initializes ICU at build time. In this archive-packaged build the data tools
#   link an EMPTY stubdata, so `genbrk` cannot find the Unicode property data it
#   needs and dies with "can not initialize ICU (U_FILE_ACCESS_ERROR)". There is
#   no way to build brkitr purely from source here.
#
# So instead of building everything from source, we:
#   1. build the BASE data subset from source -- filters.json excludes brkitr +
#      normalization, so `genbrk` never runs and the build succeeds. This step
#      also produces the `icupkg` tool we need below.
#   2. download ICU's official PREBUILT full data and graft the brkitr + .nrm
#      items (plus the zone/{root,pool}.res they transitively depend on) onto the
#      base package with `icupkg -a`.
#   3. inline the grafted package into stubdata.cpp.
#
# To bump ICU: update code_version / tz_version, re-run from extension/icu, and
# also re-copy the common/ + i18n/ sources per README "Updating the Code".

set -e

code_version=78.3
major=${code_version%%.*}            # 78
tz_version=2026b
icu_src="https://github.com/unicode-org/icu/archive/refs/tags/release-${code_version}.zip"
icu_full_data="https://github.com/unicode-org/icu/releases/download/release-${code_version}/icu4c-${code_version}-data-bin-l.zip"
src="icu-release-${code_version}/icu4c/source"
dat="icudt${major}l.dat"

here="$(cd "$(dirname "$0")/.." && pwd)"   # extension/icu
cd "$here"
mkdir -p build
cd build

# --- 1. source build: tools + BASE data (filters.json excludes brkitr/norm) ---
wget -nc "$icu_src" -O icu-src.zip
unzip -o -q icu-src.zip
rm -rf icu-data
git clone --depth 1 https://github.com/unicode-org/icu-data.git || true
cp "icu-data/tzdata/icunew/${tz_version}/44/"*.txt "${src}/data/misc/" 2>/dev/null || true
cp ../filters.json "${src}/"
(
  cd "${src}"
  ICU_DATA_FILTER_FILE=filters.json ./runConfigureICU Linux/clang --with-data-packaging=archive
  make -j"$(nproc)"
)
icupkg="${PWD}/${src}/bin/icupkg"
export LD_LIBRARY_PATH="${PWD}/${src}/lib:${PWD}/${src}/stubdata"

# --- 2. graft brkitr + normalization from the prebuilt full data ---
wget -nc "$icu_full_data" -O icu-full-data.zip
unzip -o -q icu-full-data.zip
full="$(find . -name "$dat" ! -path "*${src}*" | head -1)"
"$icupkg" --list "$full" | grep -E '^brkitr/|\.nrm$' > add.txt
printf 'zone/root.res\nzone/pool.res\n' >> add.txt   # deps of zone/tzdbNames.res in the base
rm -rf extracted stage && mkdir -p extracted stage
"$icupkg" -x add.txt -d extracted "$full"
# icupkg derives the package name from the filename, so it must be $dat, and the
# input/output paths must differ.
cp "${src}/data/out/${dat}" "stage/${dat}"
"$icupkg" -a add.txt -s extracted "stage/${dat}" "${dat}"

# --- 3. inline ---
cd "$here"
python3 scripts/inline-data.py < "build/${dat}" > third_party/icu/stubdata/stubdata.cpp
echo "Wrote third_party/icu/stubdata/stubdata.cpp"
