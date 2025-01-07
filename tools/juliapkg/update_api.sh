set -euo pipefail


echo "Updating api.jl..."

OLD_API_FILE=tools/juliapkg/src/api_old.jl
ORIG_DIR=$(pwd)
GIR_ROOT_DIR=$(git rev-parse --show-toplevel)
cd "$GIR_ROOT_DIR"



# Generate the Julia API
python tools/juliapkg/scripts/generate_c_api_julia.py \
    --auto-1-index \
    --capi-dir src/include/duckdb/main/capi/header_generation \
    tools/juliapkg/src/api.jl


echo "Formatting..."
cd "$ORIG_DIR"
./format.sh
