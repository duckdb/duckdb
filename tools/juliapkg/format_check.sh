set -e

if [[ $(git diff) ]]; then
    echo "Julia format found differences"
    exit 1
else
    echo "No differences found"
    exit 0
fi
