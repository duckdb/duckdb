set -e

./format.sh
if [[ $(git diff) ]]; then
    echo "Julia format found differences:"
    git diff
    exit 1
else
    echo "No differences found"
    exit 0
fi
