set -e

if [[ $(git diff) ]]; then
  echo "There are already differences prior to the format! Commit your changes prior to running format_check.sh"
  exit 1
fi

./format.sh
if [[ $(git diff) ]]; then
    echo "Julia format found differences:"
    git diff
    exit 1
else
    echo "No differences found"
    exit 0
fi
