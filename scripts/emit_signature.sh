#!/bin/bash

# Usage: ./script/emit_signature.sh <soutput_file> <parameters...>
# Currently hardcoded to host up to 8 fields
# Example: ./scripts/emit_signature.sh signature 0 git_hash_duckdb git_hash_extension platfrom

if (($# >= 9)); then
  echo "Too many parameters provided, current script can handle at maxium 8 fields"
  exit 1
fi

# 0 for custom section
# 213 in hex = 531 in decimal, total lenght of what follows (1 + 16 + 2 + 8x32 + 256)
# [1(continuation) + 0010011(payload) = \x93, 0(continuation) + 10(payload) = \x04]
echo -n -e '\x00' > "$1"
echo -n -e '\x93\x04' >> "$1"
# 10 in hex = 16 in decimal, lenght of name, 1 byte
echo -n -e '\x10' >> "$1"
echo -n -e 'duckdb_signature' >> "$1"
# the name of the WebAssembly custom section, 16 bytes
# 1000 in hex, 512 in decimal
# [1(continuation) + 0000000(payload) = ff, 0(continuation) + 100(payload)],
# for a grand total of 2 bytes
echo -n -e '\x80\x04' >> "$1"

rm -f "$1.*"

dd if=/dev/zero of="$1.empty_32" bs=32 count=1
dd if=/dev/zero of="$1.empty_256" bs=256 count=1

for ((i=$#; i<9; i++))
do
  cat "$1.empty_32" >> "$1"
done

for ((i=$#; i>=2; i--))
do
  rm "$1.add_trunc"
  echo "${!i}" > "$1.add"
  cat "$1.empty_32" >> "$1.add"
  dd if="$1.add" of="$1.add_trunc" bs=32 count=1
  cat "$1.add_trunc" >> "$1"
done

cat "$1.empty_256" >> "$1"
