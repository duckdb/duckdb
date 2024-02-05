#!/bin/bash

# Usage: ./script/append_metadata.sh <extension_file_to_append_to> <parameters...>
# Currently hardcoded to host up to 8 fields
# Example: ./scripts/append_metadata.sh file.duckdb_extension git_hash_duckdb_file git_hash_extension_file platfrom_file

if (($# >= 9)); then
  echo "Too many parameters provided, current script can handle at maxium 8 fields"
  exit 1
fi

# 0 for custom section
# 213 in hex = 531 in decimal, total lenght of what follows (1 + 16 + 2 + 8x32 + 256)
# [1(continuation) + 0010011(payload) = \x93, 0(continuation) + 10(payload) = \x04]
echo -n -e '\x00' >> "$1"
echo -n -e '\x93\x04' >> "$1"
# 10 in hex = 16 in decimal, lenght of name, 1 byte
echo -n -e '\x10' >> "$1"
echo -n -e 'duckdb_signature' >> "$1"
# the name of the WebAssembly custom section, 16 bytes
# 1000 in hex, 512 in decimal
# [1(continuation) + 0000000(payload) = ff, 0(continuation) + 100(payload)],
# for a grand total of 2 bytes
echo -n -e '\x80\x04' >> "$1"

dd if=/dev/zero of="$1.empty_32" bs=32 count=1 &> /dev/null
dd if=/dev/zero of="$1.empty_256" bs=256 count=1 &> /dev/null

# Write empty fields
for ((i=$#; i<8; i++))
do
  cat "$1.empty_32" >> "$1"
done

# Write provided fiedls (backwards)
for ((i=$#; i>=2; i--))
do
  cat "${!i}" > "$1.add"
  cat "$1.empty_32" >> "$1.add"
  dd if="$1.add" of="$1.add_trunc" bs=32 count=1 &> /dev/null
  cat "$1.add_trunc" >> "$1"
done

# Write how many fields have been written during previous step
  echo -n "$#" > "$1.add"
  cat "$1.empty_32" >> "$1.add"
  dd if="$1.add" of="$1.add_trunc" bs=32 count=1 &> /dev/null
  cat "$1.add_trunc" >> "$1"

cat "$1.empty_256" >> "$1"

rm -f "$1.*"
