#!/bin/bash

# Usage: ./script/emit_signature.sh <soutput_file> <parameters...>

# 0 for custom section
# 113 in hex = 275 in decimal, total lenght of what follows (1 + 16 + 2 + 256)
# [1(continuation) + 0010011(payload) = \x93, 0(continuation) + 10(payload) = \x02]
echo -n -e '\x00' > "$1"
echo -n -e '\x93\x02' >> "$1"
# 10 in hex = 16 in decimal, lenght of name, 1 byte
echo -n -e '\x10' >> "$1"
echo -n -e 'duckdb_signature' >> "$1"
# the name of the WebAssembly custom section, 16 bytes
# 100 in hex, 256 in decimal
# [1(continuation) + 0000000(payload) = ff, 0(continuation) + 10(payload)],
# for a grand total of 2 bytes
echo -n -e '\x80\x02' >> "$1"
