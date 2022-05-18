# DUCKDB NOTE
Taken from https://github.com/cwida/fsst @ commit 0f0f9057048412da1ee48e35d516155cb7edd155

# FSST
Fast Static Symbol Table (FSST): fast text compression that allows random access 

[![Watch the video](https://github.com/cwida/fsst/raw/master/fsst-presentation.png)](https://github.com/cwida/fsst/raw/master/fsst-presentation.mp4)

Authors:
- Peter Boncz (CWI)
- Viktor Leis (FSU Jena)
- Thomas Neumann (TU Munchen)

You can contact the authors via the issues of this FSST source repository : https://github.com/cwida/fsst

FSST: Fast Static Symbol Table compression
see the PVLDB paper https://github.com/cwida/fsst/raw/master/fsstcompression.pdf

FSST is a compression scheme focused on string/text data: it can compress strings from distributions with many different values (i.e. where dictionary compression will not work well). It allows *random-access* to compressed data: it is not block-based, so individual strings can be decompressed without touching the surrounding data in a compressed block. When compared to e.g. LZ4 (which is block-based), FSST further achieves similar decompression speed and compression speed, and better compression ratio.

FSST encodes strings using a symbol table -- but it works on pieces of the string, as it maps "symbols" (1-8 byte sequences) onto "codes" (single-bytes). FSST can also represent a byte as an exception (255 followed by the original byte). Hence, compression transforms a sequence of bytes into a (supposedly shorter) sequence of codes or escaped bytes. These shorter byte-sequences could be seen as strings again and fit in whatever your program is that manipulates strings. An optional 0-terminated mode (like, C-strings) is also supported.

FSST ensures that strings that are equal, are also equal in their compressed form. This means equality comparisons can be performed without decompressing the strings.

FSST compression is quite useful in database systems and data file formats. It e.g., allows fine-grained decompression of values in case of selection predicates that are pushed down into a scan operator. But, very often FSST even allows to postpone decompression of string data. This means hash tables (in joins and aggregations) become smaller, and network communication (in case of distributed query processing) is reduced. All of this without requiring much structural changes to existing systems: after all, FSST compressed strings still remain strings.

The implementation of FSST is quite portable, using CMake and has been verified to work on 64-bits x86 computers running Linux, Windows and MacOS (the latter also using arm64).

