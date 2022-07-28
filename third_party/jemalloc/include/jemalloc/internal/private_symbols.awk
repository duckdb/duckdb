#!/usr/bin/env awk -f

BEGIN {
  sym_prefix = "_"
  split("\
        _je_aligned_alloc \
        _je_calloc \
        _je_dallocx \
        _je_free \
        _je_mallctl \
        _je_mallctlbymib \
        _je_mallctlnametomib \
        _je_malloc \
        _je_malloc_conf \
        _je_malloc_conf_2_conf_harder \
        _je_malloc_message \
        _je_malloc_stats_print \
        _je_malloc_usable_size \
        _je_mallocx \
        _je_smallocx_54eaed1d8b56b1aa528be3bdd1877e59c56fa90c \
        _je_nallocx \
        _je_posix_memalign \
        _je_rallocx \
        _je_realloc \
        _je_sallocx \
        _je_sdallocx \
        _je_xallocx \
        _je_valloc \
        _je_malloc_size \
        _pthread_create \
        ", exported_symbol_names)
  # Store exported symbol names as keys in exported_symbols.
  for (i in exported_symbol_names) {
    exported_symbols[exported_symbol_names[i]] = 1
  }
}

# Process 'nm -a <c_source.o>' output.
#
# Handle lines like:
#   0000000000000008 D opt_junk
#   0000000000007574 T malloc_initialized
(NF == 3 && $2 ~ /^[ABCDGRSTVW]$/ && !($3 in exported_symbols) && $3 ~ /^[A-Za-z0-9_]+$/) {
  print substr($3, 1+length(sym_prefix), length($3)-length(sym_prefix))
}

# Process 'dumpbin /SYMBOLS <c_source.obj>' output.
#
# Handle lines like:
#   353 00008098 SECT4  notype       External     | opt_junk
#   3F1 00000000 SECT7  notype ()    External     | malloc_initialized
($3 ~ /^SECT[0-9]+/ && $(NF-2) == "External" && !($NF in exported_symbols)) {
  print $NF
}
