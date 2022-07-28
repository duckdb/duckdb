#!/usr/bin/env awk -f

BEGIN {
  sym_prefix = "_"
  split("\
        _jet_aligned_alloc \
        _jet_calloc \
        _jet_dallocx \
        _jet_free \
        _jet_mallctl \
        _jet_mallctlbymib \
        _jet_mallctlnametomib \
        _jet_malloc \
        _jet_malloc_conf \
        _jet_malloc_conf_2_conf_harder \
        _jet_malloc_message \
        _jet_malloc_stats_print \
        _jet_malloc_usable_size \
        _jet_mallocx \
        _jet_smallocx_54eaed1d8b56b1aa528be3bdd1877e59c56fa90c \
        _jet_nallocx \
        _jet_posix_memalign \
        _jet_rallocx \
        _jet_realloc \
        _jet_sallocx \
        _jet_sdallocx \
        _jet_xallocx \
        _jet_valloc \
        _jet_malloc_size \
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
