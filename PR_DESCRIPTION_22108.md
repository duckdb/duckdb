# PR Title
list_zip: add UNNEST regression for uneven VARCHAR lists

# PR Description
Adds a focused sqllogictest regression for `UNNEST(list_zip(a, b))` with table-sourced `VARCHAR[]` inputs of uneven lengths, plus `truncate=true` coverage.
This guards against regressions of the crash pattern reported in #22108.
