# Updating the collation data

The collations of the ICU extension are not produced by ICU. `collation/generated/collation_data.cpp`
holds tables that `generate_collation_data.py` derives from the Unicode Consortium data, and
`collation/` implements the Unicode Collation Algorithm over them. The version of the data the
tables were generated from is at the top of the generator and in the header of the generated file.

## When to update

The Unicode Consortium releases CLDR about twice a year and a Unicode version about once a year.
Most releases touch the collation data: new characters are given weights in the root table, and
locale tailorings are corrected. Nothing forces an update - the pinned versions keep the ordering
identical on every platform, which is the main reason the tables are generated instead of read
from ICU - so updating is a deliberate step, usually taken to pick up new scripts or a tailoring
fix that a user reports.

Since the ordering of existing strings can change, an update is a behavioural change for anyone
who stored sort keys or built an index on a collated column. Treat it like one.

## Updating

The generator needs `python3` and the `zstd` command line tool.

1. Record the current behaviour, so that the effect of the update can be reviewed:

   ```bash
   make release
   python3 extension/icu/scripts/collation_snapshot.py record /tmp/collation_before.tsv
   ```

2. Regenerate the tables with the new versions. `CLDR_VERSION` is a tag of the
   [CLDR repository](https://github.com/unicode-org/cldr/tags), `UNICODE_VERSION` a directory of
   [the UCD](https://www.unicode.org/Public/):

   ```bash
   CLDR_VERSION=release-49 UNICODE_VERSION=18.0.0 \
       python3 extension/icu/scripts/generate_collation_data.py
   ```

   The script downloads the data (cached in `~/.cache/duckdb-collation-data/<version>`), prints
   how many mappings and collations it produced, and rewrites `collation_data.cpp`. Update the
   defaults in the generator so that the next run does not need the variables, and format the
   generated file with `make format-fix`.

3. Rebuild and see what changed:

   ```bash
   make release
   python3 extension/icu/scripts/collation_snapshot.py record /tmp/collation_after.tsv
   python3 extension/icu/scripts/collation_snapshot.py compare /tmp/collation_before.tsv /tmp/collation_after.tsv
   ```

   The comparison lists the collations whose sort keys changed and the code point ranges they
   changed in. Check the ranges against the release notes of CLDR and Unicode: a new script or a
   tailoring fix that the notes mention is expected, a change in a range that the notes do not
   mention is a reason to look at the generator before trusting it.

4. Regenerate the ordering test, whose diff is the second review of the update - it covers the
   strings every collation tailors, including the contractions that the snapshot above does not
   reach:

   ```bash
   python3 extension/icu/scripts/generate_collation_tests.py
   git diff test/sql/collate/collation_tailorings.test_slow
   ```

5. Run the tests, and update the ones that encode an ordering that legitimately changed
   (`collation_sort_keys.test` pins sort keys, `test_icu_extensive.test_slow` orderings from the
   CLDR charts):

   ```bash
   make reldebug
   build/reldebug/test/unittest "test/sql/collate/*"
   build/reldebug/test/unittest
   ```

The generator writes one file, so the update is `collation_data.cpp` plus the two version
constants. The size of the compressed tables is printed at the end of the run; a large jump is
worth understanding before committing.

## The other scripts

- `makedata.sh` builds the vendored ICU data package from `filters.json`. It needs a full ICU
  build and is only needed when the *non-collation* ICU data (time zones, calendars) is updated.
- `strip-data.py` removes items from the data package that is already inlined in
  `stubdata.cpp`, without rebuilding ICU. The collation data was removed with it after the
  collator stopped using ICU: `python3 extension/icu/scripts/strip-data.py coll/`.
- `inline-data.py` turns a data package into the C array in `stubdata.cpp`.
