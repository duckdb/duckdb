# The ICU Extension

This extension provides the collations, time zones and calendar systems that DuckDB needs for
its date and time functions. It is named after the ICU library, which it used to be built on;
it no longer depends on it, and instead carries its own implementations together with the
Unicode data they are generated from.

## Updating the Time Zone Data

The time zone data lives in `datetime/generated/tz_data.cpp` and is generated from the Unicode
Consortium time zone dataset, which is the Unicode redistribution of the IANA time zone database.
The IANA data changes a few times a year; you can follow the updates by subscribing to
`tz-announce@iana.org`, and the corresponding Unicode drops by following the `unicode-org/icu-data`
project on GitHub.

To update to a new release, run `update_tz_data.py` with the IANA version (the year followed by
a sequential lowercase letter, e.g. `2026c`). It rewrites `datetime/generated/tz_data.cpp` and
reports how the new data differs from the release that is currently checked in:

```sh
$ pushd extension/icu
$ python3 scripts/update_tz_data.py 2026c
$ popd
```

```
time zone data: 2026b -> 2026c
  zones: 639 -> 639

changed (5):
  Africa/Casablanca                  from 2026-09-20 01:00:00 UTC, now +00:00
  Africa/El_Aaiun                    from 2026-09-20 01:00:00 UTC, now +00:00
  America/Edmonton                   from 2008-03-09 09:00:00 UTC, now -06:00 dst (recurring rule changed)
  ...
```

Pass `--dry-run` to see the report without writing anything, and `--from <version>` to compare
against a release other than the one checked in. Either version can also be a path to a local
`zoneinfo64.txt`, in which case `windowsZones.txt` is read from beside it. The lower-level
`generate_tz_data.py` writes the same file to standard output and does no comparison:

```sh
$ python3 scripts/generate_tz_data.py 2026c > datetime/generated/tz_data.cpp
```

### Testing the Data

Add a test to the end of `test/sql/timezone/test_icu_timezone.test` for one of the zones the
report listed as changed. This is what the report is for: the IANA release notes describe the
change in terms of countries and laws, and the report says which of the zones carried here
moved and on which date, which is what the test has to be written against.

A test sets the `timezone` to a zone that changed and checks the offset on a date after the
change. For example, in early 2026 the Canadian province of British Columbia moved to permanent
daylight savings time, so we pick a date well after the usual transition:

```sql
# 2026b
# British Columbia moved to permanent -07 on 2026-03-09.
statement ok
set timezone='America/Vancouver';

query I
select '2026-11-10'::timestamptz
----
2026-11-10 00:00:00-07
```

To be extra careful, run the test before updating to make sure it actually changes.
Occasionally, a new zone is introduced, in which case you can only check post-change.

Then rebuild and run the time zone tests:

```sh
$ make release && ./build/release/test/unittest 'test/sql/timezone/*'
```

Any release that moves a zone will also change the hash in
`test/sql/timezone/test_icu_datetime_sweep.test`, which covers every zone at once; update its
expected value once you are satisfied that the zones the report listed are the only ones that
moved.

## Updating the Collation Data

The collation data lives in `collation/generated/collation_data.cpp` and is generated from the
CLDR and Unicode data. Updating it changes the order of existing strings, so it is a deliberate
step rather than something to do on every release: `scripts/README.md` describes when to take it,
how to review what the new data changed, and how to regenerate the ordering tests that record it.

## Testing

The extension has no external dependencies, so it can be unit tested by making a list of all the
tests that mention it and using `unittest`'s `-f` flag:

```sh
$ git grep -E -l -w -i icu -- '*.test*' > smoke.test
$ ./build/release/test/unittest -f smoke.test
```

Two of those tests are worth knowing about:

* `test/sql/timezone/test_icu_datetime_sweep.test` hashes the offsets of every time zone at a
  spread of instants and the rendering of two centuries of dates in every calendar system, which
  is the broad net that catches an unintended change to either dataset.
* `test/sql/timezone/test_icu_calendar_*.test` cover the calendar systems one by one, including
  the boundaries of their eras and their leap months and days.

## Usage

Here are a small number of snippets.

### List Supported Collation Locales

The list of available collations is now available from SQL:

```sql
from pragma_collations();
```

### Create a Collator and sort a vector of strings

We can use collators to perform locale-based string ordering:

```sql
create or replace table german(n varchar collate de);
insert into german values ('Göbel'), ('Goethe'), ('Goldmann'), ('Göthe'), ('Götz'), ('Gabel');
from german order by 1;
```

|    n     |
|----------|
| Gabel    |
| Göbel    |
| Goethe   |
| Goldmann |
| Göthe    |
| Götz     |

### Listing Available Time Zones

The list of time zones is now available from SQL:

```sql
from pg_timezone_names() where name like 'US/%';
```

|       name        |      abbrev       | utc_offset | is_dst |
|-------------------|-------------------|------------|--------|
| US/Alaska         | AST               | -08:00:00  | true   |
| US/Aleutian       | US/Aleutian       | -09:00:00  | true   |
| US/Arizona        | MST               | -07:00:00  | false  |
| US/Central        | CST               | -05:00:00  | true   |
| US/East-Indiana   | IET               | -04:00:00  | true   |
| US/Eastern        | EST5EDT           | -04:00:00  | true   |
| US/Hawaii         | HST               | -10:00:00  | false  |
| US/Indiana-Starke | US/Indiana-Starke | -05:00:00  | true   |
| US/Michigan       | US/Michigan       | -04:00:00  | true   |
| US/Mountain       | Navajo            | -06:00:00  | true   |
| US/Pacific        | PST               | -07:00:00  | true   |
| US/Pacific-New    | PST               | -07:00:00  | true   |
| US/Samoa          | US/Samoa          | -11:00:00  | false  |

### Listing Available Calendars

ICU also supports non-Gregorian calendars, which are now available from SQL:

```sql
from icu_calendar_names() order by 1;
```

|        name         |
|---------------------|
| buddhist            |
| chinese             |
| coptic              |
| dangi               |
| ethiopic            |
| ethiopic-amete-alem |
| gregorian           |
| hebrew              |
| indian              |
| islamic             |
| islamic-civil       |
| islamic-rgsa        |
| islamic-tbla        |
| islamic-umalqura    |
| iso8601             |
| japanese            |
| persian             |
| roc                 |
