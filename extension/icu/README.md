# The ICU Extension

This project contains an easy-to-use version of the collation/timezone part of the ICU library.

The compiled size of the project is around 6MB. 
The majority of this is the inlined ICU data that is required to properly support collation 
and time zones for all included locales.
The header `Reducing Data Size` down below can help you 
if you want to strip out certain locales to make the included data smaller.

## Updating the Data

ICU makes periodic updates to the data tables (both collation and time zone) when the IANA data changes.
You can follow these updates by subscribing to `tz-announce@iana.org`.
You can then track updates to the ICU data by following the `unicode-org/icu-data` project on GitHub.

### Generating the Data

The shell script `extensions/icu/scripts/makedata.sh` can be used to automatically update to a particular data version.
To use it, first update the `data_version` variable to the latest code version tag corresponding to the data drop,
and update the `tz_version` variable to the time zone version. 
(Time zone versions are in the year followed by a sequential lowercase letter e.g., `2026b`).

Once the script has been updated, run it from the `extension/icu`directory:

```sh
$ pushd extension/icu
$ /bin/sh scripts/makedata.sh
```

The script will download all the code and data, then generate the new `stubdata.cpp` file.
You can then build ICU with the new data.

### Testing the Data

For time zone updates, add a new test to the end of `test/sql/timezone/test_icu_timezone.test` 
that validates one of the changes.
Typically, this will consist of setting the `timezone` to the zone whose rules changed
and then running a simple query to see if the offset is correct.
For example, in early 2026, the Canadian province of British Columbia switched 
to permanent daylight savings time (-07).
To test this, we pick a date well after the usual transition and check the offset.

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

## Updating the Code.

ICU also updates the code roughly annually,
and we import that code directly into the ICU project.
We import it because we occasionally have to fix bugs without waiting for them.
At some point it may make more sense to use a submodule,
but we have not done that yet.

### Copying the Code

The simplest way to obtain the code is to update the data to the latest version (see above).
Once the script runs, you will have the full set of code in `extension/icu/build/icu-release-<tag>`.
We use two source directories, that need to be copied to `extension/icu/third_party`:

```sh
$ cp -r -f build/icu-release-78.3/icu4c/source/common/ third_party/icu/common
$ cp -r -f build/icu-release-78.3/icu4c/source/i18n/ third_party/icu/i18n
```

### Patching the Code

The code cannot quite be used as is; most notably, we need a particular set of configuration options.
These are in `third_party/icu/common/uconfig.h`.
Diff the file against the previous version and pull in the settings we require.
Note that ICU is a live code base and what they think we should be using may not be correct,
so you may have to go down the rabbit hole and add/change some of them!

Occasionally, there are other patches that need to be applied, and the current list is here:

* `extension/icu/third_party/icu/common/utrie2.cpp:143` - The use of `U_POINTER_MASK_LSB` should be commented out because it fails on Apple ARM chips with stubbed data.

Please keep it up to date.

### New Files

New releases may contain new files that our code subset depends on.
You should add these files to the appropriate `CMakeLists.txt` file library.
Note that there are _two_ libraries: A unity library for most files and a traditional library.
The traditional library is needed because a fair number of ICU source files assume that
they are in separate compilation units and define local symbols with conflicting names.
If a file does not have this issue, it should go into the unity library to save compilation time.

### Test the Build

Since ICU is an extension, you can unit test it by simply making a list of all the tests that require ICU,
and then using `unittest`'s `-f` flag:

```sh
$ git grep -E -l -w -i icu -- '*.test*' > smoke.test
$ ./build/release/test/unittest -f smoke.test
```

Check any tests that now fail, but it is most likely that the test output has been modified by the ICU team.
Two recent examples of this:

* The Norwegian collation names were changed around 2020;
* The dates of the Meiji Restoration were updated in the Japanese calendar.

One other source of changes with code updates is `duckdb/main/extension_entries.hpp`.
You may need to regenerate it if new collations are added.

### Clean up and Commit

The simplest way to remove the unused files is to use `git clean -d -f`. 
You should then rebuild to make sure you didn't accidentally remove a new header file.

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

## Reducing Data Size

The inlined data is present in `data/icudtXXl.dat`. 
It is compiled from the ICU library as described [here](https://github.com/unicode-org/icu/blob/master/docs/userguide/icu_data/buildtool.md), 
with the filters set in `extension/icu/filters.json`.

In the default configuration, only `misc`, `"coll_tree"` and `"coll_ucadata"` are included, 
which are the parts required for collation and basic time zone support. 
However, all locales are included. The size of the data can be significantly reduced by stripping certain locales. 
The linked page describes how to do that. 
After re-packaging the data, you can run `scripts/inline-data.py` to inline a smaller segment of the data.

