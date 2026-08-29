#!/usr/bin/env python3
"""Update the compiled time zone tables to a new release and report what changed.

This drives ``generate_tz_data.py`` and additionally compares the new dataset against the one
that is currently checked in, because the useful part of an update is knowing which zones moved
and when. That is what a regression test for the update has to be written against, and the IANA
release notes do not say it in terms of the zones this extension actually carries.

Usage (from the extension/icu directory):

    python3 scripts/update_tz_data.py 2026d              # update to 2026d and report
    python3 scripts/update_tz_data.py 2026d --dry-run    # report only, write nothing
    python3 scripts/update_tz_data.py 2026d --from 2026b # compare against something else

The version is the IANA release: the year followed by a sequential lowercase letter. Either
argument can also be a path to a local ``zoneinfo64.txt``.
"""

import argparse
import datetime
import os
import re
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import generate_tz_data as gen

GENERATED = os.path.join("datetime", "generated", "tz_data.cpp")


def load(source):
    """Parse a dataset, given either an IANA version or a path to a local zoneinfo64.txt."""
    text = gen.strip_comments(gen.read_source(source, gen.DATA_URL))
    version = gen.parse_strings(gen.find_section(text, "TZVersion"))[0]
    names = gen.parse_strings(gen.find_section(text, "Names"))
    zones = gen.parse_zones(gen.find_section(text, "Zones"), names)
    rules = gen.parse_rules(gen.find_section(text, "Rules"))
    return version, zones, rules


def current_version():
    """The release the checked-in file was generated from, or None if it is not there."""
    if not os.path.exists(GENERATED):
        return None
    with open(GENERATED, encoding="utf-8") as handle:
        match = re.search(r'TZ_VERSION = "([^"]+)"', handle.read())
    return match.group(1) if match else None


def offsets_of(zone, zones):
    """The offsets a zone applies, as (transition seconds, raw, dst) in order.

    Link entries carry no data of their own, so they resolve to the entry they point at. The
    first tuple has no transition because it is what applies before the data starts.
    """
    while zone.link_target is not None:
        zone = zones[zone.link_target]
    result = [(None,) + zone.type_offsets[0]]
    for transition, kind in zip(zone.transitions, zone.type_map):
        result.append((transition,) + zone.type_offsets[kind])
    return result


def final_of(zone, zones, rules):
    """The recurring rule that applies after the transitions run out, resolved to its values."""
    while zone.link_target is not None:
        zone = zones[zone.link_target]
    if zone.final_rule is None:
        return None
    return (tuple(rules[zone.final_rule]), zone.final_raw, zone.final_year)


def when(seconds):
    """A transition time as a UTC date, which is how a test for it would be written."""
    epoch = datetime.datetime(1970, 1, 1, tzinfo=datetime.timezone.utc)
    try:
        return (epoch + datetime.timedelta(seconds=seconds)).strftime("%Y-%m-%d %H:%M:%S")
    except OverflowError:
        return "%d (out of range)" % seconds


def describe(offset):
    """An offset pair rendered the way it would appear on a timestamp."""
    total = (offset[1] + offset[2]) // 60
    sign = "-" if total < 0 else "+"
    total = abs(total)
    dst = " dst" if offset[2] else ""
    return "%s%02d:%02d%s" % (sign, total // 60, total % 60, dst)


def compare(old, new):
    """Report the zones that differ between two datasets."""
    old_version, old_zones, old_rules = old
    new_version, new_zones, new_rules = new

    old_by_name = {zone.name: zone for zone in old_zones}
    new_by_name = {zone.name: zone for zone in new_zones}

    added = sorted(set(new_by_name) - set(old_by_name))
    removed = sorted(set(old_by_name) - set(new_by_name))

    changed = []
    for name in sorted(set(old_by_name) & set(new_by_name)):
        before = offsets_of(old_by_name[name], old_zones)
        after = offsets_of(new_by_name[name], new_zones)
        final_before = final_of(old_by_name[name], old_zones, old_rules)
        final_after = final_of(new_by_name[name], new_zones, new_rules)
        if before == after and final_before == final_after:
            continue
        # the first place the two disagree is what a test for this update should target
        first = None
        for index in range(max(len(before), len(after))):
            if index >= len(before) or index >= len(after) or before[index] != after[index]:
                entry = after[index] if index < len(after) else before[index]
                first = entry
                break
        changed.append((name, first, final_before != final_after))

    print("time zone data: %s -> %s" % (old_version, new_version))
    print("  zones: %d -> %d" % (len(old_zones), len(new_zones)))
    print("")

    if added:
        print("added (%d): %s" % (len(added), ", ".join(added)))
        print("")
    if removed:
        print("removed (%d): %s" % (len(removed), ", ".join(removed)))
        print("")

    if not changed:
        print("no zone changed its offsets.")
        return added, removed, changed

    print("changed (%d):" % len(changed))
    for name, first, rule_changed in changed:
        note = " (recurring rule changed)" if rule_changed else ""
        if first is None or first[0] is None:
            print("  %-34s from the start of its data%s" % (name, note))
        else:
            print("  %-34s from %s UTC, now %s%s" % (name, when(first[0]), describe(first), note))
    print("")
    print("Write a test for one of the above in test/sql/timezone/test_icu_timezone.test:")
    name, first, _ = changed[0]
    if first is not None and first[0] is not None:
        stamp = when(first[0]).split(" ")[0]
        print("")
        print("    # %s" % new_version)
        print("    statement ok")
        print("    set timezone='%s';" % name)
        print("")
        print("    query I")
        print("    select '%s'::timestamptz" % stamp)
        print("    ----")
        print("    <the offset the new data gives, %s>" % describe(first))
    return added, removed, changed


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("version", help="the IANA release to update to, or a local zoneinfo64.txt")
    parser.add_argument(
        "--from", dest="previous", default=None, help="what to compare against (default: the checked-in release)"
    )
    parser.add_argument("--dry-run", action="store_true", help="report only, do not write")
    args = parser.parse_args()

    if not os.path.exists(os.path.dirname(GENERATED)):
        sys.stderr.write("run this from the extension/icu directory\n")
        return 1

    previous = args.previous or current_version()
    new = load(args.version)

    if previous is None:
        print("no checked-in release to compare against; generating only.")
    else:
        compare(load(previous), new)

    if args.dry_run:
        print("(dry run: %s not written)" % GENERATED)
        return 0

    version, zones, rules = new
    windows_text = gen.read_source(args.version, gen.WINDOWS_URL, windows=True)
    windows_zones = gen.parse_windows_zones(gen.strip_comments(windows_text))
    with open(GENERATED, "w", encoding="utf-8") as handle:
        handle.write(gen.generate(version, zones, rules, windows_zones))
    print("wrote %s (%s)" % (GENERATED, version))
    print("")
    print("Now rebuild and run the time zone tests:")
    print("    make release && ./build/release/test/unittest 'test/sql/timezone/*'")
    print("test_icu_datetime_sweep.test hashes every zone, so it will need its expected")
    print("value updated for any release that moves a zone.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
