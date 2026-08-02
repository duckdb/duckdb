#!/usr/bin/env python3
"""Generate the compiled time zone tables used by the ICU extension.

The input is the Unicode Consortium time zone dataset (``zoneinfo64.txt``), which is the
Unicode redistribution of the IANA time zone database:

    https://github.com/unicode-org/icu-data/tree/main/tzdata/icunew

Usage (from the extension/icu directory):

    python3 scripts/generate_tz_data.py 2026c > datetime/generated/tz_data.cpp

The version argument is the IANA release (year + sequential lowercase letter). A local
file can be used instead by passing a path.
"""

import os
import re
import sys
import urllib.request

DATA_URL = "https://raw.githubusercontent.com/unicode-org/icu-data/main/tzdata/icunew/{version}/44/zoneinfo64.txt"
WINDOWS_URL = "https://raw.githubusercontent.com/unicode-org/icu-data/main/tzdata/icunew/{version}/44/windowsZones.txt"

# The zone that ICU uses to signal "this identifier is not a time zone".
UNKNOWN_ZONE = "Etc/Unknown"


class Zone:
    """A single entry of the Zones array - either zone data or a link to another entry."""

    def __init__(self, index, name):
        self.index = index
        self.name = name
        self.link_target = None  # index of the entry holding the data, for link entries
        self.transitions = []  # transition times in seconds since the epoch
        self.type_offsets = []  # (raw offset, dst offset) pairs in seconds
        self.type_map = []  # for every transition, the index of the offsets that follow it
        self.links = []  # indexes of all entries sharing this data, including itself
        self.final_rule = None  # name of the recurring rule that applies after the data
        self.final_raw = 0  # standard offset in seconds used together with final_rule
        self.final_year = 0  # first year in which final_rule applies


def read_source(source, url, windows=False):
    """Read one of the dataset files, from a local directory or from the Unicode repository.

    A source that exists on disk is a path to a zoneinfo64.txt, and the companion file is taken
    from beside it. Anything else is an IANA version to fetch.
    """
    if os.path.exists(source):
        if windows:
            source = os.path.join(os.path.dirname(source) or ".", "windowsZones.txt")
        with open(source, encoding="utf-8-sig") as handle:
            return handle.read()
    return urllib.request.urlopen(url.format(version=source)).read().decode("utf-8-sig")


def strip_comments(text):
    return re.sub(r"//[^\n]*", "", text)


def read_block(text, pos):
    """Return the contents of the brace-delimited block starting at or after pos."""
    start = text.index("{", pos)
    depth = 0
    for i in range(start, len(text)):
        if text[i] == "{":
            depth += 1
        elif text[i] == "}":
            depth -= 1
            if depth == 0:
                return text[start + 1 : i], i + 1
    raise ValueError("unterminated block")


def find_section(text, name):
    """Return the contents of a top-level ``name`` block (with or without a type tag)."""
    match = re.search(r"\b" + name + r"\b\s*(?::\w+)?\s*\{", text)
    if not match:
        raise ValueError("missing section " + name)
    return read_block(text, match.start())[0]


def parse_ints(text):
    return [int(value) for value in re.findall(r"-?\d+", text)]


def parse_strings(text):
    return re.findall(r'"([^"]*)"', text)


def parse_zones(text, names):
    """Parse the Zones array. Entries are matched up with the (parallel) Names array."""
    zones = []
    pos = 0
    while True:
        match = re.compile(r":(int|table)\s*\{").search(text, pos)
        if not match:
            break
        body, pos = read_block(text, match.start())
        zone = Zone(len(zones), names[len(zones)])
        if match.group(1) == "int":
            zone.link_target = int(body.strip())
            zones.append(zone)
            continue
        # a table holds the actual zone data
        fields = {}
        inner = 0
        while True:
            key = re.compile(r"(\w+)\s*(?::(\w+))?\s*\{").search(body, inner)
            if not key:
                break
            value, inner = read_block(body, key.start())
            fields[key.group(1)] = value
        # transitions are split into three vectors so that they fit into 32-bit integers
        pre32 = parse_ints(fields.get("transPre32", ""))
        post32 = parse_ints(fields.get("transPost32", ""))
        zone.transitions = [(pre32[i] << 32) | (post32_lo(pre32[i + 1])) for i in range(0, len(pre32), 2)]
        zone.transitions += parse_ints(fields.get("trans", ""))
        zone.transitions += [(post32[i] << 32) | (post32_lo(post32[i + 1])) for i in range(0, len(post32), 2)]
        offsets = parse_ints(fields["typeOffsets"])
        zone.type_offsets = [(offsets[i], offsets[i + 1]) for i in range(0, len(offsets), 2)]
        type_map = parse_strings(fields.get("typeMap", ""))
        zone.type_map = bytes.fromhex("".join(type_map)) if type_map else b""
        zone.links = parse_ints(fields.get("links", ""))
        if "finalRule" in fields:
            zone.final_rule = parse_strings(fields["finalRule"])[0]
            zone.final_raw = int(fields["finalRaw"].strip())
            zone.final_year = int(fields["finalYear"].strip())
        if len(zone.type_map) != len(zone.transitions):
            raise ValueError("type map does not match the transitions of " + zone.name)
        zones.append(zone)
    if len(zones) != len(names):
        raise ValueError("the Zones and Names arrays have different sizes")
    return zones


def post32_lo(value):
    """The low half of a 64-bit transition is stored as a signed 32-bit integer."""
    return value & 0xFFFFFFFF


def parse_windows_zones(text):
    """Parse the mapping from Windows time zone names to the zones of this dataset."""
    body = find_section(text, "mapTimezones")
    mapping = []
    pos = 0
    while True:
        entry = re.compile(r'"([^"]+)"\s*\{').search(body, pos)
        if not entry:
            break
        regions, pos = read_block(body, entry.start())
        inner = 0
        while True:
            key = re.compile(r'(\w+)\s*\{').search(regions, inner)
            if not key:
                break
            value, inner = read_block(regions, key.start())
            # a region can map to several zones, of which the first one is the default
            zones = "".join(parse_strings(value)).split()
            if zones:
                mapping.append((entry.group(1), key.group(1), zones[0]))
    return sorted(mapping)


def parse_rules(text):
    rules = {}
    pos = 0
    while True:
        # rule names are not always words: releases have carried names like C-Eur
        match = re.compile(r"([\w-]+)\s*:intvector\s*\{").search(text, pos)
        if not match:
            break
        body, pos = read_block(text, match.start())
        values = parse_ints(body)
        if len(values) != 11:
            raise ValueError("unexpected rule size for " + match.group(1))
        rules[match.group(1)] = values
    return rules


def escape(name):
    return name.replace("\\", "\\\\").replace('"', '\\"')


def emit_array(out, declaration, values, per_line):
    out.append("static const %s[] = {" % declaration)
    for i in range(0, len(values), per_line):
        out.append("    " + ", ".join(values[i : i + per_line]) + ("," if i + per_line < len(values) else ""))
    out.append("};")


def generate(version, zones, rules, windows_zones):
    rule_names = sorted(rules)
    rule_index = {name: i for i, name in enumerate(rule_names)}

    out = []
    out.append("//===----------------------------------------------------------------------===//")
    out.append("//                         DuckDB")
    out.append("//")
    out.append("// tz_data.cpp")
    out.append("//")
    out.append("// This file is generated by scripts/generate_tz_data.py - do not edit.")
    out.append("// Source: Unicode Consortium time zone dataset (IANA release %s)" % version)
    out.append("//===----------------------------------------------------------------------===//")
    out.append("")
    out.append('#include "tz_data.hpp"')
    out.append("")
    out.append("namespace duckdb {")
    out.append("namespace datetime {")
    out.append("")

    # the per-zone data arrays
    data_zones = [zone for zone in zones if zone.link_target is None]
    data_index = {}
    for index, zone in enumerate(zones):
        if zone.link_target is None:
            data_index[index] = len(data_index)
    for zone in data_zones:
        suffix = data_index[zone.index]
        if zone.transitions:
            emit_array(out, "int64_t TRANSITIONS_%d" % suffix, ["%dLL" % value for value in zone.transitions], 8)
            emit_array(out, "uint8_t TYPE_MAP_%d" % suffix, ["%d" % value for value in zone.type_map], 32)
        emit_array(out, "TZTypeOffset TYPE_OFFSETS_%d" % suffix, ["{%d, %d}" % pair for pair in zone.type_offsets], 8)
        if zone.links:
            emit_array(out, "uint16_t LINKS_%d" % suffix, ["%d" % value for value in zone.links], 16)
        out.append("")

    out.append("const TZZoneData TZ_ZONE_DATA[] = {")
    for zone in data_zones:
        suffix = data_index[zone.index]
        transitions = "TRANSITIONS_%d" % suffix if zone.transitions else "nullptr"
        type_map = "TYPE_MAP_%d" % suffix if zone.transitions else "nullptr"
        links = "LINKS_%d" % suffix if zone.links else "nullptr"
        final_rule = rule_index[zone.final_rule] if zone.final_rule else -1
        out.append(
            "    {%s, %d, TYPE_OFFSETS_%d, %d, %s, %s, %d, %d, %d, %d},"
            % (
                transitions,
                len(zone.transitions),
                suffix,
                len(zone.type_offsets),
                type_map,
                links,
                len(zone.links),
                final_rule,
                zone.final_raw,
                zone.final_year,
            )
        )
    out.append("};")
    out.append("")

    out.append("const TZZone TZ_ZONES[] = {")
    for index, zone in enumerate(zones):
        target = index if zone.link_target is None else zone.link_target
        out.append('    {"%s", %d},' % (escape(zone.name), data_index[target]))
    out.append("};")
    out.append("")

    out.append("const TZRule TZ_RULES[] = {")
    for name in rule_names:
        values = rules[name]
        out.append("    {%s}," % ", ".join(str(value) for value in values))
    out.append("};")
    out.append("")

    out.append("const TZWindowsZone TZ_WINDOWS_ZONES[] = {")
    for name, region, zone in windows_zones:
        out.append('    {"%s", "%s", "%s"},' % (escape(name), escape(region), escape(zone)))
    out.append("};")
    out.append("")
    out.append("const idx_t TZ_WINDOWS_ZONE_COUNT = %d;" % len(windows_zones))
    out.append("const idx_t TZ_ZONE_COUNT = %d;" % len(zones))
    out.append('const char *const TZ_VERSION = "%s";' % version)
    out.append("")
    out.append("} // namespace datetime")
    out.append("} // namespace duckdb")
    out.append("")
    return "\n".join(out)


def main():
    if len(sys.argv) != 2:
        sys.stderr.write("usage: generate_tz_data.py <iana-version|path-to-zoneinfo64.txt>\n")
        return 1
    source = sys.argv[1]
    text = strip_comments(read_source(source, DATA_URL))
    version = parse_strings(find_section(text, "TZVersion"))[0]
    names = parse_strings(find_section(text, "Names"))
    zones = parse_zones(find_section(text, "Zones"), names)
    rules = parse_rules(find_section(text, "Rules"))

    windows_zones = parse_windows_zones(strip_comments(read_source(source, WINDOWS_URL, windows=True)))

    sys.stdout.write(generate(version, zones, rules, windows_zones))
    return 0


if __name__ == "__main__":
    sys.exit(main())
