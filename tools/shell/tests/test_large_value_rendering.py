# fmt: off

import pytest
import subprocess
import sys
import os
import re
from typing import List
from conftest import ShellTest

long_string = 'Lorem ipsum dolor sit amet, consectetur adipiscing elit, sed do eiusmod tempor incididunt ut labore et dolore magna aliqua. Ut enim ad minim veniam, quis nostrud exercitation ullamco laboris nisi ut aliquip ex ea commodo consequat. Duis aute irure dolor in reprehenderit in voluptate velit esse cillum dolore eu fugiat nulla pariatur. Excepteur sint occaecat cupidatat non proident, sunt in culpa qui officia deserunt mollit anim id est laborum.'

nested_view = '''
create view special_characters as select {
	'" this ''name contains \\special" characters "': 420, 'a': ' this ''value contains \\special" characters ', 'b': 84, 'c': DATE '2020-01-01', 'd': (SELECT lineitem FROM 'data/parquet-testing/lineitem-top10000.gzip.parquet' AS lineitem LIMIT 1),
	'e': range(10),
	'f': NULL
	} s;
'''

json_widespace = '''
{
"error"
:
false,
"statements"
:
[
1
,
2
,
3
],
"thisisalongstring"
:
{
"nested"
:
"thisisalongvalue"
}
}
'''

big_json = '''
{
  "error": false,
  "statements": [
    {
      "node": {
        "type": "SELECT_NODE",
        "modifiers": [],
        "cte_map": {"map": []},
        "select_list": [
          {
            "class": "CONSTANT",
            "type": "VALUE_CONSTANT",
            "alias": "",
            "query_location": 18446744073709551615,
            "value": {"type": {"id": "BLOB", "type_info": null}, "is_null": false, "value": "10"}
          }
        ],
        "from_table": {"type": "EMPTY", "alias": "", "sample": null, "query_location": 18446744073709551615},
        "where_clause": null,
        "group_expressions": [],
        "group_sets": [],
        "aggregate_handling": "STANDARD_HANDLING",
        "having": null,
        "sample": null,
        "qualify": null
      },
      "named_param_map": []
    }
  ]
}
'''

def test_long_string(shell):
    test = (
        ShellTest(shell)
        .statement('.maxwidth 160')
        .statement(f"SELECT '{long_string}' s")
    )
    result = test.run()
    # verify the entire string is printed
    result.check_stdout("laborum")

def test_extremely_long_string(shell):
    test = (
        ShellTest(shell)
        .statement('.maxwidth 160')
        .statement(f"SELECT repeat('abcdefghijklmnopqrstuvwxyz', 10000)")
    )
    result = test.run()
    # there's a limit to how much wrap-around we can do
    # if the string is too long we end up truncating it still
    result.check_stdout("…")

def test_multiple_long_strings(shell):
    test = (
        ShellTest(shell)
        .statement('.maxwidth 160')
        .statement(f"SELECT '{long_string}' s1, '{long_string}' s2, '{long_string}' s3")
    )
    result = test.run()
    # verify the entire string is printed
    result.check_stdout("laborum")

def test_multiple_long_strings_many_rows(shell):
    test = (
        ShellTest(shell)
        .statement('.maxwidth 160')
        .statement(f"SELECT '{long_string}' s1, '{long_string}' s2, '{long_string}' s3 FROM range(100)")
    )
    result = test.run()
    # if we have many long strings they are not stretched out
    result.check_not_exist("laborum")

    # UNLESS we increase the max rows printed
    test = (
        ShellTest(shell)
        .statement('.maxwidth 160')
        .statement('.maxrows -1')
        .statement(f"SELECT '{long_string}' s1, '{long_string}' s2, '{long_string}' s3 FROM range(100)")
    )
    result = test.run()
    result.check_stdout("laborum")

def test_wrap_column_name(shell):
    test = (
        ShellTest(shell)
        .statement('.maxwidth 100')
        .statement('select 42 "this is a very long column name that should be wrapped if our terminal width is small enough, even if the value it contains is not that large";')
    )
    result = test.run()
    result.check_stdout("that large")

def test_big_json(shell):
    test = (
        ShellTest(shell)
        .statement('.maxwidth 120')
        .statement(f"SELECT '{big_json}'::JSON s")
    )
    result = test.run()
    # verify rendering is like "|      "from_table": ...
    keys = ["from_table", "where_clause", "group_expressions", "group_sets"]
    for key in keys:
        assert re.search(f'│\\s+["]{key}["]:', result.stdout) is not None

def test_big_json_compact(shell):
    # test compact rendering - this might result in multiple keys being placed on one line
    test = (
        ShellTest(shell)
        .statement('.maxwidth 80')
        .statement(f"SELECT '{big_json}'::JSON s")
    )
    result = test.run()
    # verify rendering is like "|      "from_table": ...
    keys = ["error", "statements", "from_table"]
    for key in keys:
        assert re.search(f'│\\s+["]{key}["]:', result.stdout) is not None

def test_multi_big_json(shell):
    # test compact rendering - this might result in multiple keys being placed on one line
    test = (
        ShellTest(shell)
        .statement('.maxwidth 170')
        .statement(f"SELECT s, s, s FROM (SELECT '{big_json}'::JSON s)")
    )
    result = test.run()
    # verify rendering is like "|      "from_table": ...
    keys = ["error", "statements", "from_table"]
    for key in keys:
        assert re.search(f'│\\s+["]{key}["]:', result.stdout) is not None

def test_json_newlines(shell):
    # verify there's no literal \n in the output
    test = (
        ShellTest(shell)
        .statement('.maxwidth 80')
        .statement(f"SELECT '{json_widespace}'::JSON s")
    )
    result = test.run()
    result.check_not_exist("\\n")

def test_struct_special_characters(shell):
    test = (
        ShellTest(shell)
        .statement(nested_view)
        .statement('.maxwidth 120')
        .statement("select s from special_characters")
    )
    result = test.run()
    # verify rendering is like "|      "from_table": ...
    keys = ["l_orderkey", "l_shipdate", "f"]
    for key in keys:
        assert re.search(f"│\\s+[']{key}[']:", result.stdout) is not None

def test_variant_special_characters(shell):
    test = (
        ShellTest(shell)
        .statement(nested_view)
        .statement('.maxwidth 120')
        .statement("select s::variant from special_characters")
    )
    result = test.run()
    # verify rendering is like "|      "from_table": ...
    keys = ["l_orderkey", "l_shipdate", "f"]
    for key in keys:
        assert re.search(f"│\\s+[']{key}[']:", result.stdout) is not None

def test_json_special_characters(shell):
    test = (
        ShellTest(shell)
        .statement(nested_view)
        .statement('.maxwidth 120')
        .statement("select s::json from special_characters")
    )
    result = test.run()
    # verify rendering is like "|      "from_table": ...
    keys = ["l_orderkey", "l_shipdate", "f"]
    for key in keys:
        assert re.search(f'│\\s+["]{key}["]:', result.stdout) is not None

def test_duckbox_grapheme_clusters(shell):
    test = (
        ShellTest(shell)
        .statement(".maxwidth 80")
        .statement("""
FROM (VALUES
    ('Write a short Threads post (2 punchy sentences) for an English tech-news account about Google launching “Personal Intelligence” as a new layer in AI Mode. Include: a hook-style alert/opening, explain that it lets Search “connect the dots” across a user’s Google apps to tailor answers to their context, note it began in the Gemini app, and say it’s now rolling out inside AI Mode in Search for Google AI Pro and Ultra subscribers as a Labs experiment in the U.S. Keep it concise, newsy, and matter-of-fact. No links. Use one 🚨 emoji at the start.'),
    ('Write a short Threads post for an English tech-news account about the US government officially designating AI company Anthropic a “supply chain risk.” Start with a punchy hook and include the 🇺🇸 flag emoji. Clearly state the designation and explain why it’s notable: the label is typically reserved for foreign enemies/adversaries and hasn’t been applied to a US company before. Keep it concise (2–3 sentences), breaking lines for emphasis. Tone: urgent, newsy, slightly incredulous. No extra context, no links, no hashtags, no CTA.')
);"""
        )
    )
    result = test.run()
    result.check_stdout("🇺🇸 flag")

def test_struct_spaces_rendering(shell):
    test = (
        ShellTest(shell)
        .statement('.maxwidth 80')
        .statement("select { column1: 'apple river cloud hammer bright', column2: 'forest table ocean pencil green' } s;")
    )
    result = test.run()
    # verify the entire string is printed
    result.check_stdout("apple river cloud hammer bright")

# fmt: on
