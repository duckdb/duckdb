"""
Query exclusion logic for BWC testing framework.

This module determines which queries should be skipped or executed from SQL
instead of from serialized plans. The logic was previously in the C++ extension
(test-utils/src/serialize_queries_plans.cpp) and has been moved here to allow
configuration without recompiling the extension.

Two types of exclusions:
1. skip_query: Query is skipped entirely (not executed at all)
2. execute_from_sql: Query cannot be serialized, so it's executed from SQL text

The decisions are communicated to the C++ code via tags in the query file:
- `-- bwc_tag:skip_query` for queries to skip
- `-- bwc_tag:execute_from_sql` for queries that can't be serialized
"""

import re
from typing import Tuple


def _normalize_sql(sql: str) -> str:
    """
    Normalize SQL for pattern matching.
    Removes comments and extra whitespace, converts to lowercase.
    """
    # Remove single-line comments (but preserve bwc_tag comments for now)
    lines = []
    for line in sql.split('\n'):
        stripped = line.strip()
        if stripped.startswith('--'):
            continue
        # Remove inline comments
        if '--' in stripped:
            stripped = stripped[: stripped.index('--')]

        if len(stripped) > 0:
            lines.append(stripped)

    result = ' '.join(lines)

    # Remove multi-line comments
    result = re.sub(r'/\*.*?\*/', ' ', result, flags=re.DOTALL)

    # Normalize whitespace
    result = ' '.join(result.split())

    return result.lower()


def _remove_string_literals(sql: str) -> str:
    """
    Remove string literals from SQL to avoid false positives when matching patterns.
    Replaces 'string' and "string" with empty strings.
    """
    # Handle escaped quotes within strings
    # This is a simplified approach - handles most common cases
    result = re.sub(r"'(?:[^'\\]|\\.)*'", "''", sql)
    result = re.sub(r'"(?:[^"\\]|\\.)*"', '""', result)
    return result


# Patterns for queries that should be skipped entirely
SKIP_PATTERNS = [
    # Transaction statements - we're not testing transaction semantics
    r'\b(begin|commit|rollback)\b',
    r'\bstart\s+transaction\b',
    # Pragmas that cause verification issues
    r'\bpragma\s+enable_verification\b',
    r'\bpragma\s+verify_external\b',
    r'\bpragma\s+threads\b',
    # Thread settings can cause system errors
    r'\bset\s+threads\b',
    r'\breset\s+threads\b',
]

# Patterns for queries that can't be serialized (execute from SQL instead)
# These are checked against SQL with string literals removed
EXECUTE_FROM_SQL_PATTERNS_WITHOUT_LITERALS = [
    # DDL statements - cause "database not marked as modified" errors
    r'\bcreate(\s+or\s+replace)?\s+table\b',
    r'\bcreate(\s+or\s+replace)?(\s+unique)?\s+index\b',
    r'\bcreate(\s+or\s+replace)?\s+macro\b',
    r'\bcreate(\s+or\s+replace)?\s+function\b',
    r'\bcreate(\s+or\s+replace)?\s+sequence\b',
    r'\bcreate(\s+or\s+replace)?\s+type\b',
    r'\bcreate(\s+or\s+replace)?\s+schema\b',
    r'\bcreate(\s+or\s+replace)?(\s+recursive)?\s+view\b',
    r'\bcreate(\s+or\s+replace)?\s+secret\b',
    # ALTER - catalog changes on read-only transaction
    r'\balter\b',
    # DML - transaction is read-only but has made changes
    r'\binsert\b',
    r'\bupdate\b(?!\s*statistics)',  # UPDATE but not UPDATE STATISTICS
    r'\bdelete\b',
    r'\bdrop\b',
    # Prepared statements - not implemented
    r'\bprepare\b',
    r'\bexecute\b',
    r'\bdeallocate\b',
    # All pragmas don't serialize well
    r'\bpragma\b',
    # Table functions that don't have serialization implemented
    r'\bread_csv\s*\(',
    r'\bread_csv_auto\s*\(',
    r'\bread_json\s*\(',
    r'\bread_json_auto\s*\(',
    r'\bread_json_objects\s*\(',
    r'\bread_json_objects_auto\s*\(',
    r'\bread_ndjson\s*\(',
    r'\bread_ndjson_objects\s*\(',
    r'\bread_ndjson_auto\s*\(',
    # Function calls that don't serialize
    r'\bnextval\s*\(',
    r'\bfinalize\s*\(',
    # EXPORT_STATE doesn't serialize
    r'\bexport_state\b',
    # CALL dbgen/dsdgen - causes database modification issues
    r'\bcall\s+dbgen\s*\(',
    r'\bcall\s+dsdgen\s*\(',
]

# Patterns for queries that can't be serialized (execute from SQL instead)
# These are checked against SQL without string literals removed
EXECUTE_FROM_SQL_PATTERNS_WITH_LITERALS = [
    r"\bfrom\s+'[^']+.csv'",
    r"\bfrom\s+'[^']+.csv.gz'",
    r"\bfrom\s+'[^']+.csv.zst'",
    r"\bfrom\s+'[^']+.json'",
]

# Compiled regex patterns for performance
_SKIP_REGEX = [re.compile(p, re.IGNORECASE) for p in SKIP_PATTERNS]
_EXECUTE_FROM_SQL_REGEX_WITHOUT_LIT = [re.compile(p, re.IGNORECASE) for p in EXECUTE_FROM_SQL_PATTERNS_WITHOUT_LITERALS]
_EXECUTE_FROM_SQL_REGEX_WITH_LIT = [re.compile(p, re.IGNORECASE) for p in EXECUTE_FROM_SQL_PATTERNS_WITH_LITERALS]


def _should_skip_query(normalized_sql: str) -> bool:
    """
    Determine if a query should be skipped entirely.

    Returns True for:
    - Transaction statements (BEGIN, COMMIT, ROLLBACK)
    - PRAGMA enable_verification / verify_external
    - SET threads / RESET threads
    """
    for pattern in _SKIP_REGEX:
        if pattern.search(normalized_sql):
            return True

    return False


def _can_serialize_plan(normalized_sql: str) -> bool:
    """
    Determine if a query's plan can be serialized and executed.

    Returns False (meaning execute from SQL) for:
    - DDL statements (CREATE TABLE, CREATE INDEX, etc.)
    - DML statements (INSERT, UPDATE, DELETE, DROP)
    - ALTER statements
    - Prepared statements (PREPARE, EXECUTE)
    - PRAGMA statements
    - Queries using table functions without serialization (read_csv, read_json, etc.)
    - Queries using functions that don't serialize (nextval, finalize)
    - Queries containing EXPORT_STATE
    - CALL dbgen/dsdgen
    """
    # Remove string literals to avoid false positives
    normalized_no_strings = _remove_string_literals(normalized_sql)

    for pattern in _EXECUTE_FROM_SQL_REGEX_WITHOUT_LIT:
        if pattern.search(normalized_no_strings):
            return False

    for pattern in _EXECUTE_FROM_SQL_REGEX_WITH_LIT:
        if pattern.search(normalized_sql):
            return False

    return True


def _get_exclusion_tags(sql: str) -> Tuple[bool, bool]:
    """
    Get exclusion status for a query.

    Returns:
        Tuple of (skip_query, execute_from_sql)
        - skip_query: True if query should be skipped entirely
        - execute_from_sql: True if query can't be serialized (execute from SQL)
    """
    normalized = _normalize_sql(sql)
    skip = _should_skip_query(normalized)
    if skip:
        # If we're skipping, no need to check serialization
        return (True, False)

    can_serialize = _can_serialize_plan(normalized)
    return (False, not can_serialize)


def format_exclusion_tags(sql: str) -> str:
    """
    Format exclusion tags to prepend to a query.

    Returns a string with appropriate bwc_tag comments, or empty string if no exclusions.
    """
    skip, execute_from_sql = _get_exclusion_tags(sql)

    tags = []
    if skip:
        tags.append("-- bwc_tag:skip_query")
    if execute_from_sql:
        tags.append("-- bwc_tag:execute_from_sql")

    return '\n'.join(tags)
