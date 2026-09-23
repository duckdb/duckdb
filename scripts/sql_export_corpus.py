#!/usr/bin/env python3
"""Freeze, run, and summarize the SQL export verification corpus."""

import argparse
from collections import Counter, defaultdict
from concurrent.futures import ThreadPoolExecutor
import hashlib
import json
import os
from pathlib import Path
import signal
import subprocess
import time

ROOT = Path(__file__).resolve().parent.parent
CONFIGS = ROOT / 'test/configs'
FLARE = '[TEST_EVENT] '


def digest(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def git(*args):
    return subprocess.check_output(['git', *args], cwd=ROOT).decode().strip()


def events(text):
    decoder = json.JSONDecoder()
    for line in text.splitlines():
        if FLARE in line:
            yield decoder.raw_decode(line.split(FLARE, 1)[1])[0]


def execute(command, log, timeout, stdin=None):
    start = time.monotonic()
    with log.open('w') as output:
        process = subprocess.Popen(
            command,
            cwd=ROOT,
            stdout=output,
            stderr=subprocess.STDOUT,
            stdin=subprocess.PIPE if stdin is not None else subprocess.DEVNULL,
            start_new_session=True,
        )
        timed_out = False
        try:
            process.communicate(stdin.encode() if stdin is not None else None, timeout=timeout)
        except subprocess.TimeoutExpired:
            timed_out = True
            os.killpg(process.pid, signal.SIGKILL)
            process.communicate()
    return process.returncode, timed_out, time.monotonic() - start


def record_key(record):
    return (
        record['file'],
        record['line'],
        record['connection'],
        tuple((loop['name'], loop['iteration']) for loop in record['loops']),
        record['statement'],
    )


def structurally_validated(record):
    return record['outcome'] == 'STRUCTURALLY_VALIDATED'


def resolve_manifest(manifest):
    included, excluded = set(), set()
    for line in manifest.read_text().splitlines():
        line = line.strip()
        if not line or line.startswith('#'):
            continue
        exclude = line.startswith('!')
        pattern = line[1:] if exclude else line
        if not pattern.startswith('test/sql/') or '..' in Path(pattern).parts or not pattern.endswith('.test'):
            raise ValueError(f'Not a normal public SQL test pattern: {pattern}')
        literal = ROOT / pattern
        matches = [literal] if literal.is_file() else list(ROOT.glob(pattern))
        paths = {str(path.relative_to(ROOT)) for path in matches if path.is_file()}
        if not paths and not exclude:
            raise ValueError(f'No SQL tests match: {pattern}')
        (excluded if exclude else included).update(paths)
    paths = sorted(included - excluded)
    if not paths:
        raise ValueError('Manifest must select at least one SQL test')
    return paths


def freeze_manifest(manifest, output):
    paths = resolve_manifest(manifest)
    output.write_text('\n'.join(paths) + '\n')
    return paths


def run(args):
    output = args.output.resolve()
    output.mkdir(parents=True, exist_ok=False)
    (output / 'raw').mkdir()
    # The parent owns the shared root; individual runners only reclaim their run directories.
    temp_root = output / 'test-temp'
    temp_root.mkdir()
    executable = args.unittest.resolve()
    config = CONFIGS / f'verify_sql_export_{args.mode}.json'
    resolved_manifest = output / 'resolved_manifest.txt'
    paths = freeze_manifest(args.manifest, resolved_manifest)
    build_cache = executable.parent.parent / 'CMakeCache.txt'
    libraries = sorted((executable.parent.parent / 'src').glob('libduckdb.*'))
    provenance = {
        'revision': git('rev-parse', 'HEAD'),
        'status': git('status', '--porcelain'),
        'patch_sha256': hashlib.sha256(
            subprocess.check_output(['git', 'diff', '--binary', 'HEAD'], cwd=ROOT)
        ).hexdigest(),
        'untracked_sha256': {
            path: digest(ROOT / path) for path in git('ls-files', '--others', '--exclude-standard').splitlines()
        },
        'unittest': str(executable),
        'unittest_sha256': digest(executable),
        'library_sha256': {str(path): digest(path) for path in libraries if path.suffix in ('.so', '.dylib')},
        'config': str(config.relative_to(ROOT)),
        'config_sha256': digest(config),
        'manifest_sha256': digest(args.manifest),
        'resolved_manifest_sha256': digest(resolved_manifest),
        'resolved_test_count': len(paths),
        'timeout_seconds': args.timeout,
        'workers': args.workers,
        'mode': args.mode,
        'platform': list(os.uname()),
        'test_environment_keys': sorted(key for key in os.environ if key.startswith('DUCKDB_TEST_')),
        'test_sha256': {path: digest(ROOT / path) for path in paths},
        'build_cache': build_cache.read_text() if build_cache.is_file() else None,
    }
    (output / 'invocation.json').write_text(json.dumps(provenance, indent=2, sort_keys=True) + '\n')

    def run_file(path):
        log = output / 'raw' / (path.replace('/', '__') + '.log')
        command = [
            str(executable),
            '--temp-dir-root',
            str(temp_root),
            '--test-config',
            str(config),
            '--emit-test-events',
            '--emit-on-skip',
        ]
        if args.failure_sql:
            command.append('--sql-export-failure-sql')
        command.append(path)
        code, timed_out, duration = execute(command, log, args.timeout)
        observed = list(events(log.read_text(errors='replace')))
        records = sorted(
            (event for event in observed if event.get('event') == 'sql_export'),
            key=record_key,
        )
        explained = sorted(
            (event for event in observed if event.get('event') == 'explain_sql'),
            key=record_key,
        )
        assert len({record_key(record) for record in explained}) == len(explained), path
        ends = [event for event in observed if event.get('event') == 'end']
        status = 'timeout' if timed_out else ('error' if code else 'ok')
        if len(ends) != 1 and not timed_out:
            status = 'missing_terminal_event'
        elif ends and ends[0]['status'] == 'skip-requirement' and code == 0:
            status = 'skip-requirement'
        elif ends and ends[0]['status'] == 'error':
            status = 'error'
        terminal = {key: value for key, value in ends[0].items() if key != 'temp_dir'} if ends else None
        failure_kind = None
        if status == 'error':
            location = terminal.get('data') if terminal else None
            failed_records = [record for record in records if f"{record['file']}:{record['line']}" == location]
            if not failed_records or any(record['loops'] for record in failed_records):
                failure_kind = 'unattributed'
            elif any(
                record['strict_failure']
                or (record['eligible'] and not structurally_validated(record) and record['route'] == 'NONE')
                for record in failed_records
            ):
                failure_kind = 'verifier'
            elif any(record['route'] == 'GENERATED' for record in failed_records):
                failure_kind = 'generated_result_or_error_oracle'
            else:
                failure_kind = 'unattributed'
        summary = {
            'file': path,
            'status': status,
            'exit_code': code,
            'eligible': sum(record['eligible'] for record in records),
            'generated': sum(record['generated'] for record in records),
            'structurally_validated': sum(structurally_validated(record) for record in records),
            'generated_execution': sum(record['route'] == 'GENERATED' for record in records),
            'generated_execution_succeeded': sum(
                record['route'] == 'GENERATED' and record['execution'] == 'SUCCEEDED' for record in records
            ),
            'generated_execution_errored': sum(
                record['route'] == 'GENERATED' and record['execution'] == 'ERRORED' for record in records
            ),
            'comparable': sum(
                structurally_validated(record) and record['comparability'] == 'COMPARABLE' for record in records
            ),
            'non_repeatable': sum(
                structurally_validated(record) and record['comparability'] == 'NON_REPEATABLE' for record in records
            ),
            'comparability_unknown': sum(
                structurally_validated(record) and record['comparability'] == 'UNKNOWN' for record in records
            ),
            'strict_failures': sum(record['strict_failure'] for record in records),
            'explain_sql_generated_execution': len(explained),
            'explain_sql_execution_errored': sum(record['execution'] == 'ERRORED' for record in explained),
            'terminal': terminal,
            'failure_kind': failure_kind,
        }
        return (
            summary,
            records,
            explained,
            {'file': path, 'command': command, 'seconds': duration},
        )

    totals = Counter()
    outcomes, routes, blockers, fallback = Counter(), Counter(), Counter(), Counter()
    constructs = defaultdict(Counter)
    files = []
    with ThreadPoolExecutor(max_workers=args.workers) as pool, (output / 'records.jsonl').open('w') as ledger, (
        output / 'commands.jsonl'
    ).open('w') as commands, (output / 'explain_sql_records.jsonl').open('w') as explain_ledger:
        for index, (summary, records, explained, command) in enumerate(pool.map(run_file, paths), 1):
            files.append(summary)
            for record in explained:
                explain_ledger.write(json.dumps(record, sort_keys=True) + '\n')
            totals['explain_sql_generated_execution'] += summary['explain_sql_generated_execution']
            totals['explain_sql_execution_errored'] += summary['explain_sql_execution_errored']
            commands.write(json.dumps(command, sort_keys=True) + '\n')
            totals['statements'] += len(records)
            totals['eligible'] += summary['eligible']
            totals['generated'] += summary['generated']
            totals['structurally_validated'] += summary['structurally_validated']
            for key in (
                'generated_execution',
                'generated_execution_succeeded',
                'generated_execution_errored',
            ):
                totals[key] += summary[key]
            totals['comparable'] += summary['comparable']
            totals['non_repeatable'] += summary['non_repeatable']
            totals['comparability_unknown'] += summary['comparability_unknown']
            for record in records:
                ledger.write(json.dumps(record, sort_keys=True) + '\n')
                outcomes[record['outcome']] += 1
                routes[record['route']] += 1
                if not record['eligible']:
                    totals['not_applicable'] += 1
                elif record['outcome'].startswith('UNSUPPORTED_'):
                    totals['unsupported'] += 1
                elif not structurally_validated(record):
                    totals['failed'] += 1
                if record['eligible'] and not structurally_validated(record):
                    blockers[record['phase'] + '/' + record['code']] += 1
                if record['route'] == 'ORIGINAL_FALLBACK':
                    fallback[record['phase'] + '/' + record['code']] += 1
                for entry in record['inventory']:
                    key = entry['kind'] + '/' + entry['construct']
                    constructs[key]['occurrences'] += 1
                    constructs[key]['structurally_validated_occurrences'] += int(structurally_validated(record))
            if index % 100 == 0 or index == len(paths):
                print(f'{index}/{len(paths)} files recorded', flush=True)
    aggregate = {
        'counts': dict(totals),
        'outcomes': dict(outcomes),
        'routes': dict(routes),
        'root_blockers': dict(blockers),
        'fallback_reasons': dict(fallback),
        'constructs': dict(constructs),
        'distinct_constructs': len(constructs),
        'distinct_structurally_validated_constructs': sum(
            c['structurally_validated_occurrences'] > 0 for c in constructs.values()
        ),
        'file_statuses': dict(Counter(file['status'] for file in files)),
        'test_failures': dict(Counter(file['failure_kind'] for file in files if file['failure_kind'])),
        'files': files,
    }
    assert totals['generated_execution'] == routes['GENERATED']
    assert totals['generated_execution'] == (
        totals['generated_execution_succeeded'] + totals['generated_execution_errored']
    )
    assert totals['generated_execution'] <= totals['structurally_validated']
    assert totals['eligible'] + totals['not_applicable'] == totals['statements']
    assert totals['structurally_validated'] + totals['unsupported'] + totals['failed'] == totals['eligible']
    (output / 'aggregate.json').write_text(json.dumps(aggregate, sort_keys=True, indent=2) + '\n')
    print(
        json.dumps(
            {key: aggregate[key] for key in ('counts', 'outcomes', 'file_statuses')},
            indent=2,
        )
    )
    if args.mode == 'strict':
        return int(
            any(
                file['status'] != 'ok'
                or file['eligible'] + file['explain_sql_generated_execution'] == 0
                or file['generated'] + file['explain_sql_generated_execution'] == 0
                or file['structurally_validated'] != file['eligible']
                or file['strict_failures']
                for file in files
            )
        )
    return int(any(file['status'] not in ('ok', 'skip-requirement') for file in files))


def selftest(args):
    output = args.output.resolve()
    output.mkdir(parents=True, exist_ok=False)
    config = CONFIGS / 'verify_sql_export_strict.json'
    late_error = (
        "VALUES (1); SELECT CAST(s AS INTEGER) FROM (VALUES " + ','.join(["('0')"] * 5000 + ["('bad')"]) + ') t(s);'
    )
    probes = {
        'supported_fallback': (
            True,
            "statement ok\nSET debug_verify_sql_export='supported'; SET delim_join_as_cte=false;\n"
            "\nquery II\nSELECT i, (SELECT sum(j) FROM range(3)t(j) WHERE j < i) FROM range(3)r(i) ORDER BY i;\n"
            "----\n0\tNULL\n1\t0\n2\t1\n\nquery I\nSELECT 42;\n----\n42\n",
        ),
        'supported_execution_once': (
            True,
            "statement ok\nSET debug_verify_sql_export='supported'; CREATE SEQUENCE s;\n"
            "\nquery I\nSELECT nextval('s');\n----\n1\n"
            "\nquery I\nSELECT currval('s');\n----\n1\n",
        ),
        'supported_result_mismatch': (
            False,
            "statement ok\nSET debug_verify_sql_export='supported';\n\nquery I\nSELECT 1;\n----\n2\n",
        ),
        'supported_execution_error': (
            True,
            "statement ok\nSET debug_verify_sql_export='supported';\n"
            "\nstatement error\nSELECT CAST(s AS INTEGER) FROM (VALUES ('bad')) t(s);\n----\n<REGEX>:.*\n",
        ),
        'explain_once': (
            True,
            "statement ok\nCREATE SEQUENCE s;\n\nexplain_sql\n\nquery I\nSELECT nextval('s');\n----\n1\n"
            "\nquery I\nSELECT last_value FROM duckdb_sequences() WHERE sequence_name='s';\n----\n1\n",
        ),
        'explain_execution_error': (
            True,
            "explain_sql\n\nstatement error\nSELECT error('execution reached');\n----\nexecution reached\n"
            "\nquery T\nSELECT current_setting('debug_verify_sql_export');\n----\nstrict\n",
        ),
        'explain_failure_is_not_expected_error': (
            False,
            'explain_sql\n\nstatement error\nSELECT * FROM missing_source;\n----\n<REGEX>:.*\n',
        ),
        'explain_result_mismatch': (False, 'explain_sql\n\nquery I\nSELECT 1;\n----\n2\n'),
        'explain_multiple_statements': (False, 'explain_sql\n\nquery I\nSELECT 1; SELECT 2;\n----\n1\n'),
        'explain_missing_command': (False, 'explain_sql\n'),
        'explain_setting_restore': (
            True,
            "explain_sql\n\nquery T\nSELECT current_setting('debug_verify_sql_export');\n----\noff\n"
            "\nquery T\nSELECT current_setting('debug_verify_sql_export');\n----\nstrict\n",
        ),
        'explain_inherited_setting': (
            True,
            "statement ok\nSET GLOBAL debug_verify_sql_export='report'; RESET SESSION debug_verify_sql_export;\n"
            "\nexplain_sql\n\nquery I\nSELECT 42;\n----\n42\n"
            "\nstatement ok\nSET GLOBAL debug_verify_sql_export='strict';\n"
            "\nquery T\nSELECT current_setting('debug_verify_sql_export');\n----\nstrict\n",
        ),
        'explain_skipped_command': (
            True,
            'skipif duckdb\nexplain_sql\n\nquery I\nSELECT * FROM missing_source;\n----\n1\n'
            '\nquery I\nSELECT 42;\n----\n42\n',
        ),
        'explain_named_loop': (
            True,
            'loop i 0 3\n\nexplain_sql\n\nquery I named\nSELECT 42;\n----\n42\n\nendloop\n',
        ),
        'isolated_spill_directory': (
            True,
            "query I\nSELECT starts_with(current_setting('temp_directory'), '{TEST_DIR}/sqllogic_temp_')\n----\ntrue\n",
        ),
        'expected_verifier_error': (
            False,
            'statement ok\nSET delim_join_as_cte=false;\n\nstatement error\n'
            'SELECT i, (SELECT sum(j) FROM range(3)t(j) WHERE j < i) FROM range(3)r(i);\n----\n<REGEX>:.*\n',
        ),
        'expected_execution_error': (
            True,
            "statement error\nSELECT CAST(s AS INTEGER) FROM (VALUES ('bad')) t(s);\n----\n<REGEX>:.*\n",
        ),
        'nonvacuous': (False, 'statement ok\nCREATE TABLE t(i INTEGER)\n'),
        'result_mismatch': (False, 'query I\nVALUES (1)\n----\n2\n'),
        'two_statements': (True, 'statement ok\nVALUES (1); VALUES (2);\n'),
        'two_then_one': (
            True,
            'statement ok\nVALUES (1); VALUES (2);\n\nquery I\nVALUES (3)\n----\n3\n',
        ),
        'first_result': (True, 'query I\nVALUES (1); VALUES (2);\n----\n1\n'),
        'error_tail': (
            True,
            "statement error\nVALUES (1); SELECT CAST(s AS INTEGER) FROM (VALUES ('bad')) t(s);\n----\n<REGEX>:.*\n",
        ),
        'late_error_tail': (True, f'statement error\n{late_error}\n----\n<REGEX>:.*\n'),
        'unexpected_error_tail': (False, f'statement ok\n{late_error}\n'),
        'connections': (
            True,
            '''query I
VALUES (1)
----
1

query I named
VALUES (2)
----
2

statement ok
VALUES (3); VALUES (4);

loop i 0 2

query I
VALUES (42)
----
42

endloop

reconnect

query I
VALUES (5)
----
5

concurrentloop i 0 3

query I
VALUES (42)
----
42

endloop
''',
        ),
    }
    for name, (success, sql) in probes.items():
        command = [
            str(args.unittest.resolve()),
            '--stdin',
            '--test-config',
            str(config),
            '--emit-test-events',
            '--sql-export-failure-sql',
        ]
        code, timed_out, _ = execute(command, output / f'{name}.log', args.timeout, sql)
        observed = list(events((output / f'{name}.log').read_text(errors='replace')))
        records = [event for event in observed if event.get('event') == 'sql_export']
        assert not timed_out and (code == 0) == success, (name, code)
        explained = [event for event in observed if event.get('event') == 'explain_sql']
        if name.startswith('explain_'):
            expected_count = (
                3
                if name == 'explain_named_loop'
                else int(
                    name
                    in (
                        'explain_once',
                        'explain_execution_error',
                        'explain_result_mismatch',
                        'explain_setting_restore',
                        'explain_inherited_setting',
                    )
                )
            )
            assert len(explained) == expected_count, (name, explained)
            assert len({record_key(record) for record in explained}) == len(explained)
            if name == 'explain_execution_error':
                assert explained[0]['execution'] == 'ERRORED'
                assert len(records) == 1 and records[0]['route'] == 'GENERATED'
            elif name == 'explain_named_loop':
                assert all(record['connection'] == 'named' for record in explained)
            elif name == 'explain_setting_restore':
                assert len(records) == 1 and records[0]['route'] == 'GENERATED'
        elif name.startswith('supported_'):
            supported = [record for record in records if record['mode'] == 'SUPPORTED' and record['eligible']]
            assert supported and not any(record['strict_failure'] for record in supported)
            if name == 'supported_fallback':
                assert supported[0]['route'] == 'ORIGINAL_FALLBACK'
                assert supported[0]['outcome'] == 'UNSUPPORTED_OPERATOR'
                assert supported[-1]['route'] == 'GENERATED'
            else:
                assert all(record['route'] == 'GENERATED' for record in supported)
        elif name == 'expected_verifier_error':
            assert len(records) == 2 and records[0]['outcome'] == 'NOT_APPLICABLE'
            record = records[1]
            assert record['strict_failure'] and record['route'] == 'NONE'
            assert len(record['issues']) == 1
            issue = record['issues'][0]
            assert issue['code'] == 'UNSUPPORTED_OPERATOR' and issue['phase'] == 'PLAN_EXPORT'
            assert issue['path'] == record['path']
            assert issue['construct']['type'] == 'logical_operator'
            assert issue['construct']['logical_operator'] == 'LOGICAL_DELIM_JOIN'
            assert issue['facts'] == []
            assert issue['message']
        elif name == 'expected_execution_error':
            assert len(records) == 1 and structurally_validated(records[0])
            assert records[0]['execution'] == 'ERRORED' and not records[0]['strict_failure']
        elif name == 'connections':
            assert len(records) == 10 and all(record['route'] == 'GENERATED' for record in records)
            assert len({record_key(record) for record in records}) == len(records)
        elif name in (
            'two_statements',
            'two_then_one',
            'first_result',
            'error_tail',
            'late_error_tail',
            'unexpected_error_tail',
        ):
            indexes = [0, 1, 0] if name == 'two_then_one' else [0, 1]
            assert [record['statement'] for record in records] == indexes
            assert all(record['route'] == 'GENERATED' and structurally_validated(record) for record in records)
            assert len({record_key(record) for record in records}) == len(records)
            assert not records[0]['query_error']
            assert records[1]['query_error'] == name.endswith('error_tail')
            assert not any(record['strict_failure'] for record in records)
        elif name == 'result_mismatch':
            assert len(records) == 1 and records[0]['route'] == 'GENERATED'
            assert records[0]['generated_sql'] and not records[0]['query_error']
        print(f'{name}: PASS')
    return 0


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest='action', required=True)
    freeze = sub.add_parser('freeze')
    freeze.add_argument('--manifest', type=Path, default=CONFIGS / 'sql_export/report_manifest.txt')
    freeze.add_argument('--output', type=Path, required=True)
    runner = sub.add_parser('run')
    runner.add_argument('--mode', choices=['report', 'strict'], required=True)
    runner.add_argument('--manifest', type=Path, required=True)
    runner.add_argument('--workers', type=int, default=4)
    runner.add_argument('--failure-sql', action='store_true')
    check = sub.add_parser('selftest')
    for command in (runner, check):
        command.add_argument('--unittest', type=Path, default=ROOT / 'build/reldebug/test/unittest')
        command.add_argument('--output', type=Path, required=True)
        command.add_argument('--timeout', type=float, default=60)
    select = sub.add_parser('select-strict')
    select.add_argument('--aggregate', type=Path, required=True)
    select.add_argument('--manifest', type=Path, default=CONFIGS / 'sql_export/strict_manifest.txt')
    select.add_argument('--count', type=int, default=5)
    args = parser.parse_args()
    if args.action == 'freeze':
        paths = freeze_manifest(args.manifest, args.output)
        print(f'Frozen {len(paths)} SQL test paths')
        return 0
    if args.action == 'select-strict':
        files = json.loads(args.aggregate.read_text())['files']
        candidates = sorted(
            file['file']
            for file in files
            if file['status'] == 'ok'
            and file['eligible'] > 0
            and file['eligible'] == file['structurally_validated']
            and file['generated'] > 0
            and not file['strict_failures']
        )
        if len(candidates) < args.count:
            raise ValueError(f'Only {len(candidates)} successful nonvacuous files, requested {args.count}')
        args.manifest.write_text('\n'.join(candidates[: args.count]) + '\n')
        print(args.manifest.read_text(), end='')
        return 0
    return selftest(args) if args.action == 'selftest' else run(args)


if __name__ == '__main__':
    raise SystemExit(main())
