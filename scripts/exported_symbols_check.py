import os
import subprocess
import sys

WHITELIST = [
    '@GLIBC',
    '@GCC',
    '@CXXABI',
    '__cxa_call_terminate',
    '__gnu_cxx::',
    '_ZNSt4pairI',
    'std::',
    'N6duckdb',
    'duckdb::',
    'duckdb_miniz::',
    'duckdb_fmt::',
    'duckdb_hll::',
    'duckdb_moodycamel::',
    'duckdb_yyjson::',
    'duckdb_',
    'RefCounter',
    'registerTMCloneTable',
    'RegisterClasses',
    'Unwind_Resume',
    '__gmon_start',
    '_fini',
    '_init',
    '_version',
    '_end',
    '_edata',
    '__bss_start',
    '__udivti3',
    '__popcount',
    'Adbc',
    'ErrorArrayStream',
    'ErrorFromArrayStream',
    'CreateAPIv1()',
]


def get_symbols(library: str, selection: str) -> list[str]:
    result = subprocess.run(
        [
            'nm',
            '--extern-only',
            '--demangle',
            '--format=just-symbols',
            selection,
            library,
        ],
        check=True,
        capture_output=True,
        text=True,
    )
    return [symbol.strip() for symbol in result.stdout.splitlines() if symbol.strip()]


def find_culprits(defined_symbols: list[str], undefined_symbols: list[str]) -> list[str]:
    candidates = defined_symbols + [symbol for symbol in undefined_symbols if 'random_device' in symbol]
    return [
        symbol for symbol in candidates if not any(entry in symbol for entry in WHITELIST) or 'random_device' in symbol
    ]


def main(argv: list[str]) -> int:
    if len(argv) < 2 or not os.path.isfile(argv[1]):
        print("Usage: [libduckdb dynamic library file, release build]")
        return 1

    library = argv[1]
    defined_symbols = get_symbols(library, '--defined-only')
    undefined_symbols = get_symbols(library, '--undefined-only')
    culprits = find_culprits(defined_symbols, undefined_symbols)

    if culprits:
        print("Found leaked symbols. Either white-list above or change visibility:")
        for symbol in culprits:
            print(symbol)
        return 1

    return 0


if __name__ == '__main__':
    sys.exit(main(sys.argv))
