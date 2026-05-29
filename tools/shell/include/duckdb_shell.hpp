#pragma once

#include <cstdint>

namespace duckdb_shell {

// Selects which subcommand the shell is rendering for. SHELL is a
// superset of upstream DuckDB's standalone shell -- adds psql-style
// flag spellings (--no-align, -x/--expanded, -P/--pset, etc.) but
// no auto-connect. PSQL adds the connection options and auto-ATTACH
// against a pg-wire endpoint on top of everything SHELL has.
enum class ShellSubcommand : uint8_t {
	SHELL, // `serened shell`
	PSQL,  // `serened psql`
};

// Public entry point. Parses argv, opens the database, runs init / -cmd /
// -c / -f options, then drives the REPL or exits. `subcommand` selects
// the help layout and (for PSQL) turns on the auto-ATTACH wiring after
// option parsing. argv is read but never modified, so callers may pass
// the unmodified main() argv directly.
int Run(int argc, char **argv, ShellSubcommand subcommand);

} // namespace duckdb_shell
