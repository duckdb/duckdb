#include "shell_state.hpp"
#include "shell_highlight.hpp"
#include "terminal.hpp"

namespace duckdb_shell {

// FIXME: should be moved out of a define
#define SEP_Unit   "\x1F"
#define SEP_Record "\x1E"

template <RenderMode output_mode>
MetadataResult ToggleOutputMode(ShellState &state, const vector<string> &args) {
	state.cMode = state.mode = output_mode;
	return MetadataResult::SUCCESS;
}

MetadataResult ToggleASCIIMode(ShellState &state, const vector<string> &args) {
	state.cMode = state.mode = RenderMode::ASCII;
	state.colSeparator = SEP_Unit;
	state.rowSeparator = SEP_Record;
	return MetadataResult::SUCCESS;
}

MetadataResult ToggleCSVMode(ShellState &state, const vector<string> &args) {
	state.cMode = state.mode = RenderMode::CSV;
	state.colSeparator = ",";
	return MetadataResult::SUCCESS;
}

MetadataResult EnableBail(ShellState &state, const vector<string> &args) {
	state.bail = BailOnError::BAIL_ON_ERROR;
	return MetadataResult::SUCCESS;
}

MetadataResult EnableBatch(ShellState &state, const vector<string> &args) {
	state.stdin_is_interactive = false;
	return MetadataResult::SUCCESS;
}

MetadataResult DisableBatch(ShellState &state, const vector<string> &args) {
	state.stdin_is_interactive = true;
	return MetadataResult::SUCCESS;
}

MetadataResult SetReadOnlyMode(ShellState &state, const vector<string> &args) {
	state.config.options.access_mode = duckdb::AccessMode::READ_ONLY;
	return MetadataResult::SUCCESS;
}

template <bool HEADER>
MetadataResult ToggleHeader(ShellState &state, const vector<string> &args) {
	state.showHeader = HEADER;
	return MetadataResult::SUCCESS;
}

MetadataResult DisableStdin(ShellState &state, const vector<string> &args) {
	state.readStdin = false;
	return MetadataResult::SUCCESS;
}

MetadataResult EnableEcho(ShellState &state, const vector<string> &args) {
	state.ShellSetFlag(ShellFlags::SHFLG_Echo);
	return MetadataResult::SUCCESS;
}

MetadataResult AllowUnredacted(ShellState &state, const vector<string> &args) {
	state.config.SetOptionByName("allow_unredacted_secrets", true);
	return MetadataResult::SUCCESS;
}

MetadataResult AllowUnsigned(ShellState &state, const vector<string> &args) {
	state.config.SetOptionByName("allow_unsigned_extensions", true);
	return MetadataResult::SUCCESS;
}

MetadataResult ShowVersionAndExit(ShellState &state, const vector<string> &args) {
	printf("SereneDB %s\n", SERENEDB_SHELL_VERSION);
	return MetadataResult::EXIT;
}

MetadataResult SetPsqlHost(ShellState &state, const vector<string> &args) {
	state.psql_host = args[1];
	return MetadataResult::SUCCESS;
}

MetadataResult SetPsqlPort(ShellState &state, const vector<string> &args) {
	state.psql_port = args[1];
	return MetadataResult::SUCCESS;
}

MetadataResult SetPsqlDbname(ShellState &state, const vector<string> &args) {
	state.psql_dbname = args[1];
	return MetadataResult::SUCCESS;
}

MetadataResult SetPsqlUser(ShellState &state, const vector<string> &args) {
	state.psql_user = args[1];
	return MetadataResult::SUCCESS;
}

MetadataResult PsqlPasswordNoOp(ShellState &state, const vector<string> &args) {
	// -w/-W are psql password-prompt knobs; serened has no auth yet so
	// we accept them silently for psql script compatibility.
	return MetadataResult::SUCCESS;
}

// psql -t / --tuples-only: emit data rows only. duckdb's duckbox mode
// renders headers regardless of `showHeader`, so we drop to the
// unaligned `list` mode at the same time.
MetadataResult PsqlTuplesOnly(ShellState &state, const vector<string> &args) {
	state.cMode = state.mode = RenderMode::LIST;
	state.showHeader = false;
	state.ShellSetFlag(ShellFlags::SHFLG_HeaderSet);
	return MetadataResult::SUCCESS;
}

// psql -o FILE / --output=FILE: send query output to FILE. Same effect
// as ".output FILE" but runs as a pre-init option.
MetadataResult PsqlOutputFile(ShellState &state, const vector<string> &args) {
	string cmd = ".output " + args[1];
	auto rc = state.DoMetaCommand(cmd);
	if (rc != 0) {
		return MetadataResult::FAIL;
	}
	return MetadataResult::SUCCESS;
}

// psql -l / --list: list available databases and exit. The shell's
// post-init phase honours MetadataResult::EXIT, so queue it there.
MetadataResult PsqlListDatabases(ShellState &state, const vector<string> &args) {
	state.readStdin = false;
	auto rc = state.RunInitialCommand("SHOW DATABASES;", /*bail=*/true);
	if (rc != 0) {
		ShellState::Exit(rc);
		return MetadataResult::EXIT;
	}
	return MetadataResult::EXIT;
}

MetadataResult PrintHelpAndExit(ShellState &state, const vector<string> &args) {
	state.PrintUsage();
	return MetadataResult::EXIT;
}

// psql -q / --quiet: suppress the startup banner. (psql also suppresses
// per-statement chatter; we don't print any so this is a no-op there.)
MetadataResult PsqlQuiet(ShellState &state, const vector<string> &args) {
	state.startup_text = StartupText::NONE;
	return MetadataResult::SUCCESS;
}

// psql -n / --no-readline: switch the line editor to the dumb fallback.
MetadataResult PsqlNoReadline(ShellState &state, const vector<string> &args) {
	state.rl_version = ReadLineVersion::FALLBACK;
	return MetadataResult::SUCCESS;
}

// psql -z / --field-separator-zero: column separator = single NUL byte.
MetadataResult PsqlFieldSepZero(ShellState &state, const vector<string> &args) {
	state.colSeparator = string(1, '\0');
	return MetadataResult::SUCCESS;
}

// psql -0 / --record-separator-zero: row separator = single NUL byte.
MetadataResult PsqlRecordSepZero(ShellState &state, const vector<string> &args) {
	state.rowSeparator = string(1, '\0');
	return MetadataResult::SUCCESS;
}

// psql -1 / --single-transaction: when a -c/-f is queued, the shell
// wraps the whole batch in BEGIN/COMMIT (done in the main loop after
// parsing, not here).
MetadataResult PsqlSingleTransaction(ShellState &state, const vector<string> &args) {
	state.psql_single_transaction = true;
	return MetadataResult::SUCCESS;
}

// psql flags that have no clean serened equivalent (-E echo-hidden,
// -L log-file, -s single-step, -S single-line, -P pset, -T table-attr).
// Accepted so psql-shaped scripts don't fail at argv parsing; behaviour
// is documented in the per-entry description.
MetadataResult PsqlAcceptIgnore(ShellState &state, const vector<string> &args) {
	return MetadataResult::SUCCESS;
}

// psql -P VAR[=ARG] / --pset=VAR[=ARG]: apply a subset of psql's
// `\pset` printing options. Recognised keys map directly to duckdb shell
// state; unrecognised keys (and unrecognised values for recognised keys)
// emit a warning to stderr but don't fail the invocation.
//
// Implemented keys:
//   format = aligned | unaligned | csv | html | latex | wrapped
//   expanded = on | off                  (toggles when no value)
//   null = STRING
//   tuples_only = on | off               (toggles when no value)
//   pager = on | off | always
// Synonyms accepted: `x` for `expanded`, `t` for `tuples_only`.
MetadataResult PsqlPset(ShellState &state, const vector<string> &args) {
	const string &raw = args[1];
	string key;
	string value;
	bool has_value = false;
	if (auto eq = raw.find('='); eq != string::npos) {
		key = raw.substr(0, eq);
		value = raw.substr(eq + 1);
		has_value = true;
	} else {
		key = raw;
	}
	const string keyl = StringUtil::Lower(key);
	const string vall = has_value ? StringUtil::Lower(value) : string {};

	auto warn = [&](const string &msg) {
		state.PrintF(PrintOutput::STDERR, "Warning: --pset: %s\n", msg);
	};
	auto require_value = [&]() {
		if (!has_value) {
			warn(StringUtil::Format("'%s' requires a value", key));
			return false;
		}
		return true;
	};
	auto parse_bool = [&](bool default_when_missing) -> bool {
		if (!has_value) {
			return default_when_missing;
		}
		if (vall == "on" || vall == "true" || vall == "1") {
			return true;
		}
		if (vall == "off" || vall == "false" || vall == "0") {
			return false;
		}
		warn(StringUtil::Format("unrecognised boolean value '%s' for '%s'; ignored", value, key));
		return default_when_missing;
	};

	// Switch render mode via SetOutputMode so the separator/header
	// side-effects (e.g. csv → ',' + CRLF) fire just like `.mode csv` would.
	auto set_mode = [&](const char *mode_name) {
		state.SetOutputMode(mode_name, nullptr);
		state.cMode = state.mode;
	};

	if (keyl == "format") {
		if (!require_value()) {
			return MetadataResult::SUCCESS;
		}
		// Resolve psql-only aliases to a duckdb mode name; everything
		// else passes through as a duckdb mode name directly. That way
		// `--pset format=csv` (psql shape) and `--pset format=markdown`
		// (duckdb shape) both work.
		string target;
		if (vall == "aligned" || vall == "wrapped") {
			target = "duckbox";
		} else if (vall == "unaligned") {
			target = "list";
		} else {
			target = vall;
		}
		static const char *const duckdb_modes[] = {"ascii",    "box",   "column",    "csv",   "duckbox", "html",
		                                           "insert",   "json",  "jsonlines", "latex", "line",    "list",
		                                           "markdown", "quote", "table",     "tabs",  "tcl",     "trash"};
		bool match = false;
		for (auto *m : duckdb_modes) {
			if (target == m) {
				match = true;
				break;
			}
		}
		if (match) {
			set_mode(target.c_str());
		} else {
			warn(StringUtil::Format("format '%s' is not supported; try one of: "
			                        "aligned/unaligned/wrapped (psql), csv/html/latex (shared), or "
			                        "box/duckbox/markdown/json/jsonlines/column/line/table/quote/insert/"
			                        "ascii/tabs/tcl/trash (duckdb); ignored",
			                        value));
		}
	} else if (keyl == "expanded" || keyl == "x") {
		// toggle when no value: turn on if not already in LINE mode
		bool on = parse_bool(/*default_when_missing=*/state.mode != RenderMode::LINE);
		set_mode(on ? "line" : "duckbox");
	} else if (keyl == "null") {
		if (!require_value()) {
			return MetadataResult::SUCCESS;
		}
		state.nullValue = value;
	} else if (keyl == "tuples_only" || keyl == "t") {
		// toggle when no value: turn on if headers currently shown
		bool on = parse_bool(/*default_when_missing=*/state.showHeader);
		if (on) {
			set_mode("list");
			state.showHeader = false;
		} else {
			set_mode("duckbox");
			state.showHeader = true;
		}
		state.ShellSetFlag(ShellFlags::SHFLG_HeaderSet);
	} else if (keyl == "pager") {
		if (!has_value || vall == "on") {
			state.pager_mode = PagerMode::PAGER_AUTOMATIC;
		} else if (vall == "always") {
			state.pager_mode = PagerMode::PAGER_ON;
		} else if (vall == "off") {
			state.pager_mode = PagerMode::PAGER_OFF;
		} else {
			warn(StringUtil::Format("pager value '%s' is not supported (try on/off/always); ignored", value));
		}
	} else {
		warn(StringUtil::Format("'%s' is not implemented; ignored", key));
	}
	return MetadataResult::SUCCESS;
}

// psql --help=interactive: list dot-commands (our equivalent of psql's
// backslash commands) and exit.
MetadataResult PsqlHelpInteractive(ShellState &state, const vector<string> &args) {
	state.DoMetaCommand(".help");
	return MetadataResult::EXIT;
}

// psql --help=interactive-all: list all dot-commands (verbose) and exit.
MetadataResult PsqlHelpInteractiveAll(ShellState &state, const vector<string> &args) {
	state.DoMetaCommand(".help --all");
	return MetadataResult::EXIT;
}

MetadataResult LaunchUI(ShellState &state, const vector<string> &args) {
	// run the UI command
	auto rc = state.RunInitialCommand((char *)state.ui_command.c_str(), true);
	if (rc != 0) {
		ShellState::Exit(rc);
		return MetadataResult::EXIT;
	}
	return MetadataResult::SUCCESS;
}

MetadataResult SetNewlineSeparator(ShellState &state, const vector<string> &args) {
	// run the UI command
	state.rowSeparator = args[1];
	return MetadataResult::SUCCESS;
}

MetadataResult SetStorageVersion(ShellState &state, const vector<string> &args) {
	auto &storage_version = args[1];
	try {
		state.config.options.storage_compatibility = duckdb::StorageCompatibility::FromString(storage_version);
	} catch (std::exception &ex) {
		duckdb::ErrorData error(ex);
		state.PrintF(PrintOutput::STDERR, "%s: Error: unknown argument (%s) for '-storage-version': %s\n",
		             state.program_name, storage_version.c_str(), error.Message().c_str());
		return MetadataResult::EXIT;
	}
	return MetadataResult::SUCCESS;
}

MetadataResult ProcessFile(ShellState &state, const vector<string> &args) {
	state.readStdin = false;
	auto &file = args[1];
	if (!state.ProcessFile(file)) {
		ShellState::Exit(1);
		return MetadataResult::EXIT;
	}
	return MetadataResult::SUCCESS;
}

MetadataResult SetInitFile(ShellState &state, const vector<string> &args) {
	state.initFile = args[1];
	return MetadataResult::SUCCESS;
}

MetadataResult SkipInit(ShellState &state, const vector<string> &args) {
	state.run_init = false;
	return MetadataResult::SUCCESS;
}

template <bool EXIT>
MetadataResult RunCommand(ShellState &state, const vector<string> &args) {
	if (EXIT) {
		state.readStdin = false;
	}
	// Always bail if -c or -s fail
	bool bail = true;
	if (state.bail != BailOnError::AUTOMATIC) {
		bail = state.bail == BailOnError::BAIL_ON_ERROR;
	}
	auto &cmd = args[1];
	auto rc = state.RunInitialCommand(cmd.c_str(), bail);
	if (rc != 0) {
		ShellState::Exit(rc);
		return MetadataResult::EXIT;
	}
	return MetadataResult::SUCCESS;
}

MetadataResult FormatStdin(ShellState &state, const vector<string> &args) {
	state.readStdin = false;

	if (duckdb::Terminal::IsAtty()) {
		state.PrintF(PrintOutput::STDERR,
		             "%s: Error: -format requires SQL input on stdin (e.g. echo 'SELECT 1' | duckdb -format)\n",
		             state.program_name);
		return MetadataResult::FAIL;
	}

	// Read all of stdin into a string.
	string sql = state.ReadFileContents(stdin);

	auto result = state.FormatSQL(sql);
	if (result != MetadataResult::SUCCESS) {
		return result;
	}

	// Write formatted SQL to stdout, with syntax highlighting if stdout is a terminal.
	state.HighlightSQL(sql);
	state.Print(PrintOutput::STDOUT, sql);
	return MetadataResult::SUCCESS;
}

MetadataResult FormatFile(ShellState &state, const vector<string> &args) {
	state.readStdin = false;
	const string &filename = args[1];

	string sql = state.ReadFileContents(filename);

	auto result = state.FormatSQL(sql);
	if (result != MetadataResult::SUCCESS) {
		return result;
	}
	state.HighlightSQL(sql);
	state.Print(PrintOutput::STDOUT, sql);
	return MetadataResult::SUCCESS;
}

// psql-mode-only options. Looked up before `command_line_options` when
// `state.subcommand == ShellSubcommand::PSQL`. Long-form entries (e.g.
// "host") sit alongside short forms ("h") so both `--host=` and `-h `
// parse identically; the long-form `--foo=value` shorthand is handled
// at the argv loop in RunShell.
static const CommandLineOption psql_options[] = {
    {"h", 1, "HOSTNAME", SetPsqlHost, nullptr, "(psql) server host (default: $PGHOST or 'localhost')"},
    {"host", 1, "HOSTNAME", SetPsqlHost, nullptr, "(psql) server host"},
    {"p", 1, "PORT", SetPsqlPort, nullptr, "(psql) server port (default: $PGPORT or '5432')"},
    {"port", 1, "PORT", SetPsqlPort, nullptr, "(psql) server port"},
    {"d", 1, "DBNAME", SetPsqlDbname, nullptr, "(psql) database name (default: $PGDATABASE or USERNAME)"},
    {"dbname", 1, "DBNAME", SetPsqlDbname, nullptr, "(psql) database name"},
    {"U", 1, "USERNAME", SetPsqlUser, nullptr, "(psql) user name (default: $PGUSER, $USER, or 'postgres')"},
    {"username", 1, "USERNAME", SetPsqlUser, nullptr, "(psql) user name"},
    {"w", 0, "", PsqlPasswordNoOp, nullptr, "(psql) accepted, ignored (no authentication yet)"},
    {"no-password", 0, "", PsqlPasswordNoOp, nullptr, "(psql) accepted, ignored"},
    {"W", 0, "", PsqlPasswordNoOp, nullptr, "(psql) accepted, ignored (no authentication yet)"},
    {"password", 0, "", PsqlPasswordNoOp, nullptr, "(psql) accepted, ignored"},
    // Help / version: psql spells them -? and -V.
    {"?", 0, "", EnableBatch, PrintHelpAndExit, "(psql) show help message"},
    {"help", 0, "", EnableBatch, PrintHelpAndExit, "(psql) show help message"},
    {"V", 0, "", nullptr, ShowVersionAndExit, "(psql) show SereneDB version"},
    {"version", 0, "", nullptr, ShowVersionAndExit, "(psql) show SereneDB version"},
    // Long-form aliases for shell options that already exist short-form.
    {"command", 1, "COMMAND", EnableBatch, RunCommand<true>, "(psql) run COMMAND and exit"},
    {"file", 1, "FILENAME", EnableBatch, ProcessFile, "(psql) read/process named file and exit"},
    // Echo flags: both -a and -e map to the same shell-level toggle.
    {"a", 0, "", nullptr, EnableEcho, "(psql) echo all input from script"},
    {"echo-all", 0, "", nullptr, EnableEcho, "(psql) echo all input from script"},
    {"e", 0, "", nullptr, EnableEcho, "(psql) echo commands sent to server"},
    {"echo-queries", 0, "", nullptr, EnableEcho, "(psql) echo commands sent to server"},
    // Output format.
    {"A", 0, "", nullptr, ToggleOutputMode<RenderMode::LIST>, "(psql) unaligned table output mode"},
    {"no-align", 0, "", nullptr, ToggleOutputMode<RenderMode::LIST>, "(psql) unaligned table output mode"},
    {"csv", 0, "", nullptr, ToggleCSVMode, "(psql) CSV output mode"},
    {"H", 0, "", nullptr, ToggleOutputMode<RenderMode::HTML>, "(psql) HTML output mode"},
    {"html", 0, "", nullptr, ToggleOutputMode<RenderMode::HTML>, "(psql) HTML output mode"},
    {"t", 0, "", nullptr, PsqlTuplesOnly, "(psql) print rows only (no column headers)"},
    {"tuples-only", 0, "", nullptr, PsqlTuplesOnly, "(psql) print rows only (no column headers)"},
    {"x", 0, "", nullptr, ToggleOutputMode<RenderMode::LINE>, "(psql) one value per line (expanded)"},
    {"expanded", 0, "", nullptr, ToggleOutputMode<RenderMode::LINE>, "(psql) one value per line (expanded)"},
    {"F", 1, "STRING", nullptr, ShellState::SetSeparator, "(psql) field separator for unaligned output"},
    {"field-separator", 1, "STRING", nullptr, ShellState::SetSeparator, "(psql) field separator for unaligned output"},
    {"R", 1, "STRING", nullptr, SetNewlineSeparator, "(psql) record separator for unaligned output"},
    {"record-separator", 1, "STRING", nullptr, SetNewlineSeparator, "(psql) record separator for unaligned output"},
    {"o", 1, "FILENAME", nullptr, PsqlOutputFile, "(psql) send query results to FILE"},
    {"output", 1, "FILENAME", nullptr, PsqlOutputFile, "(psql) send query results to FILE"},
    {"X", 0, "", SkipInit, nullptr, "(psql) do not read startup file (~/.duckdbrc)"},
    {"no-psqlrc", 0, "", SkipInit, nullptr, "(psql) do not read startup file (~/.duckdbrc)"},
    {"l", 0, "", EnableBatch, PsqlListDatabases, "(psql) list available databases, then exit"},
    {"list", 0, "", EnableBatch, PsqlListDatabases, "(psql) list available databases, then exit"},
    {"1", 0, "", PsqlSingleTransaction, nullptr, "(psql) execute as a single transaction (if non-interactive)"},
    {"single-transaction", 0, "", PsqlSingleTransaction, nullptr,
     "(psql) execute as a single transaction (if non-interactive)"},
    {"q", 0, "", PsqlQuiet, nullptr, "(psql) run quietly (suppress startup messages)"},
    {"quiet", 0, "", PsqlQuiet, nullptr, "(psql) run quietly (suppress startup messages)"},
    {"n", 0, "", PsqlNoReadline, nullptr, "(psql) disable enhanced command line editing (readline)"},
    {"no-readline", 0, "", PsqlNoReadline, nullptr, "(psql) disable enhanced command line editing (readline)"},
    // psql -b echoes failed commands only; we don't gate echo on success
    // so this maps onto plain echo (also enabled by -e/-a).
    {"b", 0, "", nullptr, EnableEcho, "(psql) echo failed commands"},
    {"echo-errors", 0, "", nullptr, EnableEcho, "(psql) echo failed commands"},
    {"z", 0, "", PsqlFieldSepZero, nullptr, "(psql) set field separator for unaligned output to zero byte"},
    {"field-separator-zero", 0, "", PsqlFieldSepZero, nullptr,
     "(psql) set field separator for unaligned output to zero byte"},
    {"0", 0, "", PsqlRecordSepZero, nullptr, "(psql) set record separator for unaligned output to zero byte"},
    {"record-separator-zero", 0, "", PsqlRecordSepZero, nullptr,
     "(psql) set record separator for unaligned output to zero byte"},
    // Accepted-and-ignored for psql-script compatibility: there is no
    // serened equivalent for these knobs yet.
    {"E", 0, "", PsqlAcceptIgnore, nullptr, "(psql) accepted, ignored (display of internal queries not supported)"},
    {"echo-hidden", 0, "", PsqlAcceptIgnore, nullptr,
     "(psql) accepted, ignored (display of internal queries not supported)"},
    {"L", 1, "FILENAME", PsqlAcceptIgnore, nullptr, "(psql) accepted, ignored (session log not supported)"},
    {"log-file", 1, "FILENAME", PsqlAcceptIgnore, nullptr, "(psql) accepted, ignored (session log not supported)"},
    {"s", 0, "", PsqlAcceptIgnore, nullptr, "(psql) accepted, ignored (single-step mode not supported)"},
    {"single-step", 0, "", PsqlAcceptIgnore, nullptr, "(psql) accepted, ignored (single-step mode not supported)"},
    {"S", 0, "", PsqlAcceptIgnore, nullptr, "(psql) accepted, ignored (single-line mode not supported)"},
    {"single-line", 0, "", PsqlAcceptIgnore, nullptr, "(psql) accepted, ignored (single-line mode not supported)"},
    // -P / --pset: implemented for format/expanded/null/tuples_only/pager;
    // unknown keys emit a warning to stderr but don't error out.
    {"P", 1, "VAR", PsqlPset, nullptr, "(psql) set printing option (format/expanded/null/tuples_only/pager)"},
    {"pset", 1, "VAR", PsqlPset, nullptr, "(psql) set printing option (format/expanded/null/tuples_only/pager)"},
    {"T", 1, "TEXT", PsqlAcceptIgnore, nullptr, "(psql) accepted, ignored (HTML table attrs not modelled)"},
    {"table-attr", 1, "TEXT", PsqlAcceptIgnore, nullptr, "(psql) accepted, ignored (HTML table attrs not modelled)"},
    // Help targets: psql uses `--help=commands` and `--help=variables`
    // for backslash commands and special variables. We don't have psql
    // variables; we expose `.help` / `.help --all` as the closest fit.
    // Names: "interactive" (per dot-command's interactive use) and
    // "interactive-all" (verbose listing). "commands" is psql's own
    // namespace so we deliberately don't reuse it.
    {"help=interactive", 0, "", EnableBatch, PsqlHelpInteractive, "(psql) list interactive (dot) commands, then exit"},
    {"help=interactive-all", 0, "", EnableBatch, PsqlHelpInteractiveAll,
     "(psql) list all interactive (dot) commands, then exit"},
    {nullptr, 0, nullptr, nullptr, nullptr, nullptr}};

static const CommandLineOption command_line_options[] = {
    {"ascii", 0, "", nullptr, ToggleASCIIMode, "set output mode to 'ascii'"},
    {"bail", 0, "", nullptr, EnableBail, "stop after hitting an error"},
    {"batch", 0, "", EnableBatch, EnableBatch, "force batch I/O"},
    {"box", 0, "", nullptr, ToggleOutputMode<RenderMode::BOX>, "set output mode to 'box'"},
    {"column", 0, "", nullptr, ToggleOutputMode<RenderMode::COLUMN>, "set output mode to 'column'"},
    {"cmd", 1, "COMMAND", nullptr, RunCommand<false>, "run \"COMMAND\" before reading stdin"},
    {"csv", 0, "", nullptr, ToggleCSVMode, "set output mode to 'csv'"},
    {"c", 1, "COMMAND", EnableBatch, RunCommand<true>, "run \"COMMAND\" and exit"},
    {"command", 1, "COMMAND", EnableBatch, RunCommand<true>, "alias for -c"},
    {"echo", 0, "", nullptr, EnableEcho, "print commands before execution"},
    {"f", 1, "FILENAME", EnableBatch, ProcessFile, "read/process named file and exit"},
    {"file", 1, "FILENAME", EnableBatch, ProcessFile, "alias for -f"},
    {"format", 0, "", EnableBatch, FormatStdin, "format SQL from stdin, writing result to stdout"},
    {"format-file", 1, "FILENAME", EnableBatch, FormatFile, "format SQL in file, writing result to stdout"},
    {"init", 1, "FILENAME", SetInitFile, nullptr, "read/process named file"},
    {"header", 0, "", nullptr, ToggleHeader<true>, "turn headers on"},
    {"h", 0, "", EnableBatch, PrintHelpAndExit, "show help message"},
    {"help", 0, "", EnableBatch, PrintHelpAndExit, "show help message"},
    {"html", 0, "", nullptr, ToggleOutputMode<RenderMode::HTML>, "set output mode to HTML"},
    {"interactive", 0, "", nullptr, DisableBatch, "force interactive I/O"},
    {"json", 0, "", nullptr, ToggleOutputMode<RenderMode::JSON>, "set output mode to 'json'"},
    {"jsonlines", 0, "", nullptr, ToggleOutputMode<RenderMode::JSONLINES>, "set output mode to 'jsonlines'"},
    {"line", 0, "", nullptr, ToggleOutputMode<RenderMode::LINE>, "set output mode to 'line'"},
    {"list", 0, "", nullptr, ToggleOutputMode<RenderMode::LIST>, "set output mode to 'list'"},
    {"markdown", 0, "", nullptr, ToggleOutputMode<RenderMode::MARKDOWN>, "set output mode to 'markdown'"},
    {"newline", 1, "SEP", nullptr, SetNewlineSeparator, "set output row separator. Default: '\\n'"},
    {"no-init", 0, "", SkipInit, nullptr, "skip processing the init file"},
    {"no-stdin", 0, "", nullptr, DisableStdin, "exit after processing options instead of reading stdin"},
    {"noheader", 0, "", nullptr, ToggleHeader<false>, "turn headers off"},
    {"nullvalue", 1, "TEXT", nullptr, ShellState::SetNullValue, "set text string for NULL values. Default 'NULL'"},
    {"quote", 0, "", nullptr, ToggleOutputMode<RenderMode::QUOTE>, "set output mode to 'quote'"},
    {"readonly", 0, "", SetReadOnlyMode, nullptr, "open the database read-only"},
    {"s", 1, "COMMAND", EnableBatch, RunCommand<true>, "run \"COMMAND\" and exit"},
    {"safe", 0, "", ShellState::EnableSafeMode, nullptr, "enable safe-mode"},
    {"separator", 1, "SEP", nullptr, ShellState::SetSeparator, "set output column separator. Default: '|'"},
    {"storage-version", 1, "VER", SetStorageVersion, nullptr,
     "database storage compatibility version to use. Default: 'v0.10.0'"},
    {"table", 0, "", nullptr, ToggleOutputMode<RenderMode::TABLE>, "set output mode to 'table'"},
    {"ui", 0, "", nullptr, LaunchUI, "launches a web interface using the ui extension (configurable with .ui_command)"},
    {"unredacted", 0, "", AllowUnredacted, nullptr, "allow printing unredacted secrets"},
    {"unsigned", 0, "", AllowUnsigned, nullptr, "allow loading of unsigned extensions"},
    {"version", 0, "", nullptr, ShowVersionAndExit, "show SereneDB version"},
    {"V", 0, "", nullptr, ShowVersionAndExit, "alias for --version"},
    {nullptr, 0, nullptr, nullptr, nullptr, nullptr}};

static const CommandLineOption *FindInTable(const CommandLineOption *table, const char *name) {
	for (idx_t c = 0; table[c].option; c++) {
		if (StringUtil::Equals(name, table[c].option)) {
			return &table[c];
		}
	}
	return nullptr;
}

// psql connection-related options. Available only in PSQL mode -- in
// SHELL mode they'd be meaningless (no auto-connect) and -h is reserved
// for shell mode's "help" alias.
static bool IsConnectionOption(const char *name) {
	for (const char *conn :
	     {"h", "host", "p", "port", "d", "dbname", "U", "username", "w", "no-password", "W", "password"}) {
		if (StringUtil::Equals(name, conn)) {
			return true;
		}
	}
	return false;
}

static const CommandLineOption *FindOptionPtr(const char *name, ShellSubcommand subcommand) {
	if (subcommand == ShellSubcommand::PSQL) {
		// psql-mode flags ('-h' = host, '-V' = version, '-?' = help) take
		// precedence; fall back to the shell table for shared things.
		if (auto *opt = FindInTable(psql_options, name)) {
			return opt;
		}
		return FindInTable(command_line_options, name);
	}
	// SHELL mode: shell table first (so -h stays help, -list stays
	// "list output mode"); fall back to psql table for psql-style
	// names (-x, --no-align, --tuples-only, -P/--pset, -1, ...). But
	// connection options are masked off -- shell doesn't connect.
	if (auto *opt = FindInTable(command_line_options, name)) {
		return opt;
	}
	if (IsConnectionOption(name)) {
		return nullptr;
	}
	return FindInTable(psql_options, name);
}

// Display convention: single-char names get one dash (`-c`), multi-char
// names get two (`--cmd`). Both forms remain accepted by the parser; this
// only controls how help text and "Did you mean" suggestions are spelled.
static const char *DashPrefix(const char *name) {
	return (name[0] != '\0' && name[1] == '\0') ? "-" : "--";
}

// Subcommand-aware "Run X --help" footer for error messages.
static string HelpHintLine(const ShellState &state) {
	const char *display = (state.subcommand == ShellSubcommand::PSQL) ? "serened psql" : "serened shell";
	return StringUtil::Format("Run '%s --help' for a list of options.\n", display);
}

optional_ptr<const CommandLineOption> ShellState::FindCommandLineOption(const string &option, const string &raw_arg,
                                                                        string &error_msg) const {
	const CommandLineOption *opt = FindOptionPtr(option.c_str(), subcommand);
	if (!opt) {
		// Legacy alias: underscores map to dashes (e.g. -storage_version).
		auto option_name = StringUtil::Replace(option, "_", "-");
		opt = FindOptionPtr(option_name.c_str(), subcommand);
	}
	if (!opt) {
		// Echo the user-typed form (preserves dash count: `--foo`, `---`, etc).
		const string display_arg = raw_arg.empty() ? string("-") + option : raw_arg;
		error_msg = StringUtil::Format("Unknown Option Error: Unrecognized option '%s'\n", display_arg);
		vector<string> option_names;
		// Both modes see both tables for suggestions. Connection options
		// are hidden in SHELL mode (no auto-connect in shell mode).
		for (idx_t c = 0; psql_options[c].option; c++) {
			if (subcommand != ShellSubcommand::PSQL && IsConnectionOption(psql_options[c].option)) {
				continue;
			}
			option_names.push_back(string(DashPrefix(psql_options[c].option)) + psql_options[c].option);
		}
		for (idx_t c = 0; command_line_options[c].option; c++) {
			option_names.push_back(string(DashPrefix(command_line_options[c].option)) + command_line_options[c].option);
		}
		// Match what the user typed when computing candidate suggestions
		// (so `--foo` looks for similar `--xxx` names, not `-xxx`).
		auto candidates_msg = StringUtil::CandidatesErrorMessage(option_names, display_arg, "Did you mean");
		error_msg += candidates_msg + "\n";
		error_msg += HelpHintLine(*this);
		return nullptr;
	}
	return opt;
}

// Build the displayed program invocation for help/usage output.
static string DisplayProgramName(const ShellState &state) {
	return (state.subcommand == ShellSubcommand::PSQL) ? "serened psql" : "serened shell";
}

// psql help: column at which descriptions start (matches real psql).
static constexpr idx_t PSQL_DESC_COL = 27;

// Print one psql help line. `lhs_template` is the LHS with placeholder
// values wrapped in `<>` markers, e.g. "  -c, --command=<COMMAND>" or
// "  -?, --help[=<options>]". Text outside markers is rendered as
// KEYWORD (flag name, '=', brackets); text inside markers is rendered as
// STRING_CONSTANT (only the bare placeholder gets the value colour).
// The `<` / `>` markers themselves are stripped from output but counted
// out of the LHS width.
//
// Description text is aligned to PSQL_DESC_COL; embedded '\n' splits
// into continuation lines, all indented to the same column. If the LHS
// is already wider than PSQL_DESC_COL, the description starts on the
// next line (matches psql).
static void PrintPsqlEntry(ShellState &state, ShellHighlight &h, const string &lhs_template, const string &desc) {
	idx_t lhs_len = 0;
	idx_t i = 0;
	while (i < lhs_template.size()) {
		auto lt = lhs_template.find('<', i);
		if (lt == string::npos) {
			string chunk = lhs_template.substr(i);
			h.PrintText(chunk, PrintOutput::STDOUT, HighlightElementType::KEYWORD);
			lhs_len += chunk.size();
			break;
		}
		if (lt > i) {
			string chunk = lhs_template.substr(i, lt - i);
			h.PrintText(chunk, PrintOutput::STDOUT, HighlightElementType::KEYWORD);
			lhs_len += chunk.size();
		}
		auto gt = lhs_template.find('>', lt + 1);
		if (gt == string::npos) {
			// no closing '>' -- treat rest as keyword (defensive)
			string chunk = lhs_template.substr(lt);
			h.PrintText(chunk, PrintOutput::STDOUT, HighlightElementType::KEYWORD);
			lhs_len += chunk.size();
			break;
		}
		string value = lhs_template.substr(lt + 1, gt - lt - 1);
		h.PrintText(value, PrintOutput::STDOUT, HighlightElementType::STRING_CONSTANT);
		lhs_len += value.size();
		i = gt + 1;
	}

	auto lines = StringUtil::Split(desc, '\n');
	const string indent(PSQL_DESC_COL, ' ');
	for (idx_t row = 0; row < lines.size(); row++) {
		if (row == 0) {
			if (lhs_len >= PSQL_DESC_COL) {
				state.PrintF("\n%s", indent.c_str());
			} else {
				state.PrintF("%s", string(PSQL_DESC_COL - lhs_len, ' ').c_str());
			}
		} else {
			state.PrintF("%s", indent.c_str());
		}
		state.PrintF("%s\n", lines[row].c_str());
	}
}

static void PrintPsqlSectionHeader(ShellHighlight &h, const char *name) {
	h.PrintText(string(name) + "\n", PrintOutput::STDOUT, PrintColor::STANDARD, PrintIntensity::BOLD);
}

// Entries used by BOTH shell and psql help, in section order. Differences
// between modes (e.g. `-s`, `-l`) are handled by separate per-mode entries.
static void PrintInputOutputSection(ShellState &state, ShellHighlight &h, ShellSubcommand mode) {
	auto entry = [&](const string &lhs, const string &desc) {
		PrintPsqlEntry(state, h, lhs, desc);
	};
	PrintPsqlSectionHeader(h, "Input and output options:");
	entry("  -a, --echo-all", "echo all input from script");
	entry("  -b, --echo-errors", "echo failed commands");
	entry("  -e, --echo-queries", "echo commands sent to server");
	entry("  -E, --echo-hidden", "(psql) accepted, ignored (no equivalent)");
	entry("  -L, --log-file=<FILENAME>", "(psql) accepted, ignored (session log not supported)");
	entry("  -n, --no-readline", "disable enhanced command line editing (readline)");
	entry("  -o, --output=<FILENAME>", "send query results to file (or |pipe)");
	entry("  -q, --quiet", "run quietly (no messages, only query output)");
	if (mode == ShellSubcommand::PSQL) {
		entry("  -s, --single-step", "(psql) accepted, ignored (single-step mode not supported)");
	} else {
		// In SHELL mode -s resolves to the shell's RunCommand<true> (alias of -c).
		entry("  -s <COMMAND>", "alias for -c (run COMMAND and exit)");
	}
	entry("  -S, --single-line", "(psql) accepted, ignored (single-line mode not supported)");
	entry("      --interactive", "force interactive I/O");
	entry("      --batch", "force batch I/O");
	entry("      --no-stdin", "exit after processing options (don't read stdin)");
	state.PrintF("\n");
}

static void PrintOutputFormatSection(ShellState &state, ShellHighlight &h) {
	auto entry = [&](const string &lhs, const string &desc) {
		PrintPsqlEntry(state, h, lhs, desc);
	};
	PrintPsqlSectionHeader(h, "Output format options:");
	entry("  -A, --no-align", "unaligned table output mode");
	entry("      --csv", "CSV (Comma-Separated Values) table output mode");
	entry("  -F, --field-separator=<STRING>", "field separator for unaligned output (default: \"|\")");
	entry("      --separator=<SEP>", "alias for --field-separator");
	entry("  -H, --html", "HTML table output mode");
	entry("  -P, --pset=<VAR>[=<ARG>]", "set printing option (format/expanded/null/tuples_only/pager)");
	entry("  -R, --record-separator=<STRING>", "record separator for unaligned output (default: newline)");
	entry("      --newline=<SEP>", "alias for --record-separator");
	entry("  -t, --tuples-only / --noheader", "print rows only (no column headers)");
	entry("      --header", "force column headers on");
	entry("  -T, --table-attr=<TEXT>", "(psql) accepted, ignored (HTML attrs not modelled)");
	entry("  -x, --expanded", "turn on expanded table output");
	entry("  -z, --field-separator-zero", "set field separator for unaligned output to zero byte");
	entry("  -0, --record-separator-zero", "set record separator for unaligned output to zero byte");
	entry("      --nullvalue=<TEXT>", "text string for NULL values (default 'NULL')");
	entry("      --<MODE>", "set mode directly. MODE is one of:\nascii, box, column, csv, duckbox, html, json,\n"
	                        "jsonlines, latex, line, list, markdown, quote,\ntable, tabs, tcl, trash");
	state.PrintF("\n");
}

static void PrintDatabaseSection(ShellState &state, ShellHighlight &h) {
	auto entry = [&](const string &lhs, const string &desc) {
		PrintPsqlEntry(state, h, lhs, desc);
	};
	PrintPsqlSectionHeader(h, "Database options:");
	entry("      --readonly", "open the database read-only");
	entry("      --safe", "enable safe-mode");
	entry("      --storage-version=<VER>", "database storage compatibility version (default: 'v0.10.0')");
	entry("      --unsigned", "allow loading of unsigned extensions");
	entry("      --unredacted", "allow printing unredacted secrets");
	entry("      --ui", "launch a web UI via the ui extension");
	state.PrintF("\n");
}

static void PrintShellSections(ShellState &state) {
	ShellHighlight h(state);
	auto entry = [&](const string &lhs, const string &desc) {
		PrintPsqlEntry(state, h, lhs, desc);
	};

	PrintPsqlSectionHeader(h, "General options:");
	entry("  -c, --command=<COMMAND>", "run only single command (SQL or internal) and exit");
	entry("  -f, --file=<FILENAME>", "execute commands from file, then exit");
	entry("      --cmd=<COMMAND>", "run COMMAND before reading stdin (pre-init, does not exit)");
	entry("      --init=<FILENAME>", "pre-init from file (overrides default ~/.duckdbrc lookup)");
	entry("  -X, --no-psqlrc / --no-init", "do not read startup file (~/.duckdbrc)");
	entry("      --bail", "stop after hitting an error");
	entry("  -1 (\"one\"), --single-transaction", "wrap -c/-f batch in BEGIN/COMMIT (if non-interactive)");
	entry("      --format", "format SQL from stdin, writing result to stdout");
	entry("      --format-file=<FILENAME>", "format SQL in file, writing result to stdout");
	entry("  -V, --version", "output version information, then exit");
	entry("  -?, --help[=<options>]", "show this help, then exit");
	entry("      --help=interactive", "list interactive (dot) commands, then exit");
	entry("      --help=interactive-all", "list all interactive (dot) commands, then exit");
	state.PrintF("\n");

	PrintInputOutputSection(state, h, ShellSubcommand::SHELL);
	PrintOutputFormatSection(state, h);
	PrintDatabaseSection(state, h);
}

static void PrintPsqlSections(ShellState &state) {
	ShellHighlight h(state);
	auto entry = [&](const string &lhs, const string &desc) {
		PrintPsqlEntry(state, h, lhs, desc);
	};

	PrintPsqlSectionHeader(h, "General options:");
	entry("  -c, --command=<COMMAND>", "run only single command (SQL or internal) and exit");
	entry("  -d, --dbname=<DBNAME>", "database name (default: $PGDATABASE or USERNAME)");
	entry("  -f, --file=<FILENAME>", "execute commands from file, then exit");
	entry("      --cmd=<COMMAND>", "run COMMAND before reading stdin (pre-init, does not exit)");
	entry("      --init=<FILENAME>", "pre-init from file (overrides default ~/.duckdbrc lookup)");
	entry("      --bail", "stop after hitting an error");
	entry("      --format", "format SQL from stdin, writing result to stdout");
	entry("      --format-file=<FILENAME>", "format SQL in file, writing result to stdout");
	entry("  -l, --list", "list available databases, then exit");
	entry("  -V, --version", "output version information, then exit");
	entry("  -X, --no-psqlrc / --no-init", "do not read startup file (~/.duckdbrc)");
	entry("  -1 (\"one\"), --single-transaction", "execute as a single transaction (if non-interactive)");
	entry("  -?, --help[=<options>]", "show this help, then exit");
	entry("      --help=interactive", "list interactive (dot) commands, then exit");
	entry("      --help=interactive-all", "list all interactive (dot) commands, then exit");
	state.PrintF("\n");

	PrintInputOutputSection(state, h, ShellSubcommand::PSQL);
	PrintOutputFormatSection(state, h);
	PrintDatabaseSection(state, h);

	PrintPsqlSectionHeader(h, "Connection options:");
	entry("  -h, --host=<HOSTNAME>",
	      "server host (default: $PGHOST or \"localhost\";\npsql defaults to a Unix socket, serened to TCP)");
	entry("  -p, --port=<PORT>", "server port (default: $PGPORT or \"5432\")");
	entry("  -U, --username=<USERNAME>", "user name (default: $PGUSER, $USER, or \"postgres\")");
	entry("  -w, --no-password", "accepted, ignored (no authentication yet)");
	entry("  -W, --password", "accepted, ignored (no authentication yet)");
	state.PrintF("\n");
}

// Per-subcommand "Notes" block printed before the shared footer.
// Highlights what's different from psql / DuckDB shell so users coming
// from either side aren't surprised.
static void PrintNotes(ShellState &state, ShellHighlight &h) {
	PrintPsqlSectionHeader(h, "Notes:");
	if (state.subcommand == ShellSubcommand::PSQL) {
		state.PrintF("  * pg-wire client; auto-connects on startup (host/port/dbname/user\n"
		             "    resolved from flags or PG* env vars). For a local-only shell, use\n"
		             "    `serened shell`.\n"
		             "  * Accepts DuckDB shell flags too -- --cmd, --init, --bail, --format,\n"
		             "    --MODE, the whole Database options section, and more.\n"
		             "  * In this mode: -h is host, -l lists databases, -s is single-step\n"
		             "    (no-op). All three mean something different in `serened shell`.\n"
		             "  * Default output is `duckbox` (Unicode + type row), not psql's aligned\n"
		             "    ASCII. Switch with `--pset format=aligned`.\n"
		             "  * Not implemented yet: -v / --set / --variable (psql variables),\n"
		             "    --help=commands, --help=variables, and backslash commands (\\d, \\c,\n"
		             "    \\copy, \\timing, ...).\n");
	} else {
		state.PrintF("  * Local shell -- no auto-connect. For a pg-wire client, use\n"
		             "    `serened psql`.\n"
		             "  * Accepts psql-style flags too -- -A, -x, -t, -P, -1, --no-align,\n"
		             "    --expanded, --tuples-only, --pset, --single-transaction, and more.\n"
		             "  * In this mode: -h is help, -l is `list output mode`, -s is alias for\n"
		             "    -c. All three mean something different in `serened psql`.\n"
		             "  * Default output is `duckbox` (Unicode + type row); change with\n"
		             "    `--pset format=...` or any `--MODE` short flag.\n");
	}
	state.PrintF("\n");
}

static void PrintFooter(ShellState &state) {
	state.PrintF("For more information, type \".help\" (for dot-commands) or \".help --all\"\n"
	             "(for the full listing) from within serened, or consult the SereneDB\n"
	             "documentation.\n"
	             "\n"
	             "Report bugs at <https://github.com/serenedb/serenedb/issues>.\n"
	             "SereneDB home page: <https://serenedb.com>\n");
}

void ShellState::PrintUsage() {
	ShellHighlight highlighter(*this);
	const auto display = DisplayProgramName(*this);

	if (subcommand == ShellSubcommand::PSQL) {
		PrintF("serened psql is a PostgreSQL-compatible client for SereneDB.\n\n");
		highlighter.PrintText("Usage:\n  ", PrintOutput::STDOUT, PrintColor::STANDARD, PrintIntensity::BOLD);
		highlighter.PrintText(display, PrintOutput::STDOUT, HighlightElementType::KEYWORD);
		highlighter.PrintText(" [OPTION]... [DBNAME [USERNAME]]\n\n", PrintOutput::STDOUT,
		                      HighlightElementType::STRING_CONSTANT);
		PrintPsqlSections(*this);
		PrintNotes(*this, highlighter);
		PrintFooter(*this);
		ShellState::Exit(0);
	}

	// SHELL mode: local-only superset of upstream DuckDB shell with
	// psql-style flag spellings accepted too. Positional is a duckdb
	// database file (defaults to :memory:); optional second positional
	// is SQL run before the REPL starts.
	PrintF("serened shell is the local interactive shell for SereneDB.\n\n");
	highlighter.PrintText("Usage:\n  ", PrintOutput::STDOUT, PrintColor::STANDARD, PrintIntensity::BOLD);
	highlighter.PrintText(display, PrintOutput::STDOUT, HighlightElementType::KEYWORD);
	highlighter.PrintText(" [OPTION]... [FILENAME [SQL]]\n\n", PrintOutput::STDOUT,
	                      HighlightElementType::STRING_CONSTANT);
	PrintF("FILENAME is the path to a DuckDB database file. A new file is created\n"
	       "if it does not exist; omit it (or pass \":memory:\") for an in-memory database.\n"
	       "SQL is an optional command run before the REPL starts.\n\n");
	PrintShellSections(*this);
	PrintNotes(*this, highlighter);
	PrintFooter(*this);
	ShellState::Exit(0);
}

} // namespace duckdb_shell
