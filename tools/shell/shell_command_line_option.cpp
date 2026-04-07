#include "shell_state.hpp"
#include "shell_highlight.hpp"
#include "terminal.hpp"

namespace duckdb_shell {

// FIXME: should be moved out of a define
#define SEP_Unit   "\x1F"
#define SEP_Record "\x1E"

using duckdb::BaseShellState;
using duckdb::ShellCommandResult;

template <RenderMode output_mode>
ShellCommandResult ToggleOutputMode(BaseShellState &base_state, const vector<string> &args) {
	auto &state = static_cast<ShellState &>(base_state);
	state.cMode = state.mode = output_mode;
	return ShellCommandResult::SUCCESS;
}

ShellCommandResult ToggleASCIIMode(BaseShellState &base_state, const vector<string> &args) {
	auto &state = static_cast<ShellState &>(base_state);
	state.cMode = state.mode = RenderMode::ASCII;
	state.colSeparator = SEP_Unit;
	state.rowSeparator = SEP_Record;
	return ShellCommandResult::SUCCESS;
}

ShellCommandResult ToggleCSVMode(BaseShellState &base_state, const vector<string> &args) {
	auto &state = static_cast<ShellState &>(base_state);
	state.cMode = state.mode = RenderMode::CSV;
	state.colSeparator = ",";
	return ShellCommandResult::SUCCESS;
}

ShellCommandResult EnableBail(BaseShellState &base_state, const vector<string> &args) {
	auto &state = static_cast<ShellState &>(base_state);
	state.bail = BailOnError::BAIL_ON_ERROR;
	return ShellCommandResult::SUCCESS;
}

ShellCommandResult EnableBatch(BaseShellState &base_state, const vector<string> &args) {
	auto &state = static_cast<ShellState &>(base_state);
	state.stdin_is_interactive = false;
	return ShellCommandResult::SUCCESS;
}

ShellCommandResult DisableBatch(BaseShellState &base_state, const vector<string> &args) {
	auto &state = static_cast<ShellState &>(base_state);
	state.stdin_is_interactive = true;
	return ShellCommandResult::SUCCESS;
}

ShellCommandResult SetReadOnlyMode(BaseShellState &base_state, const vector<string> &args) {
	auto &state = static_cast<ShellState &>(base_state);
	state.config.options.access_mode = duckdb::AccessMode::READ_ONLY;
	return ShellCommandResult::SUCCESS;
}

template <bool HEADER>
ShellCommandResult ToggleHeader(BaseShellState &base_state, const vector<string> &args) {
	auto &state = static_cast<ShellState &>(base_state);
	state.showHeader = HEADER;
	return ShellCommandResult::SUCCESS;
}

ShellCommandResult DisableStdin(BaseShellState &base_state, const vector<string> &args) {
	auto &state = static_cast<ShellState &>(base_state);
	state.readStdin = false;
	return ShellCommandResult::SUCCESS;
}

ShellCommandResult EnableEcho(BaseShellState &base_state, const vector<string> &args) {
	auto &state = static_cast<ShellState &>(base_state);
	state.ShellSetFlag(ShellFlags::SHFLG_Echo);
	return ShellCommandResult::SUCCESS;
}

ShellCommandResult AllowUnredacted(BaseShellState &base_state, const vector<string> &args) {
	auto &state = static_cast<ShellState &>(base_state);
	state.config.SetOptionByName("allow_unredacted_secrets", true);
	return ShellCommandResult::SUCCESS;
}

ShellCommandResult AllowUnsigned(BaseShellState &base_state, const vector<string> &args) {
	auto &state = static_cast<ShellState &>(base_state);
	state.config.SetOptionByName("allow_unsigned_extensions", true);
	return ShellCommandResult::SUCCESS;
}

ShellCommandResult ShowVersionAndExit(BaseShellState &, const vector<string> &args) {
	printf("%s (%s) %s\n", duckdb::DuckDB::LibraryVersion(), duckdb::DuckDB::ReleaseCodename(),
	       duckdb::DuckDB::SourceID());
	return ShellCommandResult::EXIT;
}

ShellCommandResult PrintHelpAndExit(BaseShellState &base_state, const vector<string> &args) {
	auto &state = static_cast<ShellState &>(base_state);
	state.PrintUsage();
	return ShellCommandResult::EXIT;
}

ShellCommandResult LaunchUI(BaseShellState &base_state, const vector<string> &args) {
	auto &state = static_cast<ShellState &>(base_state);
	// run the UI command
	auto rc = state.RunInitialCommand((char *)state.ui_command.c_str(), true);
	if (rc != 0) {
		ShellState::Exit(rc);
		return ShellCommandResult::EXIT;
	}
	return ShellCommandResult::SUCCESS;
}

ShellCommandResult SetNewlineSeparator(BaseShellState &base_state, const vector<string> &args) {
	auto &state = static_cast<ShellState &>(base_state);
	// run the UI command
	state.rowSeparator = args[1];
	return ShellCommandResult::SUCCESS;
}

ShellCommandResult SetStorageVersion(BaseShellState &base_state, const vector<string> &args) {
	auto &state = static_cast<ShellState &>(base_state);
	auto &storage_version = args[1];
	try {
		state.config.options.serialization_compatibility =
		    duckdb::SerializationCompatibility::FromString(storage_version);
	} catch (std::exception &ex) {
		duckdb::ErrorData error(ex);
		state.PrintF(PrintOutput::STDERR, "%s: Error: unknown argument (%s) for '-storage-version': %s\n",
		             state.program_name, storage_version.c_str(), error.Message().c_str());
		return ShellCommandResult::EXIT;
	}
	return ShellCommandResult::SUCCESS;
}

ShellCommandResult ProcessFile(BaseShellState &base_state, const vector<string> &args) {
	auto &state = static_cast<ShellState &>(base_state);
	state.readStdin = false;
	auto &file = args[1];
	if (!state.ProcessFile(file)) {
		ShellState::Exit(1);
		return ShellCommandResult::EXIT;
	}
	return ShellCommandResult::SUCCESS;
}

ShellCommandResult SetInitFile(BaseShellState &base_state, const vector<string> &args) {
	auto &state = static_cast<ShellState &>(base_state);
	state.initFile = args[1];
	return ShellCommandResult::SUCCESS;
}

ShellCommandResult SkipInit(BaseShellState &base_state, const vector<string> &args) {
	auto &state = static_cast<ShellState &>(base_state);
	state.run_init = false;
	return ShellCommandResult::SUCCESS;
}

template <bool EXIT>
ShellCommandResult RunCommand(BaseShellState &base_state, const vector<string> &args) {
	auto &state = static_cast<ShellState &>(base_state);
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
		return ShellCommandResult::EXIT;
	}
	return ShellCommandResult::SUCCESS;
}

ShellCommandResult FormatStdin(BaseShellState &base_state, const vector<string> &args) {
	auto &state = static_cast<ShellState &>(base_state);
	state.readStdin = false;

	if (duckdb::Terminal::IsAtty()) {
		state.PrintF(PrintOutput::STDERR,
		             "%s: Error: -format requires SQL input on stdin (e.g. echo 'SELECT 1' | duckdb -format)\n",
		             state.program_name);
		return ShellCommandResult::FAIL;
	}

	// Read all of stdin into a string.
	string sql = state.ReadFileContents(stdin);

	auto result = state.FormatSQL(sql);
	if (result != ShellCommandResult::SUCCESS) {
		return result;
	}

	// Write formatted SQL to stdout, with syntax highlighting if stdout is a terminal.
	state.HighlightSQL(sql);
	state.Print(PrintOutput::STDOUT, sql);
	return ShellCommandResult::SUCCESS;
}

ShellCommandResult FormatFile(BaseShellState &base_state, const vector<string> &args) {
	auto &state = static_cast<ShellState &>(base_state);
	state.readStdin = false;
	const string &filename = args[1];

	string sql = state.ReadFileContents(filename);

	auto result = state.FormatSQL(sql);
	if (result != ShellCommandResult::SUCCESS) {
		return result;
	}
	state.HighlightSQL(sql);
	state.Print(PrintOutput::STDOUT, sql);
	return ShellCommandResult::SUCCESS;
}

static const CommandLineOption command_line_options[] = {
    {"ascii", 0, "", nullptr, ToggleASCIIMode, "set output mode to 'ascii'"},
    {"bail", 0, "", nullptr, EnableBail, "stop after hitting an error"},
    {"batch", 0, "", EnableBatch, EnableBatch, "force batch I/O'"},
    {"box", 0, "", nullptr, ToggleOutputMode<RenderMode::BOX>, "set output mode to 'box'"},
    {"column", 0, "", nullptr, ToggleOutputMode<RenderMode::COLUMN>, "set output mode to 'column'"},
    {"cmd", 1, "COMMAND", nullptr, RunCommand<false>, "run \"COMMAND\" before reading stdin"},
    {"csv", 0, "", nullptr, ToggleCSVMode, "set output mode to 'csv'"},
    {"c", 1, "COMMAND", EnableBatch, RunCommand<true>, "run \"COMMAND\" and exit"},
    {"echo", 0, "", nullptr, EnableEcho, "print commands before execution"},
    {"f", 1, "FILENAME", EnableBatch, ProcessFile, "read/process named file and exit"},
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
    {"version", 0, "", nullptr, ShowVersionAndExit, "show DuckDB version"},
    {nullptr, 0, nullptr, nullptr, nullptr, nullptr}};

optional_idx FindOption(const char *name) {
	for (idx_t c = 0; command_line_options[c].option; c++) {
		auto &option = command_line_options[c];
		if (!StringUtil::Equals(name, option.option)) {
			// not this one
			continue;
		}
		// found it!
		return c;
	}
	return optional_idx();
}

optional_ptr<const CommandLineOption> ShellState::FindCommandLineOption(const string &option, string &error_msg) const {
	auto c = FindOption(option.c_str());
	if (!c.IsValid()) {
		// we haven't found it yet - try substituting all underscores with dashes
		// this is legacy behavior - we allow e.g. "-storage_version" to be used instead of "-storage-version"
		auto option_name = StringUtil::Replace(option, "_", "-");
		c = FindOption(option_name.c_str());
	}
	if (!c.IsValid()) {
		// not found
		error_msg = StringUtil::Format("Unknown Option Error: Unrecognized option '-%s'\n", option);
		vector<string> option_names;
		for (idx_t c = 0; command_line_options[c].option; c++) {
			auto &option = command_line_options[c];
			option_names.push_back(string("-") + option.option);
		}
		auto candidates_msg = StringUtil::CandidatesErrorMessage(option_names, "-" + option, "Did you mean");
		error_msg += candidates_msg + "\n";
		error_msg += StringUtil::Format("Run '%s -help' for a list of options.\n", program_name);
		return nullptr;
	}
	return command_line_options[c.GetIndex()];
}

struct PrintOptionInfo {
	string command_name;
	string arguments;
	string description;
};

void ShellState::PrintUsage() {
	ShellHighlight highlighter(*this);
	highlighter.PrintText("Usage: ", PrintOutput::STDOUT, PrintColor::STANDARD, PrintIntensity::BOLD);
	highlighter.PrintText(program_name, PrintOutput::STDOUT, HighlightElementType::KEYWORD);
	highlighter.PrintText(" [OPTIONS] FILENAME [SQL]\n\n", PrintOutput::STDOUT, HighlightElementType::STRING_CONSTANT);
	highlighter.PrintText("FILENAME", PrintOutput::STDOUT, PrintColor::STANDARD, PrintIntensity::BOLD);
	PrintF(" is the name of a DuckDB database. A new database is created\n"
	       "if the file does not previously exist.\n\n");
	highlighter.PrintText("OPTIONS:\n", PrintOutput::STDOUT, PrintColor::STANDARD, PrintIntensity::BOLD);
	constexpr idx_t INITIAL_SPACING = 2;
	constexpr idx_t MIN_SPACING = 4;
	vector<PrintOptionInfo> print_options;
	for (idx_t c = 0; command_line_options[c].option; c++) {
		auto &option = command_line_options[c];
		PrintOptionInfo print_option;
		print_option.command_name = string(INITIAL_SPACING, ' ') + "-" + option.option;
		print_option.arguments = option.arguments;
		print_option.description = option.description;
		print_options.push_back(std::move(print_option));
	}
	idx_t max_lhs_length = 0;
	for (auto &option : print_options) {
		auto lhs_length = option.command_name.size() + option.arguments.size();
		if (!option.arguments.empty()) {
			lhs_length++;
		}
		if (lhs_length > max_lhs_length) {
			max_lhs_length = lhs_length;
		}
	}
	// print the options
	for (auto &option : print_options) {
		auto lhs_length = option.command_name.size() + option.arguments.size();
		if (!option.arguments.empty()) {
			lhs_length++;
		}
		idx_t padding = max_lhs_length - lhs_length + MIN_SPACING;
		string spaces(padding, ' ');
		highlighter.PrintText(option.command_name, PrintOutput::STDOUT, HighlightElementType::KEYWORD);
		if (!option.arguments.empty()) {
			highlighter.PrintText(" " + option.arguments, PrintOutput::STDOUT, HighlightElementType::STRING_CONSTANT);
		}
		PrintF("%s%s\n", spaces.c_str(), option.description.c_str());
	}
	ShellState::Exit(0);
}

} // namespace duckdb_shell
