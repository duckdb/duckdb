#include "shell_state.hpp"

#include <cstdio>
#include <cstdlib>
#include <exception>

using namespace duckdb_shell;

int RunShell(int argc, const char **argv);

#ifdef DUCKDB_SHELL_X86_64_V3
static bool SupportsX86_64V3() {
	__builtin_cpu_init();
	return __builtin_cpu_supports("cx16") && __builtin_cpu_supports("sahf") && __builtin_cpu_supports("sse3") &&
	       __builtin_cpu_supports("ssse3") && __builtin_cpu_supports("sse4.1") && __builtin_cpu_supports("sse4.2") &&
	       __builtin_cpu_supports("popcnt") && __builtin_cpu_supports("avx") && __builtin_cpu_supports("avx2") &&
	       __builtin_cpu_supports("bmi") && __builtin_cpu_supports("bmi2") && __builtin_cpu_supports("f16c") &&
	       __builtin_cpu_supports("fma") && __builtin_cpu_supports("lzcnt") && __builtin_cpu_supports("movbe");
}

// Run before static initializers that may contain x86-64-v3 instructions.
__attribute__((constructor(101))) static void CheckCPUCompatibility() {
	if (SupportsX86_64V3()) {
		return;
	}
	fputs("This DuckDB CLI requires a CPU that supports the x86-64-v3 instruction set.\n"
	      "Install a compatible DuckDB CLI using https://install.duckdb.org\n",
	      stderr);
	fflush(stderr);
	std::_Exit(1);
}
#endif

#if !((defined(_WIN32) || defined(WIN32)) && defined(_MSC_VER))
int main(int argc, const char **argv) {
#else
int wmain(int argc, wchar_t **wargv) {
	vector<string> utf8_args;
	utf8_args.resize(argc);
	vector<const char *> utf8_args_ptrs;
	utf8_args_ptrs.resize(argc);
	const char **argv = utf8_args_ptrs.data();
	for (int i = 0; i < argc; i++) {
		utf8_args[i] = ShellState::Win32UnicodeToUtf8(wargv[i]);
		utf8_args_ptrs[i] = utf8_args[i].c_str();
	}
#endif

	auto &shell_state = ShellState::GetReference();
	int rc = 0;
	try {
		rc = RunShell(argc, argv);
	} catch (const ShellExitException &ex) {
		rc = ex.exit_code;
	} catch (std::exception &ex) {
		rc = 1;
		ErrorData error(ex);
		fprintf(stderr, "Exited due to error: %s", error.Message().c_str());
	}
	try {
		// Stop new interrupt callbacks before destroying any shell owners.
		ShellState::IgnoreInterrupts();
		// Print before output cleanup forgets whether stdout was a console.
		if (shell_state) {
			shell_state->PrintExitHint(rc);
		}
		// destroy shell state prior to program clean-up
		if (shell_state) {
			delete shell_state;
		}
		shell_state = nullptr;
	} catch (std::exception &ex) {
		rc = 1;
		ErrorData error(ex);
		fprintf(stderr, "Error during clean-up due to error: %s", error.Message().c_str());
	}
	return rc;
}
