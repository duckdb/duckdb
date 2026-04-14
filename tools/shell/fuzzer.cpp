#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#if (!defined(_WIN32) && !defined(WIN32)) || defined(__MINGW32__)
#include <unistd.h>
#endif

#include "shell_fuzzer.hpp"
#include "shell_state.hpp"

namespace duckdb_shell {

#ifdef DUCKDB_FUZZER
static bool FuzzerUnsafeModeEnabled() {
	auto value = getenv("DUCKDB_FUZZER_UNSAFE");
	if (!value) {
		return false;
	}
	return value[0] != '\0' && strcmp(value, "0") != 0;
}

static string BuildFuzzScript(const uint8_t *data, size_t size) {
	constexpr size_t MAX_SCRIPT_SIZE = 1 << 20;
	if (size > MAX_SCRIPT_SIZE) {
		size = MAX_SCRIPT_SIZE;
	}

	string script;
	script.reserve(size + 1);
	for (size_t i = 0; i < size; i++) {
		auto c = static_cast<char>(data[i]);
		if (c == '\0') {
			c = '\n';
		}
		script.push_back(c);
	}
	if (script.empty() || script.back() != '\n') {
		script.push_back('\n');
	}
	return script;
}

static void RunFuzzIteration(const uint8_t *data, size_t size, bool safe_mode) {
	auto &shell_state = ShellState::GetReference();
	if (shell_state) {
		delete shell_state;
		shell_state = nullptr;
	}

	auto &state = ShellState::Get();
	state.out = tmpfile();
	if (!state.out) {
		state.out = stdout;
	}
	state.stdin_is_interactive = false;
	state.stdout_is_console = false;
	state.stderr_is_console = false;
	state.readStdin = true;
	state.run_init = false;
	state.safe_mode = safe_mode;
	state.zDbFilename = ":memory:";
	state.Initialize();
	state.OpenDB(ShellOpenFlags::KEEP_ALIVE_ON_FAILURE);
	state.ExecuteQuery("PRAGMA disable_progress_bar");

	auto script = BuildFuzzScript(data, size);
	auto input = tmpfile();
	if (input) {
		fwrite(script.data(), 1, script.size(), input);
		rewind(input);
		state.in = input;
		(void)state.ProcessInput(InputMode::STANDARD);
		fclose(input);
	}

	state.SetTableName(0);
	state.Destroy();
	state.ResetOutput();
	state.doXdgOpen = 0;
	state.ClearTempFile();

	if (shell_state) {
		delete shell_state;
		shell_state = nullptr;
	}
}

int RunFuzzer() {
	__AFL_FUZZ_INIT();
#ifdef __AFL_HAVE_MANUAL_CONTROL
	__AFL_INIT();
#endif

	auto safe_mode = !FuzzerUnsafeModeEnabled();
	auto *buf = __AFL_FUZZ_TESTCASE_BUF;
	while (__AFL_LOOP(1000)) {
		auto size = static_cast<size_t>(__AFL_FUZZ_TESTCASE_LEN);
		if (!buf || size == 0) {
			continue;
		}
		RunFuzzIteration(buf, size, safe_mode);
	}
	return 0;
}
#endif

} // namespace duckdb_shell
