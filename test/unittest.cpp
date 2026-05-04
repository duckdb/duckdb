#define CATCH_CONFIG_RUNNER
#include "catch.hpp"
#include <stdlib.h>

#include "duckdb/common/file_system.hpp"
#include "duckdb/common/string_util.hpp"
#include "sqlite/sqllogic_test_logger.hpp"
#include "test_helpers.hpp"
#include "test_config.hpp"

#include <algorithm>
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <deque>
#include <set>
#include <string>
#include <vector>

#ifndef _WIN32
#include <errno.h>
#include <fcntl.h>
#include <poll.h>
#include <signal.h>
#include <spawn.h>
#include <sys/wait.h>
#include <unistd.h>
extern char **environ;
#endif

using namespace duckdb;

// =============================================================================
// Per-test-file parallelism for unittest.
//
// When the user passes --jobs N (or --shard-index/--shard-count), this binary
// runs as a coordinator: it enumerates the tests that match the user's filter,
// then spawns up to N child processes via posix_spawn, each running exactly one
// .test file (= one Catch2 TEST_CASE) in its own --test-temp-dir. Children's
// output is captured per-pipe and printed atomically as each child exits.
//
// When --jobs is 1 (default), behavior is identical to the prior single-process
// implementation.
//
// POSIX-only for now: on Windows --jobs / --shard-* fall back to serial mode
// with a warning. A CreateProcess + WaitForMultipleObjects port is straight-
// forward future work but not in this PR.
//
// Known limitations (worth a follow-up issue, not in this PR):
//   * Reporter aggregation: with -r junit/xml, each child writes its own
//     output file and they overwrite each other. A coordinator-level
//     reporter that gives each child a unique --out and merges results
//     would close this gap.
//   * Per-child startup amortisation: every child re-loads DuckDB. A
//     worker-pool variant where each child runs a queue of tests would
//     win significantly for short tests.
// =============================================================================

namespace {

struct ParallelOptions {
	int jobs = 1;
	int shard_index = 0;
	int shard_count = 1;
	bool sharding_requested = false;
	// Coordinator-side abort threshold. 0 = never abort the coordinator.
	// Mirrors Catch's `-a` / `-x N`; we still forward the flag to children so
	// each child also honors it within its single-test run.
	int abort_after = 0;
};

// Try to consume a "--flag value" or "--flag=value" pair. On match, advances i
// past the consumed tokens and returns true; the caller must `continue`.
bool TryLongValue(const std::string &arg, const char *flag, std::string &out, idx_t argc, char **argv, idx_t &i) {
	const std::string lf = flag;
	if (arg == lf) {
		if (i + 1 >= argc) {
			fprintf(stderr, "Missing value for %s\n", flag);
			std::exit(1);
		}
		out = std::string(argv[++i]);
		return true;
	}
	const std::string prefix = lf + "=";
	if (arg.size() > prefix.size() && arg.compare(0, prefix.size(), prefix) == 0) {
		out = arg.substr(prefix.size());
		return true;
	}
	return false;
}

bool ParseInt(const std::string &val, int &out) {
	try {
		size_t consumed = 0;
		int n = std::stoi(val, &consumed);
		if (consumed != val.size()) {
			return false;
		}
		out = n;
		return true;
	} catch (...) {
		return false;
	}
}

// Catch2 long-form flags that consume a following value. We use this allowlist
// only when forwarding the parent's argv to a child (see BuildChildArgv): a
// value-flag's next argv slot is its value and must come along; bool flags
// stand alone. Unknown flags are forwarded as bool flags — users with rare
// value-flags should use the --flag=value form, which works regardless.
const std::set<std::string> &CatchValueFlags() {
	static const std::set<std::string> s = {
	    "--reporter",
	    "-r",
	    "--out",
	    "-o",
	    "--rng-seed",
	    "--section",
	    "-c",
	    "--durations",
	    "-d",
	    "--abortx",
	    "-x",
	    "--order",
	    "--use-colour",
	    "--colour-mode",
	    "--config",
	    "--wait-for-keypress",
	    "--name",
	    "-n",
	    "--min-duration",
	    "-D",
	    "--input-file",
	    "-f",
	    "--start-offset",
	    "--end-offset",
	    "--start-offset-percentage",
	    "--end-offset-percentage",
	    "--verbosity",
	    "-v",
	    "--warn",
	    "-w",
	};
	return s;
}

const std::set<std::string> &WrapperValueFlags() {
	static const std::set<std::string> s = {
	    "--require",
	    "--test-dir",
	};
	return s;
}

bool IsValueFlag(const std::string &arg) {
	return CatchValueFlags().count(arg) > 0 || WrapperValueFlags().count(arg) > 0;
}

bool IsParallelFlag(const std::string &arg) {
	if (arg == "--jobs" || arg == "-j" || arg == "--shard-index" || arg == "--shard-count" ||
	    arg == "--test-temp-dir") {
		return true;
	}
	auto starts = [&](const char *p) {
		const size_t n = strlen(p);
		return arg.size() >= n && arg.compare(0, n, p) == 0;
	};
	return starts("--jobs=") || starts("--shard-index=") || starts("--shard-count=") || starts("--test-temp-dir=");
}

// Build child argv: copy the parent's argv but
//   * drop parallel-mode and --test-temp-dir flags (with their values),
//   * drop positional test specs (the user's filter is already resolved by
//     the coordinator; the child gets exactly one assigned test),
//   * add --test-temp-dir <child_temp_dir> and one positional for the test.
// The test name is wrapped in Catch2's quoted-name syntax for exact matching.
std::vector<std::string> BuildChildArgv(int argc, char **argv, const std::string &child_temp_dir,
                                        const std::string &test_name) {
	std::vector<std::string> result;
	result.emplace_back(argv[0]);
	for (int i = 1; i < argc; i++) {
		std::string a = argv[i];
		if (IsParallelFlag(a)) {
			if (a == "--jobs" || a == "-j" || a == "--shard-index" || a == "--shard-count" || a == "--test-temp-dir") {
				if (i + 1 < argc) {
					i++;
				}
			}
			continue;
		}
		if (!a.empty() && a[0] == '-') {
			result.push_back(a);
			if (IsValueFlag(a) && i + 1 < argc) {
				result.emplace_back(argv[++i]);
			}
			continue;
		}
		// positional → drop (was a Catch test-spec filter from the user)
	}
	result.emplace_back("--test-temp-dir");
	result.push_back(child_temp_dir);
	result.push_back("\"" + test_name + "\"");
	return result;
}

#ifndef _WIN32

struct ChildProc {
	pid_t pid = -1;
	int fd = -1;
	std::string test_name;
	std::string output;
	std::chrono::steady_clock::time_point start;
};

int SpawnChild(const std::vector<std::string> &cargv, ChildProc &c) {
	int pipefd[2];
	if (pipe(pipefd) != 0) {
		perror("pipe");
		return -1;
	}

	posix_spawn_file_actions_t actions;
	if (posix_spawn_file_actions_init(&actions) != 0) {
		close(pipefd[0]);
		close(pipefd[1]);
		return -1;
	}
	posix_spawn_file_actions_addclose(&actions, pipefd[0]);
	posix_spawn_file_actions_adddup2(&actions, pipefd[1], STDOUT_FILENO);
	posix_spawn_file_actions_adddup2(&actions, pipefd[1], STDERR_FILENO);
	posix_spawn_file_actions_addclose(&actions, pipefd[1]);

	std::vector<char *> ptrs;
	ptrs.reserve(cargv.size() + 1);
	for (auto &s : cargv) {
		// posix_spawn requires non-const argv; the child does not modify these.
		ptrs.push_back(const_cast<char *>(s.c_str()));
	}
	ptrs.push_back(nullptr);

	pid_t pid = -1;
	int rc = posix_spawn(&pid, ptrs[0], &actions, nullptr, ptrs.data(), environ);
	posix_spawn_file_actions_destroy(&actions);
	close(pipefd[1]);
	if (rc != 0) {
		errno = rc;
		perror("posix_spawn");
		close(pipefd[0]);
		return -1;
	}

	int flags = fcntl(pipefd[0], F_GETFL, 0);
	if (flags >= 0) {
		fcntl(pipefd[0], F_SETFL, flags | O_NONBLOCK);
	}

	c.pid = pid;
	c.fd = pipefd[0];
	c.start = std::chrono::steady_clock::now();
	return 0;
}

void DrainFd(ChildProc &c) {
	char buf[4096];
	while (true) {
		ssize_t n = read(c.fd, buf, sizeof(buf));
		if (n > 0) {
			c.output.append(buf, static_cast<size_t>(n));
			continue;
		}
		// EOF or EAGAIN — caller decides what to do next.
		return;
	}
}

// Signal handler state. `volatile sig_atomic_t` is the only safe shared type
// from a handler back to main(); the handler is fully async-signal-safe.
volatile sig_atomic_t g_sigint = 0;
void OnSigInt(int) {
	g_sigint = 1;
}

int RunCoordinator(int argc, char **argv, const ParallelOptions &opts, const std::vector<std::string> &test_names,
                   const std::string &base_temp_dir) {
	struct sigaction sa;
	sa.sa_handler = OnSigInt;
	sigemptyset(&sa.sa_mask);
	sa.sa_flags = 0;
	sigaction(SIGINT, &sa, nullptr);
	sigaction(SIGTERM, &sa, nullptr);

	const int total = static_cast<int>(test_names.size());
	if (total == 0) {
		fprintf(stderr, "No tests matched.\n");
		return 0;
	}
	const int jobs = std::max(1, std::min(opts.jobs, total));
	fprintf(stderr, "Running %d test%s with %d parallel worker%s\n", total, total == 1 ? "" : "s", jobs,
	        jobs == 1 ? "" : "s");

	std::deque<size_t> pending;
	for (size_t i = 0; i < test_names.size(); i++) {
		pending.push_back(i);
	}

	std::vector<ChildProc> active;
	int completed = 0;
	int failed = 0;
	bool aborted = false;
	std::string aggregate_failures;

	auto trigger_abort = [&]() {
		if (aborted) {
			return;
		}
		aborted = true;
		pending.clear();
		fprintf(stderr, "Aborting after %d failure%s; terminating %zu in-flight test%s\n", failed,
		        failed == 1 ? "" : "s", active.size(), active.size() == 1 ? "" : "s");
		for (auto &c : active) {
			if (c.pid > 0) {
				kill(c.pid, SIGTERM);
			}
		}
	};

	auto launch_one = [&]() {
		while (!pending.empty() && static_cast<int>(active.size()) < jobs && !g_sigint && !aborted) {
			const size_t idx = pending.front();
			pending.pop_front();
			ChildProc c;
			c.test_name = test_names[idx];
			std::string child_dir = base_temp_dir + "/c" + std::to_string(idx);
			auto cargv = BuildChildArgv(argc, argv, child_dir, c.test_name);
			if (SpawnChild(cargv, c) != 0) {
				fprintf(stderr, "Failed to spawn child for %s\n", c.test_name.c_str());
				completed++;
				failed++;
				if (opts.abort_after > 0 && failed >= opts.abort_after) {
					trigger_abort();
				}
				continue;
			}
			active.push_back(std::move(c));
		}
	};

	launch_one();

	while (!active.empty()) {
		std::vector<struct pollfd> pfds;
		pfds.reserve(active.size());
		for (auto &c : active) {
			struct pollfd p;
			p.fd = c.fd;
			p.events = POLLIN;
			p.revents = 0;
			pfds.push_back(p);
		}
		int rc = poll(pfds.data(), pfds.size(), 1000);
		if (g_sigint) {
			fprintf(stderr, "\nInterrupted — terminating %zu active children\n", active.size());
			for (auto &c : active) {
				if (c.pid > 0) {
					kill(c.pid, SIGTERM);
				}
			}
			for (auto &c : active) {
				int st;
				if (c.pid > 0) {
					waitpid(c.pid, &st, 0);
				}
				if (c.fd >= 0) {
					close(c.fd);
				}
			}
			return 130;
		}
		if (rc < 0) {
			if (errno == EINTR) {
				continue;
			}
			perror("poll");
			break;
		}

		for (size_t i = 0; i < active.size();) {
			auto &c = active[i];
			if (pfds[i].revents & (POLLIN | POLLHUP | POLLERR)) {
				DrainFd(c);
			}
			int status = 0;
			pid_t r = waitpid(c.pid, &status, WNOHANG);
			if (r == c.pid) {
				DrainFd(c);
				close(c.fd);
				const bool ok = WIFEXITED(status) && WEXITSTATUS(status) == 0;
				completed++;
				if (!ok) {
					failed++;
				}
				const int pct = total > 0 ? static_cast<int>((completed * 100.0) / total) : 0;
				// Match Catch2's renderTestProgress prefix exactly (catch.hpp:13449).
				printf("[%d/%d] (%d%%): %s\n", completed, total, pct, c.test_name.c_str());
				fflush(stdout);
				if (!ok) {
					fprintf(stderr, "----- FAIL: %s -----\n%s\n----- end %s -----\n", c.test_name.c_str(),
					        c.output.c_str(), c.test_name.c_str());
					aggregate_failures += "[FAIL] " + c.test_name + "\n";
					if (opts.abort_after > 0 && failed >= opts.abort_after) {
						trigger_abort();
					}
				}
				active.erase(active.begin() + static_cast<std::ptrdiff_t>(i));
				launch_one();
				continue;
			}
			i++;
		}
	}

	fprintf(stderr, "\n====================================================\n");
	if (aborted) {
		fprintf(stderr, "ABORTED after %d failure%s (ran %d/%d tests)\n", failed, failed == 1 ? "" : "s", completed,
		        total);
	} else {
		fprintf(stderr, "Results: %d/%d passed, %d failed\n", total - failed, total, failed);
	}
	if (!aggregate_failures.empty()) {
		fprintf(stderr, "\nFailed tests:\n%s", aggregate_failures.c_str());
	}
	return failed == 0 ? 0 : 1;
}

#endif // !_WIN32

// Register --jobs / --shard-* / --test-temp-dir / --test-dir / --require with
// Catch's Clara parser so they appear in `unittest -h`. We don't actually
// consume their values via Clara (the parent argv loop has already stripped
// them); registering is purely for help output and discoverability.
void RegisterExtraHelpOptions(Catch::Session &session) {
	using namespace Catch::clara;
	auto cli =
	    session.cli() | Opt([](int) {}, "n")["-j"]["--jobs"]("number of parallel test workers (per .test file)") |
	    Opt([](int) {}, "n")["--shard-index"]("0-based shard index (use with --shard-count)") |
	    Opt([](int) {}, "n")["--shard-count"]("total number of shards") |
	    Opt([](std::string const &) {}, "dir")["--test-temp-dir"]("directory used as base for per-test working dirs") |
	    Opt([](std::string const &) {}, "dir")["--test-dir"]("root test directory") |
	    Opt([](std::string const &) {}, "name")["--require"]("required loaded extension");
	session.cli(cli);
}

} // anonymous namespace

int main(int argc_in, char *argv[]) {
	duckdb::unique_ptr<FileSystem> fs = FileSystem::CreateLocal();
	string test_directory = DUCKDB_ROOT_DIRECTORY;

	auto &test_config = TestConfiguration::Get();
	test_config.Initialize();

	ParallelOptions popts;
	std::string user_test_temp_dir;

	idx_t argc = NumericCast<idx_t>(argc_in);
	int new_argc = 0;
	auto new_argv = duckdb::unique_ptr<char *[]>(new char *[argc]);
	for (idx_t i = 0; i < argc; i++) {
		string argument(argv[i]);
		std::string val;

		// Wrapper-consumed flags (NOT forwarded to Catch).
		if (argument == "--test-dir") {
			if (i + 1 >= argc) {
				fprintf(stderr, "Missing value for --test-dir\n");
				return 1;
			}
			test_directory = string(argv[++i]);
			continue;
		}
		if (TryLongValue(argument, "--test-temp-dir", val, argc, argv, i)) {
			SetDeleteTestPath(false);
			user_test_temp_dir = val;
			SetTestDirectory(val);
			continue;
		}
		if (argument == "--require") {
			if (i + 1 >= argc) {
				fprintf(stderr, "Missing value for --require\n");
				return 1;
			}
			AddRequire(string(argv[++i]));
			continue;
		}

		// Parallel-mode flags (NOT forwarded).
		if (TryLongValue(argument, "--jobs", val, argc, argv, i)) {
			if (!ParseInt(val, popts.jobs) || popts.jobs < 1) {
				fprintf(stderr, "Invalid value for --jobs: %s\n", val.c_str());
				return 1;
			}
			continue;
		}
		if (argument == "-j") {
			if (i + 1 >= argc) {
				fprintf(stderr, "Missing value for -j\n");
				return 1;
			}
			val = std::string(argv[++i]);
			if (!ParseInt(val, popts.jobs) || popts.jobs < 1) {
				fprintf(stderr, "Invalid value for -j: %s\n", val.c_str());
				return 1;
			}
			continue;
		}
		if (TryLongValue(argument, "--shard-index", val, argc, argv, i)) {
			if (!ParseInt(val, popts.shard_index)) {
				fprintf(stderr, "Invalid value for --shard-index: %s\n", val.c_str());
				return 1;
			}
			popts.sharding_requested = true;
			continue;
		}
		if (TryLongValue(argument, "--shard-count", val, argc, argv, i)) {
			if (!ParseInt(val, popts.shard_count)) {
				fprintf(stderr, "Invalid value for --shard-count: %s\n", val.c_str());
				return 1;
			}
			popts.sharding_requested = true;
			continue;
		}

		// Observed-and-forwarded flags. We record the threshold for the
		// coordinator's own abort logic and let Catch see them too so each
		// child also honors them.
		if (argument == "-a" || argument == "--abort") {
			popts.abort_after = std::max(popts.abort_after, 1);
			// fall through
		} else if ((argument == "-x" || argument == "--abortx") && i + 1 < argc) {
			int n;
			if (ParseInt(argv[i + 1], n) && n > 0) {
				popts.abort_after = n;
			}
			// fall through (do NOT consume; Catch will parse it from new_argv)
		}

		if (test_config.ParseArgument(argument, argc, argv, i)) {
			continue;
		}

		new_argv[new_argc] = argv[i];
		new_argc++;
	}
	test_config.ChangeWorkingDirectory(test_directory);

	const bool parallel_requested = popts.jobs > 1 || popts.sharding_requested;
#ifdef _WIN32
	if (parallel_requested) {
		fprintf(stderr, "Note: --jobs / --shard-* are not yet supported on Windows; falling back to serial mode.\n");
	}
	const bool parallel_mode = false;
#else
	const bool parallel_mode = parallel_requested;
#endif

	if (!parallel_mode) {
		// Serial path — original upstream behavior.
		if (!user_test_temp_dir.empty() && fs->DirectoryExists(user_test_temp_dir)) {
			fprintf(stderr, "--test-temp-dir cannot point to a directory that already exists (%s)\n",
			        user_test_temp_dir.c_str());
			return 1;
		}
		auto dir = TestCreatePath("");
		try {
			TestDeleteDirectory(dir);
			TestCreateDirectory(dir);
		} catch (std::exception &ex) {
			fprintf(stderr, "Failed to create testing directory \"%s\": %s\n", dir.c_str(), ex.what());
			return 1;
		}

		// Override the home dir so the .duckdb dir is isolated per test process.
#ifdef DUCKDB_WINDOWS
		if (_putenv_s("USERPROFILE", dir.c_str()) != 0) {
			fprintf(stderr, "Failed to set USERPROFILE environment variable\n");
			return 1;
		}
#else
		if (setenv("HOME", dir.c_str(), 1) != 0) {
			fprintf(stderr, "Failed to set HOME environment variable\n");
			return 1;
		}
#endif

		if (test_config.GetSkipCompiledTests()) {
			Catch::getMutableRegistryHub().clearTests();
		}
		RegisterSqllogictests();

		Catch::Session session;
		RegisterExtraHelpOptions(session);
		int result = session.run(new_argc, new_argv.get());

		std::string failures_summary = FailureSummary::GetFailureSummary();
		if (!failures_summary.empty()) {
			auto description = test_config.GetDescription();
			if (!description.empty()) {
				std::cerr << "\n====================================================" << std::endl;
				std::cerr << "====================  TEST INFO  ===================" << std::endl;
				std::cerr << "====================================================\n" << std::endl;
				std::cerr << description << std::endl;
			}
			std::cerr << "\n====================================================" << std::endl;
			std::cerr << "================  FAILURES SUMMARY  ================" << std::endl;
			std::cerr << "====================================================\n" << std::endl;
			std::cerr << failures_summary;
		}

		if (DeleteTestPath()) {
			TestDeleteDirectory(dir);
		}
		return result;
	}

#ifndef _WIN32
	// Coordinator path (POSIX only — Windows requested falls back above).
	// Tests run in children; the coordinator does not run any tests itself,
	// so it does NOT set HOME (each child handles its own HOME isolation in
	// its serial path).
	if (test_config.GetSkipCompiledTests()) {
		Catch::getMutableRegistryHub().clearTests();
	}
	RegisterSqllogictests();

	Catch::Session session;
	RegisterExtraHelpOptions(session);
	int code = session.applyCommandLine(new_argc, new_argv.get());
	if (code != 0) {
		return code;
	}

	// Defer to session.run() for metadata-only modes (-h, -l, -t, etc.). These
	// do not actually run tests, so spawning children would be wrong.
	const auto &cfg = session.config();
	if (cfg.showHelp() || cfg.listTests() || cfg.listTags() || cfg.listTestNamesOnly() || cfg.listReporters()) {
		return session.run();
	}

	auto matched = Catch::filterTests(Catch::getAllTestCasesSorted(cfg), cfg.testSpec(), cfg);
	std::vector<std::string> names;
	names.reserve(matched.size());
	for (auto const &tc : matched) {
		names.push_back(tc.getTestCaseInfo().name);
	}

	if (popts.sharding_requested) {
		if (popts.shard_count <= 0 || popts.shard_index < 0 || popts.shard_index >= popts.shard_count) {
			fprintf(stderr, "Invalid shard configuration: index=%d count=%d\n", popts.shard_index, popts.shard_count);
			return 1;
		}
		std::vector<std::string> shard;
		for (size_t i = 0; i < names.size(); i++) {
			if (static_cast<int>(i % static_cast<size_t>(popts.shard_count)) == popts.shard_index) {
				shard.push_back(names[i]);
			}
		}
		names = std::move(shard);
		fprintf(stderr, "Shard %d/%d: %zu tests\n", popts.shard_index, popts.shard_count, names.size());
	}

	// Choose a base directory for child --test-temp-dir args. If the user
	// supplied one, honor the same "must not pre-exist" contract the serial
	// path enforces. Otherwise create a uniquely-named directory via mkdtemp
	// so we never blindly clobber an unrelated path.
	std::string base_temp_dir;
	if (!user_test_temp_dir.empty()) {
		if (fs->DirectoryExists(user_test_temp_dir)) {
			fprintf(stderr, "--test-temp-dir cannot point to a directory that already exists (%s)\n",
			        user_test_temp_dir.c_str());
			return 1;
		}
		base_temp_dir = user_test_temp_dir;
		try {
			TestCreateDirectory(base_temp_dir);
		} catch (std::exception &ex) {
			fprintf(stderr, "Failed to create --test-temp-dir \"%s\": %s\n", base_temp_dir.c_str(), ex.what());
			return 1;
		}
	} else {
		// Honor TMPDIR (macOS in particular puts user temp dirs under /var/folders/...).
		const char *tmp_root = getenv("TMPDIR");
		if (tmp_root == nullptr || *tmp_root == '\0') {
			tmp_root = "/tmp";
		}
		std::string tmpl_str = tmp_root;
		if (!tmpl_str.empty() && tmpl_str.back() == '/') {
			tmpl_str.pop_back();
		}
		tmpl_str += "/duckdb-unittest-XXXXXX";
		std::vector<char> tmpl(tmpl_str.begin(), tmpl_str.end());
		tmpl.push_back('\0');
		if (mkdtemp(tmpl.data()) == nullptr) {
			fprintf(stderr, "Failed to create coordinator temp dir under %s: %s\n", tmp_root, strerror(errno));
			return 1;
		}
		base_temp_dir = tmpl.data();
	}

	int result = RunCoordinator(argc_in, argv, popts, names, base_temp_dir);

	try {
		TestDeleteDirectory(base_temp_dir);
	} catch (...) {
		// Leak the directory rather than mask a real error. A child crashing
		// mid-test can leave files behind; surfacing the stale dir on `ls`
		// is more useful than a silent rm-rf that hides whatever went wrong.
	}
	return result;
#else
	// Unreachable: parallel_mode is forced to false on Windows above. Keep an
	// explicit return so the compiler is happy without falling off the end.
	return 0;
#endif
}
