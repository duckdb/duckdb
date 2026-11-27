#pragma once

#include "duckdb/common/file_system.hpp"
#include "test_config.hpp"
#include "test_helpers.hpp"

#include <cstdio>
#include <cstdlib>

namespace duckdb {

class TestEnvironmentScope {
public:
	TestEnvironmentScope()
	    : working_directory(FileSystem::GetWorkingDirectory()), configuration(TestConfiguration::Get()),
	      helper_state(CaptureTestHelperState()) {
		auto value = std::getenv(HomeVariable());
		has_home = value != nullptr;
		if (value) {
			home = value;
		}
		ResetTestHelperState();
		TestConfiguration::Get() = TestConfiguration();
	}

	~TestEnvironmentScope() {
#ifdef DUCKDB_WINDOWS
		auto result = _putenv_s(HomeVariable(), has_home ? home.c_str() : "");
#else
		auto result = has_home ? setenv(HomeVariable(), home.c_str(), 1) : unsetenv(HomeVariable());
#endif
		if (result != 0) {
			std::fprintf(stderr, "Failed to restore %s after running tests\n", HomeVariable());
		}
		try {
			FileSystem::SetWorkingDirectory(working_directory);
		} catch (std::exception &ex) {
			std::fprintf(stderr, "Failed to restore working directory after running tests: %s\n", ex.what());
		}
		TestConfiguration::Get() = std::move(configuration);
		RestoreTestHelperState(*helper_state);
	}

	TestEnvironmentScope(const TestEnvironmentScope &) = delete;
	TestEnvironmentScope &operator=(const TestEnvironmentScope &) = delete;

private:
	static const char *HomeVariable() {
#ifdef DUCKDB_WINDOWS
		return "USERPROFILE";
#else
		return "HOME";
#endif
	}

	string working_directory;
	TestConfiguration configuration;
	shared_ptr<TestHelperState> helper_state;
	FailureSummary::Scope failure_summary;
	bool has_home;
	string home;
};

} // namespace duckdb
