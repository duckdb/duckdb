#include "duckdb.hpp"

#if defined(DUCKDB_FUZZER) && defined(__linux__)
#include <cerrno>
#include <cstdio>
#include <cstring>
#include <filesystem>
#include <unordered_set>

#include <fcntl.h>
#include <linux/landlock.h>
#include <sys/prctl.h>
#include <sys/syscall.h>
#include <unistd.h>
#endif

namespace duckdb {

#if defined(DUCKDB_FUZZER) && defined(__linux__)
static constexpr __u64 LANDLOCK_WRITE_ACCESS_MASK =
    LANDLOCK_ACCESS_FS_WRITE_FILE | LANDLOCK_ACCESS_FS_REMOVE_DIR | LANDLOCK_ACCESS_FS_REMOVE_FILE |
    LANDLOCK_ACCESS_FS_MAKE_CHAR | LANDLOCK_ACCESS_FS_MAKE_DIR | LANDLOCK_ACCESS_FS_MAKE_REG |
    LANDLOCK_ACCESS_FS_MAKE_SOCK | LANDLOCK_ACCESS_FS_MAKE_FIFO | LANDLOCK_ACCESS_FS_MAKE_BLOCK |
    LANDLOCK_ACCESS_FS_MAKE_SYM | LANDLOCK_ACCESS_FS_REFER | LANDLOCK_ACCESS_FS_TRUNCATE;

static int LandlockCreateRuleset(struct landlock_ruleset_attr *attr, size_t size, __u32 flags) {
	return static_cast<int>(syscall(SYS_landlock_create_ruleset, attr, size, flags));
}

static int LandlockAddRule(int ruleset_fd, enum landlock_rule_type rule_type, const void *rule_attr, __u32 flags) {
	return static_cast<int>(syscall(SYS_landlock_add_rule, ruleset_fd, rule_type, rule_attr, flags));
}

static int LandlockRestrictSelf(int ruleset_fd, __u32 flags) {
	return static_cast<int>(syscall(SYS_landlock_restrict_self, ruleset_fd, flags));
}

bool ApplyLinuxLandlockSandbox(const vector<string> &raw_writable_dirs) {
	std::unordered_set<string> seen;
	vector<string> writable_dirs;
	writable_dirs.reserve(raw_writable_dirs.size());

	for (auto &raw_path : raw_writable_dirs) {
		if (raw_path.empty()) {
			fprintf(stderr, "Error: empty --writable-dir value is not allowed\n");
			return false;
		}
		std::filesystem::path path(raw_path);
		std::error_code ec;
		std::filesystem::create_directories(path, ec);
		if (ec && !std::filesystem::exists(path, ec)) {
			fprintf(stderr, "Error: failed to create writable directory '%s': %s\n", raw_path.c_str(),
			        ec.message().c_str());
			return false;
		}
		auto normalized = std::filesystem::weakly_canonical(path, ec);
		if (ec) {
			normalized = std::filesystem::absolute(path, ec);
			if (ec) {
				fprintf(stderr, "Error: failed to resolve writable directory '%s': %s\n", raw_path.c_str(),
				        ec.message().c_str());
				return false;
			}
		}
		auto normalized_path = normalized.lexically_normal().string();
		if (seen.insert(normalized_path).second) {
			writable_dirs.push_back(normalized_path);
		}
	}

	struct landlock_ruleset_attr ruleset_attr = {};
	ruleset_attr.handled_access_fs = LANDLOCK_WRITE_ACCESS_MASK;

	int ruleset_fd = LandlockCreateRuleset(&ruleset_attr, sizeof(ruleset_attr), 0);
	if (ruleset_fd < 0) {
		fprintf(stderr, "Error: landlock_create_ruleset failed: %s\n", strerror(errno));
		return false;
	}

	for (auto &writable_dir : writable_dirs) {
		int dir_fd = open(writable_dir.c_str(), O_PATH | O_CLOEXEC);
		if (dir_fd < 0) {
			fprintf(stderr, "Error: failed to open writable dir '%s': %s\n", writable_dir.c_str(), strerror(errno));
			close(ruleset_fd);
			return false;
		}
		struct landlock_path_beneath_attr path_beneath = {};
		path_beneath.allowed_access = ruleset_attr.handled_access_fs;
		path_beneath.parent_fd = dir_fd;
		if (LandlockAddRule(ruleset_fd, LANDLOCK_RULE_PATH_BENEATH, &path_beneath, 0) < 0) {
			fprintf(stderr, "Error: landlock_add_rule failed for '%s': %s\n", writable_dir.c_str(), strerror(errno));
			close(dir_fd);
			close(ruleset_fd);
			return false;
		}
		close(dir_fd);
	}

	if (prctl(PR_SET_NO_NEW_PRIVS, 1, 0, 0, 0)) {
		fprintf(stderr, "Error: prctl(PR_SET_NO_NEW_PRIVS) failed: %s\n", strerror(errno));
		close(ruleset_fd);
		return false;
	}
	if (LandlockRestrictSelf(ruleset_fd, 0) < 0) {
		fprintf(stderr, "Error: landlock_restrict_self failed: %s\n", strerror(errno));
		close(ruleset_fd);
		return false;
	}
	close(ruleset_fd);
	return true;
}
#endif

} // namespace duckdb
