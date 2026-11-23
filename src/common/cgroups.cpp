#include "duckdb/common/cgroups.hpp"

#include "duckdb/common/string_util.hpp"
#include "duckdb/main/os_util.hpp"
#include "duckdb/common/limits.hpp"
#include "duckdb/common/types/cast_helpers.hpp"
#include "duckdb/common/operator/cast_operators.hpp"
#include "duckdb/common/printer.hpp"

#include <cinttypes>

namespace duckdb {

#if defined(__linux__) && !defined(DUCKDB_WASM)

namespace {

struct CGroupEntry {
public:
	CGroupEntry(idx_t hierarchy_id, vector<string> &&controller_list, const string &cgroup_path)
	    : hierarchy_id(hierarchy_id), controller_list(std::move(controller_list)), cgroup_path(cgroup_path) {
	}

public:
	bool IsRoot() const {
		if (hierarchy_id != 0) {
			return false;
		}
		if (controller_list.size() != 1) {
			return false;
		}
		return controller_list[0].empty();
	}

public:
	idx_t hierarchy_id;
	vector<string> controller_list;
	string cgroup_path;
};

static vector<CGroupEntry> ParseGroupEntries() {
	vector<CGroupEntry> result;
	string cgroup_file_content;
	if (!OSUtil::TryReadSystemFile(SystemFile::PROCESS_CGROUPS, string(), cgroup_file_content)) {
		return result;
	}

	auto lines = StringUtil::Split(cgroup_file_content, "\n");
	for (auto &line : lines) {
		//! NOTE: this can not use StringUtil::Split, as it counts '::' as a single delimiter
		vector<string> parts;
		auto it = line.begin();
		while (it != line.end()) {
			//! Don't make more than 3 splits
			auto next = parts.size() == 2 ? line.end() : std::find_if(it, line.end(), [](char c) { return c == ':'; });

			parts.emplace_back(it, next);
			if (next == line.end()) {
				break;
			}
			it = std::next(next);
		}

		if (parts.size() < 3) {
			//! cgroup entries are in this format:
			// hierarchy-ID:controller-list:cgroup-path
			break;
		}
		auto hierarchy_id = std::stoi(parts[0]);
		auto controller_list = StringUtil::Split(parts[1], ",");
		auto cgroup_path = parts[2] == "/" ? "" : parts[2];
		result.emplace_back(hierarchy_id, std::move(controller_list), cgroup_path);
	}
	return result;
}

static optional_idx GetCPUCountV2(const string &cgroup_path) {
	//! See https://docs.kernel.org/scheduler/sched-bwc.html
	//! run-time replenished within a period (in microseconds)
	int64_t quota;
	//! the length of a period (in microseconds)
	int64_t period;

	// cgroup v2
	string cpu_max;
	if (!OSUtil::TryReadSystemFile(SystemFile::CGROUP_V2_CPU_MAX, cgroup_path, cpu_max)) {
		return optional_idx();
	}
	if (std::sscanf(cpu_max.c_str(), "%" SCNd64 " %" SCNd64 "", &quota, &period) != 2) {
		return optional_idx();
	}

	if (quota > 0 && period > 0) {
		return idx_t(std::ceil((double)quota / (double)period));
	}
	return optional_idx();
}

static optional_idx GetCPUCountV1(const string &cgroup_path) {
	//! See https://docs.kernel.org/scheduler/sched-bwc.html
	//! run-time replenished within a period (in microseconds)
	int64_t quota;
	//! the length of a period (in microseconds)
	int64_t period;

	// cgroup v1
	string cfs_quota;
	string cfs_period;
	if (!OSUtil::TryReadSystemFile(SystemFile::CGROUP_V1_CPU_QUOTA, cgroup_path, cfs_quota) ||
	    !OSUtil::TryReadSystemFile(SystemFile::CGROUP_V1_CPU_PERIOD, cgroup_path, cfs_period)) {
		return optional_idx();
	}
	if (std::sscanf(cfs_quota.c_str(), "%" SCNd64 "", &quota) != 1) {
		return optional_idx();
	}
	if (std::sscanf(cfs_period.c_str(), "%" SCNd64 "", &period) != 1) {
		return optional_idx();
	}

	if (quota > 0 && period > 0) {
		return idx_t(std::ceil((double)quota / (double)period));
	}
	return optional_idx();
}

static optional_idx ReadMemoryLimit(SystemFile file, const string &cgroup_path) {
	string contents;
	if (!OSUtil::TryReadSystemFile(file, cgroup_path, contents)) {
		return optional_idx();
	}

	idx_t value;
	if (TryCast::Operation<string_t, idx_t>(string_t(contents.c_str()), value)) {
		return optional_idx(value);
	}
	return optional_idx();
}

} // namespace

optional_idx CGroups::GetMemoryLimit() {
	optional_idx root_entry;
	optional_idx memory_entry;

	auto cgroup_entries = ParseGroupEntries();
	for (idx_t i = 0; i < cgroup_entries.size(); i++) {
		auto &entry = cgroup_entries[i];
		auto &controller_list = entry.controller_list;
		if (entry.IsRoot()) {
			root_entry = i;
			continue;
		}
		for (auto &controller : controller_list) {
			if (controller == "memory") {
				memory_entry = i;
				break;
			}
		}
	}

	// TODO: we currently fall back to the root directory, because in virtual environments
	// the cgroups are often mapped to the root directory.
	// To properly handle this, we should parse and use the mapping in `/proc/self/mountinfo`
	if (memory_entry.IsValid()) {
		auto &entry = cgroup_entries[memory_entry.GetIndex()];
		auto memory_limit = ReadMemoryLimit(SystemFile::CGROUP_V1_MEMORY_LIMIT, entry.cgroup_path);
		if (memory_limit.IsValid()) {
			return memory_limit;
		}
		//! try falling back to the root directory
		memory_limit = ReadMemoryLimit(SystemFile::CGROUP_V1_MEMORY_LIMIT, string());
		if (memory_limit.IsValid()) {
			return memory_limit;
		}
	}
	if (root_entry.IsValid()) {
		auto &entry = cgroup_entries[root_entry.GetIndex()];
		auto memory_limit = ReadMemoryLimit(SystemFile::CGROUP_V2_MEMORY_MAX, entry.cgroup_path);
		if (memory_limit.IsValid()) {
			return memory_limit;
		}
		//! try falling back to the root directory
		memory_limit = ReadMemoryLimit(SystemFile::CGROUP_V2_MEMORY_MAX, string());
		if (memory_limit.IsValid()) {
			return memory_limit;
		}
	}
	return optional_idx();
}

idx_t CGroups::GetCPULimit(idx_t physical_cores) {
	optional_idx root_entry;
	optional_idx cpu_entry;

	auto cgroup_entries = ParseGroupEntries();
	for (idx_t i = 0; i < cgroup_entries.size(); i++) {
		auto &entry = cgroup_entries[i];
		auto &controller_list = entry.controller_list;
		if (entry.IsRoot()) {
			root_entry = i;
			continue;
		}
		for (auto &controller : controller_list) {
			if (controller == "cpu") {
				cpu_entry = i;
				break;
			}
		}
	}

	// TODO: we currently fall back to the root directory, because in virtual environments
	// the cgroups are often mapped to the root directory.
	// To properly handle this, we should parse and use the mapping in `/proc/self/mountinfo`
	if (cpu_entry.IsValid()) {
		auto &entry = cgroup_entries[cpu_entry.GetIndex()];
		auto res = GetCPUCountV1(entry.cgroup_path);
		if (res.IsValid()) {
			return res.GetIndex();
		}
		//! try falling back to the root directory
		res = GetCPUCountV1(string());
		if (res.IsValid()) {
			return res.GetIndex();
		}
	}
	if (root_entry.IsValid()) {
		auto &entry = cgroup_entries[root_entry.GetIndex()];
		auto res = GetCPUCountV2(entry.cgroup_path);
		if (res.IsValid()) {
			return res.GetIndex();
		}
		//! try falling back to the root directory
		res = GetCPUCountV2(string());
		if (res.IsValid()) {
			return res.GetIndex();
		}
	}
	return physical_cores;
}

#else

optional_idx CGroups::GetMemoryLimit() {
	return optional_idx();
}

idx_t CGroups::GetCPULimit(idx_t physical_cores) {
	return physical_cores;
}

#endif

} // namespace duckdb
