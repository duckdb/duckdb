#include "duckdb/common/cgroups.hpp"

#include "duckdb/common/string_util.hpp"
#include "duckdb/common/file_system.hpp"
#include "duckdb/common/limits.hpp"
#include "duckdb/common/types/cast_helpers.hpp"
#include "duckdb/common/operator/cast_operators.hpp"
#include "duckdb/common/printer.hpp"

#include <cinttypes>

namespace duckdb {

#if defined(__linux__) && !defined(DUCKDB_WASM)

namespace {

static constexpr const char *CGROUP_PATH = "/proc/self/cgroup";
static constexpr const idx_t DEFAULT_CGROUP_FILE_BUFFER_SIZE = 1024;

struct CGroupEntry {
public:
	CGroupEntry(idx_t hierarchy_id, vector<string> &&controller_list, const string &cgroup_path)
	    : hierarchy_id(hierarchy_id), controller_list(std::move(controller_list)), cgroup_path(cgroup_path) {
	}

public:
	idx_t hierarchy_id;
	vector<string> controller_list;
	string cgroup_path;
};

static vector<CGroupEntry> ParseGroupEntries(FileSystem &fs) {
	vector<CGroupEntry> result;
	if (!fs.FileExists(CGROUP_PATH)) {
		return result;
	}

	auto handle = fs.OpenFile(CGROUP_PATH, FileFlags::FILE_FLAGS_READ);
	string cgroup_file_content;
	int64_t file_size = NumericCast<int64_t>(handle->GetFileSize());
	if (file_size != 0) {
		auto buffer = make_unsafe_uniq_array_uninitialized<char>(file_size + 1);
		auto bytes_read = fs.Read(*handle, buffer.get(), file_size);
		buffer[bytes_read] = '\0';
		cgroup_file_content = string(buffer.get());
	} else {
		char buffer[DEFAULT_CGROUP_FILE_BUFFER_SIZE];
		auto bytes_read = fs.Read(*handle, buffer, sizeof(buffer) - 1);
		buffer[bytes_read] = '\0';
		cgroup_file_content = string(buffer);
	}
	Printer::PrintF("cgroup_file_content size: %d", cgroup_file_content.size());
	Printer::PrintF("cgroup_file_content:\n%s", cgroup_file_content);

	size_t pos = 0;
	string line;
	while ((pos = cgroup_file_content.find('\n')) != string::npos) {
		line = cgroup_file_content.substr(0, pos);
		auto parts = StringUtil::Split(line, ":");
		if (parts.size() != 3) {
			//! cgroup entries are in this format:
			// hierarchy-ID:controller-list:cgroup-path
			break;
		}
		auto hierarchy_id = std::stoi(parts[0]);
		auto controller_list = StringUtil::Split(parts[1], ",");
		auto cgroup_path = parts[2] == "/" ? "" : parts[2];
		result.emplace_back(hierarchy_id, std::move(controller_list), cgroup_path);
		cgroup_file_content.erase(0, pos + 1);
	}
	return result;
}

static optional_idx GetCPUCountV2(const CGroupEntry &entry, FileSystem &fs) {
	static constexpr const char *CPU_MAX = "/sys/fs/cgroup%s/cpu.max";

	auto cpu_max = StringUtil::Format(CPU_MAX, entry.cgroup_path);
	Printer::PrintF("Reading CPU cgroupsv2 from %s", cpu_max);

	//! See https://docs.kernel.org/scheduler/sched-bwc.html
	//! run-time replenished within a period (in microseconds)
	int64_t quota;
	//! the length of a period (in microseconds)
	int64_t period;

	if (!fs.FileExists(cpu_max)) {
		return optional_idx();
	}

	// cgroup v2
	char byte_buffer[1000];
	auto handle = fs.OpenFile(cpu_max, FileFlags::FILE_FLAGS_READ);
	int64_t read_bytes = fs.Read(*handle, (void *)byte_buffer, 999);
	byte_buffer[read_bytes] = '\0';
	if (std::sscanf(byte_buffer, "%" SCNd64 " %" SCNd64 "", &quota, &period) != 2) {
		return optional_idx();
	}

	if (quota > 0 && period > 0) {
		return idx_t(std::ceil((double)quota / (double)period));
	}
	return optional_idx();
}

static optional_idx GetCPUCountV1(const CGroupEntry &entry, FileSystem &fs) {
	static constexpr const char *CFS_QUOTA = "/sys/fs/cgroup/cpu%s/cpu.cfs_quota_us";
	static constexpr const char *CFS_PERIOD = "/sys/fs/cgroup/cpu%s/cpu.cfs_period_us";

	auto cfs_quota = StringUtil::Format(CFS_QUOTA, entry.cgroup_path);
	auto cfs_period = StringUtil::Format(CFS_PERIOD, entry.cgroup_path);
	Printer::PrintF("Reading CPU quota cgroupsv1 from %s", cfs_quota);
	Printer::PrintF("Reading CPU period cgroupsv1 from %s", cfs_period);

	if (!fs.FileExists(cfs_quota) || !fs.FileExists(cfs_period)) {
		return optional_idx();
	}

	//! See https://docs.kernel.org/scheduler/sched-bwc.html
	//! run-time replenished within a period (in microseconds)
	int64_t quota;
	//! the length of a period (in microseconds)
	int64_t period;

	// cgroup v1
	char byte_buffer[1000];
	{
		auto handle = fs.OpenFile(cfs_quota, FileFlags::FILE_FLAGS_READ);
		int64_t read_bytes = fs.Read(*handle, (void *)byte_buffer, 999);
		byte_buffer[read_bytes] = '\0';
		if (std::sscanf(byte_buffer, "%" SCNd64 "", &quota) != 1) {
			return optional_idx();
		}
	}
	{
		auto handle = fs.OpenFile(cfs_period, FileFlags::FILE_FLAGS_READ);
		int64_t read_bytes = fs.Read(*handle, (void *)byte_buffer, 999);
		byte_buffer[read_bytes] = '\0';
		if (std::sscanf(byte_buffer, "%" SCNd64 "", &period) != 1) {
			return optional_idx();
		}
	}

	if (quota > 0 && period > 0) {
		return idx_t(std::ceil((double)quota / (double)period));
	}
	return optional_idx();
}

static optional_idx ReadMemoryLimit(FileSystem &fs, const string &file_path) {
	if (!fs.FileExists(file_path)) {
		return optional_idx();
	}

	auto handle = fs.OpenFile(file_path, FileFlags::FILE_FLAGS_READ);
	char buffer[100];
	auto bytes_read = fs.Read(*handle, buffer, 99);
	buffer[bytes_read] = '\0';

	auto contents = string_t(buffer);
	idx_t value;
	if (TryCast::Operation<string_t, idx_t>(contents, value)) {
		return optional_idx(value);
	}
	return optional_idx();
}

} // namespace

optional_idx CGroups::GetMemoryLimit(FileSystem &fs) {
	optional_idx root_entry;
	optional_idx memory_entry;

	auto cgroup_entries = ParseGroupEntries(fs);
	for (idx_t i = 0; i < cgroup_entries.size(); i++) {
		auto &entry = cgroup_entries[i];
		Printer::PrintF("HierarchyId: %d | ControllerList: %s | CGroupPath: %s", entry.hierarchy_id,
		                StringUtil::Join(entry.controller_list, ", "), entry.cgroup_path);
		auto &controller_list = entry.controller_list;
		if (entry.hierarchy_id == 0 && controller_list.empty()) {
			root_entry = i;
			continue;
		}
		for (auto &controller : controller_list) {
			if (controller == "memory") {
				memory_entry = i;
				continue;
			}
		}
	}

	if (root_entry.IsValid()) {
		Printer::PrintF("Has root_entry");
		auto &entry = cgroup_entries[root_entry.GetIndex()];
		auto path = StringUtil::Format("/sys/fs/cgroup%s/memory.max", entry.cgroup_path);
		auto memory_limit = ReadMemoryLimit(fs, path.c_str());
		if (memory_limit.IsValid()) {
			Printer::PrintF("Found limit at %s", path);
			return memory_limit;
		}
		Printer::PrintF("Could not read limit from %s", path);
	}
	if (memory_entry.IsValid()) {
		Printer::PrintF("Has memory_entry");
		auto &entry = cgroup_entries[memory_entry.GetIndex()];
		auto path = StringUtil::Format("/sys/fs/cgroup/memory%s/memory.limit_in_bytes", entry.cgroup_path);
		auto memory_limit = ReadMemoryLimit(fs, path.c_str());
		if (memory_limit.IsValid()) {
			Printer::PrintF("Found limit at %s", path);
			return memory_limit;
		}
		Printer::PrintF("Could not read limit from %s", path);
	}
	return optional_idx();
}

idx_t CGroups::GetCPULimit(FileSystem &fs, idx_t physical_cores) {
	optional_idx root_entry;
	optional_idx cpu_entry;

	auto cgroup_entries = ParseGroupEntries(fs);
	for (idx_t i = 0; i < cgroup_entries.size(); i++) {
		auto &entry = cgroup_entries[i];
		auto &controller_list = entry.controller_list;
		if (entry.hierarchy_id == 0 && controller_list.empty()) {
			root_entry = i;
			continue;
		}
		for (auto &controller : controller_list) {
			if (controller == "cpu") {
				cpu_entry = i;
				continue;
			}
		}
	}

	if (root_entry.IsValid()) {
		Printer::PrintF("Has root_entry");
		auto &entry = cgroup_entries[root_entry.GetIndex()];
		auto res = GetCPUCountV2(entry, fs);
		if (res.IsValid()) {
			Printer::PrintF("Found limit with root_entry");
			return res.GetIndex();
		}
		Printer::PrintF("Could not find limit with root_entry");
	}
	if (cpu_entry.IsValid()) {
		auto &entry = cgroup_entries[cpu_entry.GetIndex()];
		auto res = GetCPUCountV1(entry, fs);
		if (res.IsValid()) {
			Printer::PrintF("Found limit with cpu_entry");
			return res.GetIndex();
		}
		Printer::PrintF("Could not find limit with cpu_entry");
	}
	return physical_cores;
}

#else

optional_idx CGroups::GetMemoryLimit(FileSystem &fs) {
	return optional_idx();
}

idx_t CGroups::GetCPULimit(FileSystem &fs, idx_t physical_cores) {
	return physical_cores;
}

#endif

} // namespace duckdb
