#include "duckdb/common/cgroups.hpp"

#include "duckdb/common/string_util.hpp"
#include "duckdb/common/file_system.hpp"
#include "duckdb/common/limits.hpp"
#include "duckdb/common/types/cast_helpers.hpp"
#include "duckdb/common/operator/cast_operators.hpp"

#include <cinttypes>

namespace duckdb {

optional_idx CGroups::GetMemoryLimit(FileSystem &fs) {
	// First, try cgroup v2
	auto cgroup_v2_limit = GetCGroupV2MemoryLimit(fs);
	if (cgroup_v2_limit.IsValid()) {
		return cgroup_v2_limit;
	}

	// If cgroup v2 fails, try cgroup v1
	return GetCGroupV1MemoryLimit(fs);
}

optional_idx CGroups::GetCGroupV2MemoryLimit(FileSystem &fs) {
#if defined(__linux__) && !defined(DUCKDB_WASM)
	const char *cgroup_self = "/proc/self/cgroup";
	const char *memory_max = "/sys/fs/cgroup/%s/memory.max";

	if (!fs.FileExists(cgroup_self)) {
		return optional_idx();
	}

	string cgroup_path = ReadCGroupPath(fs, cgroup_self);
	if (cgroup_path.empty()) {
		return optional_idx();
	}

	char memory_max_path[256];
	snprintf(memory_max_path, sizeof(memory_max_path), memory_max, cgroup_path.c_str());

	if (!fs.FileExists(memory_max_path)) {
		return optional_idx();
	}

	return ReadCGroupValue(fs, memory_max_path);
#else
	return optional_idx();
#endif
}

optional_idx CGroups::GetCGroupV1MemoryLimit(FileSystem &fs) {
#if defined(__linux__) && !defined(DUCKDB_WASM)
	const char *cgroup_self = "/proc/self/cgroup";
	const char *memory_limit = "/sys/fs/cgroup/memory/%s/memory.limit_in_bytes";

	if (!fs.FileExists(cgroup_self)) {
		return optional_idx();
	}

	string memory_cgroup_path = ReadMemoryCGroupPath(fs, cgroup_self);
	if (memory_cgroup_path.empty()) {
		return optional_idx();
	}

	char memory_limit_path[256];
	snprintf(memory_limit_path, sizeof(memory_limit_path), memory_limit, memory_cgroup_path.c_str());

	if (!fs.FileExists(memory_limit_path)) {
		return optional_idx();
	}

	return ReadCGroupValue(fs, memory_limit_path);
#else
	return optional_idx();
#endif
}

string CGroups::ReadCGroupPath(FileSystem &fs, const char *cgroup_file) {
#if defined(__linux__) && !defined(DUCKDB_WASM)
	auto handle = fs.OpenFile(cgroup_file, FileFlags::FILE_FLAGS_READ);
	char buffer[1024];
	auto bytes_read = fs.Read(*handle, buffer, sizeof(buffer) - 1);
	buffer[bytes_read] = '\0';

	// For cgroup v2, we're looking for a single line with "0::/path"
	string content(buffer);
	auto pos = content.find("::");
	if (pos != string::npos) {
		// remove trailing \n
		auto pos2 = content.find('\n', pos + 2);
		if (pos2 != string::npos) {
			return content.substr(pos + 2, pos2 - (pos + 2));
		} else {
			return content.substr(pos + 2);
		}
	}
#endif
	return "";
}

string CGroups::ReadMemoryCGroupPath(FileSystem &fs, const char *cgroup_file) {
#if defined(__linux__) && !defined(DUCKDB_WASM)
	auto handle = fs.OpenFile(cgroup_file, FileFlags::FILE_FLAGS_READ);
	char buffer[1024];
	auto bytes_read = fs.Read(*handle, buffer, sizeof(buffer) - 1);
	buffer[bytes_read] = '\0';

	// For cgroup v1, we're looking for a line with "memory:/path"
	string content(buffer);
	size_t pos = 0;
	string line;
	while ((pos = content.find('\n')) != string::npos) {
		line = content.substr(0, pos);
		if (line.find("memory:") == 0) {
			return line.substr(line.find(':') + 1);
		}
		content.erase(0, pos + 1);
	}
#endif
	return "";
}

optional_idx CGroups::ReadCGroupValue(FileSystem &fs, const char *file_path) {
#if defined(__linux__) && !defined(DUCKDB_WASM)
	auto handle = fs.OpenFile(file_path, FileFlags::FILE_FLAGS_READ);
	char buffer[100];
	auto bytes_read = fs.Read(*handle, buffer, 99);
	buffer[bytes_read] = '\0';

	idx_t value;
	if (TryCast::Operation<string_t, idx_t>(string_t(buffer), value)) {
		return optional_idx(value);
	}
#endif
	return optional_idx();
}

idx_t CGroups::GetCPULimit(FileSystem &fs, idx_t physical_cores) {
#if defined(__linux__) && !defined(DUCKDB_WASM)
	static constexpr const char *cpu_max = "/sys/fs/cgroup/cpu.max";
	static constexpr const char *cfs_quota = "/sys/fs/cgroup/cpu/cpu.cfs_quota_us";
	static constexpr const char *cfs_period = "/sys/fs/cgroup/cpu/cpu.cfs_period_us";

	int64_t quota, period;
	char byte_buffer[1000];
	unique_ptr<FileHandle> handle;
	int64_t read_bytes;

	if (fs.FileExists(cpu_max)) {
		// cgroup v2
		handle = fs.OpenFile(cpu_max, FileFlags::FILE_FLAGS_READ);
		read_bytes = fs.Read(*handle, (void *)byte_buffer, 999);
		byte_buffer[read_bytes] = '\0';
		if (std::sscanf(byte_buffer, "%" SCNd64 " %" SCNd64 "", &quota, &period) != 2) {
			return physical_cores;
		}
	} else if (fs.FileExists(cfs_quota) && fs.FileExists(cfs_period)) {
		// cgroup v1
		handle = fs.OpenFile(cfs_quota, FileFlags::FILE_FLAGS_READ);
		read_bytes = fs.Read(*handle, (void *)byte_buffer, 999);
		byte_buffer[read_bytes] = '\0';
		if (std::sscanf(byte_buffer, "%" SCNd64 "", &quota) != 1) {
			return physical_cores;
		}

		handle = fs.OpenFile(cfs_period, FileFlags::FILE_FLAGS_READ);
		read_bytes = fs.Read(*handle, (void *)byte_buffer, 999);
		byte_buffer[read_bytes] = '\0';
		if (std::sscanf(byte_buffer, "%" SCNd64 "", &period) != 1) {
			return physical_cores;
		}
	} else {
		// No cgroup quota
		return physical_cores;
	}
	if (quota > 0 && period > 0) {
		return idx_t(std::ceil((double)quota / (double)period));
	} else {
		return physical_cores;
	}
#else
	return physical_cores;
#endif
}

} // namespace duckdb
