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
	const char *CGROUP_SELF = "/proc/self/cgroup";
	const char *MEMORY_MAX = "/sys/fs/cgroup/%s/memory.max";

	if (!fs.FileExists(CGROUP_SELF)) {
		return optional_idx();
	}

	string cgroup_path = ReadCGroupPath(fs, CGROUP_SELF);
	if (cgroup_path.empty()) {
		return optional_idx();
	}

	char memory_max_path[256];
	snprintf(memory_max_path, sizeof(memory_max_path), MEMORY_MAX, cgroup_path.c_str());

	if (!fs.FileExists(memory_max_path)) {
		return optional_idx();
	}

	return ReadCGroupValue(fs, memory_max_path);
}

optional_idx CGroups::GetCGroupV1MemoryLimit(FileSystem &fs) {
	const char *CGROUP_SELF = "/proc/self/cgroup";
	const char *MEMORY_LIMIT = "/sys/fs/cgroup/memory/%s/memory.limit_in_bytes";

	if (!fs.FileExists(CGROUP_SELF)) {
		return optional_idx();
	}

	string memory_cgroup_path = ReadMemoryCGroupPath(fs, CGROUP_SELF);
	if (memory_cgroup_path.empty()) {
		return optional_idx();
	}

	char memory_limit_path[256];
	snprintf(memory_limit_path, sizeof(memory_limit_path), MEMORY_LIMIT, memory_cgroup_path.c_str());

	if (!fs.FileExists(memory_limit_path)) {
		return optional_idx();
	}

	return ReadCGroupValue(fs, memory_limit_path);
}

string CGroups::ReadCGroupPath(FileSystem &fs, const char *cgroup_file) {
	auto handle = fs.OpenFile(cgroup_file, FileFlags::FILE_FLAGS_READ);
	char buffer[1024];
	idx_t bytes_read = fs.Read(*handle, buffer, sizeof(buffer) - 1);
	buffer[bytes_read] = '\0';

	// For cgroup v2, we're looking for a single line with "0::/path"
	string content(buffer);
	auto pos = content.find("::");
	if (pos != string::npos) {
		return content.substr(pos + 2);
	}

	return "";
}

string CGroups::ReadMemoryCGroupPath(FileSystem &fs, const char *cgroup_file) {
	auto handle = fs.OpenFile(cgroup_file, FileFlags::FILE_FLAGS_READ);
	char buffer[1024];
	idx_t bytes_read = fs.Read(*handle, buffer, sizeof(buffer) - 1);
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

	return "";
}

optional_idx CGroups::ReadCGroupValue(FileSystem &fs, const char *file_path) {
	auto handle = fs.OpenFile(file_path, FileFlags::FILE_FLAGS_READ);
	char buffer[100];
	idx_t bytes_read = fs.Read(*handle, buffer, 99);
	buffer[bytes_read] = '\0';

	idx_t value;
	if (TryCast::Operation<string_t, idx_t>(string_t(buffer), value)) {
		return optional_idx(value);
	}
	return optional_idx();
}

idx_t CGroups::GetCPULimit(FileSystem &fs, idx_t physical_cores) {
	static constexpr const char *CPU_MAX = "/sys/fs/cgroup/cpu.max";
	static constexpr const char *CFS_QUOTA = "/sys/fs/cgroup/cpu/cpu.cfs_quota_us";
	static constexpr const char *CFS_PERIOD = "/sys/fs/cgroup/cpu/cpu.cfs_period_us";

	int64_t quota, period;
	char byte_buffer[1000];
	unique_ptr<FileHandle> handle;
	int64_t read_bytes;

	if (fs.FileExists(CPU_MAX)) {
		// cgroup v2
		handle = fs.OpenFile(CPU_MAX, FileFlags::FILE_FLAGS_READ);
		read_bytes = fs.Read(*handle, (void *)byte_buffer, 999);
		byte_buffer[read_bytes] = '\0';
		if (std::sscanf(byte_buffer, "%" SCNd64 " %" SCNd64 "", &quota, &period) != 2) {
			return physical_cores;
		}
	} else if (fs.FileExists(CFS_QUOTA) && fs.FileExists(CFS_PERIOD)) {
		// cgroup v1
		handle = fs.OpenFile(CFS_QUOTA, FileFlags::FILE_FLAGS_READ);
		read_bytes = fs.Read(*handle, (void *)byte_buffer, 999);
		byte_buffer[read_bytes] = '\0';
		if (std::sscanf(byte_buffer, "%" SCNd64 "", &quota) != 1) {
			return physical_cores;
		}

		handle = fs.OpenFile(CFS_PERIOD, FileFlags::FILE_FLAGS_READ);
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
}

} // namespace duckdb
