#include "duckdb/main/profiler/samply.hpp"

#include "duckdb/common/string_util.hpp"
#include "utf8proc_wrapper.hpp"

#include <cerrno>
#include <cstdio>

#if defined(__linux__) || defined(__APPLE__)
#include <cstdlib>
#include <fcntl.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <time.h>
#include <unistd.h>
#endif

#ifdef __linux__
#include <sys/mman.h>
#include <sys/syscall.h>
#endif

#ifdef __APPLE__
#include <mach/mach_time.h>
#include <pthread.h>
#endif

namespace duckdb {

string SamplyQueryMarkerName(const string &query) {
	static constexpr idx_t MAX_QUERY_CHARACTERS = 500;
	static constexpr idx_t MAX_MARKER_NAME_BYTES = 900;
	const string prefix = "Query: ";
	const string ellipsis = "…";

	string normalized_query;
	normalized_query.reserve(MinValue<idx_t>(query.size(), MAX_MARKER_NAME_BYTES));
	bool pending_space = false;
	for (const auto character : query) {
		if (StringUtil::CharacterIsSpace(character)) {
			pending_space = !normalized_query.empty();
			continue;
		}
		if (pending_space) {
			normalized_query += ' ';
			pending_space = false;
		}
		normalized_query += character == '\0' ? '?' : character;
	}
	if (normalized_query.empty()) {
		return prefix + "<empty>";
	}
	if (!Utf8Proc::IsValid(normalized_query.c_str(), normalized_query.size())) {
		Utf8Proc::MakeValid(&normalized_query[0], normalized_query.size());
	}

	idx_t character_count = 0;
	idx_t query_bytes = 0;
	while (query_bytes < normalized_query.size() && character_count < MAX_QUERY_CHARACTERS) {
		auto next_position =
		    Utf8Proc::NextGraphemeCluster(normalized_query.c_str(), normalized_query.size(), query_bytes);
		if (prefix.size() + next_position > MAX_MARKER_NAME_BYTES) {
			break;
		}
		query_bytes = next_position;
		character_count++;
	}

	if (query_bytes == normalized_query.size()) {
		return prefix + normalized_query;
	}
	if (character_count == MAX_QUERY_CHARACTERS) {
		query_bytes = Utf8Proc::PreviousGraphemeCluster(normalized_query.c_str(), normalized_query.size(), query_bytes);
	}
	while (prefix.size() + query_bytes + ellipsis.size() > MAX_MARKER_NAME_BYTES) {
		query_bytes = Utf8Proc::PreviousGraphemeCluster(normalized_query.c_str(), normalized_query.size(), query_bytes);
	}
	return prefix + normalized_query.substr(0, query_bytes) + ellipsis;
}

#if defined(__linux__) || defined(__APPLE__)

static uint64_t SamplyTimestamp() noexcept {
#ifdef __APPLE__
	static mach_timebase_info_data_t timebase = [] {
		mach_timebase_info_data_t result;
		mach_timebase_info(&result);
		return result;
	}();
	auto ticks = mach_absolute_time();
	auto scale = reinterpret_cast<const uint32_t *>(&timebase);
	return static_cast<uint64_t>((static_cast<__uint128_t>(ticks) * scale[0]) / scale[1]);
#else
	struct timespec timestamp;
	if (clock_gettime(CLOCK_MONOTONIC, &timestamp) != 0) {
		return 0;
	}
	return static_cast<uint64_t>(timestamp.tv_sec) * 1000000000ULL + static_cast<uint64_t>(timestamp.tv_nsec);
#endif
}

static uint64_t SamplyThreadId() noexcept {
#ifdef __APPLE__
	uint64_t thread_id = 0;
	if (pthread_threadid_np(nullptr, &thread_id) != 0) {
		return 0;
	}
	return static_cast<uint32_t>(thread_id);
#else
	return static_cast<uint64_t>(syscall(SYS_gettid));
#endif
}

struct SamplyDirectoryState {
	SamplyDirectoryState() noexcept : path {0} {
		const char *temporary_directory = getenv("TMPDIR");
		if (!temporary_directory || temporary_directory[0] == '\0') {
			temporary_directory = "/tmp";
		}
		auto length = snprintf(path, sizeof(path), "%s/duckdb-samply-%llu-XXXXXX", temporary_directory,
		                       static_cast<unsigned long long>(getpid()));
		if (length <= 0 || static_cast<idx_t>(length) >= sizeof(path) || !mkdtemp(path)) {
			path[0] = '\0';
		}
	}

	char path[4096];
};

static const char *SamplyDirectory() noexcept {
	static SamplyDirectoryState directory;
	return directory.path[0] == '\0' ? nullptr : directory.path;
}

#endif

SamplyMarkerWriter::SamplyMarkerWriter(const char *directory_p) noexcept
    : directory(directory_p), file_descriptor(-1), failed(false), path {0} {
}

SamplyMarkerWriter::~SamplyMarkerWriter() {
#if defined(__linux__) || defined(__APPLE__)
	if (file_descriptor >= 0) {
		close(file_descriptor);
	}
#endif
}

bool SamplyMarkerWriter::Initialize() noexcept {
#if defined(__linux__) || defined(__APPLE__)
	if (failed) {
		return false;
	}
	if (file_descriptor >= 0) {
		return true;
	}
	if (!directory) {
		directory = SamplyDirectory();
	}
	if (!directory) {
		failed = true;
		return false;
	}
	auto length =
	    snprintf(path, sizeof(path), "%s/marker-%llu-%llu.txt", directory, static_cast<unsigned long long>(getpid()),
	             static_cast<unsigned long long>(SamplyThreadId()));
	if (length <= 0 || static_cast<idx_t>(length) >= sizeof(path)) {
		failed = true;
		return false;
	}
	file_descriptor = open(path, O_RDWR | O_CREAT | O_APPEND | O_CLOEXEC, S_IRUSR | S_IWUSR);
	if (file_descriptor < 0) {
		failed = true;
		return false;
	}
#ifdef __linux__
	if (ftruncate(file_descriptor, 1) != 0) {
		failed = true;
		return false;
	}
	auto mapping = mmap(nullptr, 1, PROT_READ | PROT_EXEC, MAP_PRIVATE, file_descriptor, 0);
	if (mapping == MAP_FAILED) {
		failed = true;
		return false;
	}
	munmap(mapping, 1);
	if (ftruncate(file_descriptor, 0) != 0) {
		failed = true;
		return false;
	}
#endif
	return true;
#else
	failed = true;
	return false;
#endif
}

bool SamplyMarkerWriter::Write(const char *data, idx_t size) noexcept {
#if defined(__linux__) || defined(__APPLE__)
	if (!Initialize()) {
		return false;
	}
	idx_t offset = 0;
	while (offset < size) {
		auto written = write(file_descriptor, data + offset, size - offset);
		if (written < 0 && errno == EINTR) {
			continue;
		}
		if (written <= 0) {
			failed = true;
			return false;
		}
		offset += static_cast<idx_t>(written);
	}
	return true;
#else
	return false;
#endif
}

bool SamplyMarkerWriter::WriteMarker(uint64_t start_ns, uint64_t end_ns, const char *name) noexcept {
#if defined(__linux__) || defined(__APPLE__)
	char marker[1024];
	auto length = snprintf(marker, sizeof(marker), "%llu %llu %s\n", static_cast<unsigned long long>(start_ns),
	                       static_cast<unsigned long long>(end_ns), name);
	if (length <= 0 || static_cast<idx_t>(length) >= sizeof(marker)) {
		failed = true;
		return false;
	}
	return Write(marker, static_cast<idx_t>(length));
#else
	return false;
#endif
}

const char *SamplyMarkerWriter::GetPath() const noexcept {
	return path;
}

static SamplyMarkerWriter &ThreadMarkerWriter() noexcept {
	thread_local SamplyMarkerWriter writer;
	return writer;
}

SamplyMarker::SamplyMarker() noexcept : start_ns(0), active(false) {
}

SamplyMarker::SamplyMarker(bool enabled) noexcept : start_ns(0), active(false) {
#if defined(__linux__) || defined(__APPLE__)
	if (enabled) {
		start_ns = SamplyTimestamp();
		active = start_ns != 0;
	}
#else
	(void)enabled;
#endif
}

SamplyMarker::SamplyMarker(SamplyMarker &&other) noexcept : start_ns(other.start_ns), active(other.active) {
	other.active = false;
}

SamplyMarker &SamplyMarker::operator=(SamplyMarker &&other) noexcept {
	start_ns = other.start_ns;
	active = other.active;
	other.active = false;
	return *this;
}

void SamplyMarker::End(const char *name) noexcept {
#if defined(__linux__) || defined(__APPLE__)
	if (active) {
		active = false;
		ThreadMarkerWriter().WriteMarker(start_ns, SamplyTimestamp(), name);
	}
#else
	(void)name;
#endif
}

void SamplyMarker::Reset() noexcept {
	active = false;
}

} // namespace duckdb
