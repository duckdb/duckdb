#include "duckdb/main/profiler/samply.hpp"

#include "duckdb/common/string_util.hpp"
#include "utf8proc_wrapper.hpp"

#include <cerrno>
#include <chrono>
#include <condition_variable>
#include <cstdio>
#include <cstring>
#include <limits>
#include <mutex>
#include <thread>
#include <unordered_map>
#include <vector>

#if defined(__linux__) || defined(__APPLE__)
#include <cstdlib>
#include <fcntl.h>
#include <sys/resource.h>
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
#include <mach/mach.h>
#include <mach/mach_time.h>
#include <net/if.h>
#include <net/route.h>
#include <pthread.h>
#include <sys/sysctl.h>
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
		const char *configured_directory = getenv("DUCKDB_SAMPLY_DIR");
		if (configured_directory && configured_directory[0] != '\0') {
			auto length = snprintf(path, sizeof(path), "%s", configured_directory);
			if (length <= 0 || static_cast<idx_t>(length) >= sizeof(path)) {
				path[0] = '\0';
			}
			return;
		}
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

SamplyCounterWriter::SamplyCounterWriter(const char *directory_p) noexcept
    : directory(directory_p), file_descriptor(-1), failed(false), wrote_clock(false), path {0} {
	try {
		buffer.reserve(64 * 1024);
	} catch (...) {
		failed = true;
	}
}

SamplyCounterWriter::~SamplyCounterWriter() {
	Flush();
#if defined(__linux__) || defined(__APPLE__)
	if (file_descriptor >= 0) {
		close(file_descriptor);
	}
#endif
}

bool SamplyCounterWriter::Initialize() noexcept {
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
	    snprintf(path, sizeof(path), "%s/counter-%llu.txt", directory, static_cast<unsigned long long>(getpid()));
	if (length <= 0 || static_cast<idx_t>(length) >= sizeof(path)) {
		failed = true;
		return false;
	}
	file_descriptor = open(path, O_WRONLY | O_CREAT | O_APPEND | O_CLOEXEC, S_IRUSR | S_IWUSR);
	if (file_descriptor < 0) {
		failed = true;
		return false;
	}
	return true;
#else
	failed = true;
	return false;
#endif
}

bool SamplyCounterWriter::Append(const char *data, idx_t size) noexcept {
	if (!Initialize()) {
		return false;
	}
	try {
		buffer.append(data, size);
		return true;
	} catch (...) {
		failed = true;
		return false;
	}
}

bool SamplyCounterWriter::WriteClock(uint64_t monotonic_ns, uint64_t unix_ns) noexcept {
	if (wrote_clock) {
		return true;
	}
	char record[96];
	auto length = snprintf(record, sizeof(record), "clock %llu %llu\n", static_cast<unsigned long long>(monotonic_ns),
	                       static_cast<unsigned long long>(unix_ns));
	if (length <= 0 || static_cast<idx_t>(length) >= sizeof(record)) {
		failed = true;
		return false;
	}
	if (!Append(record, static_cast<idx_t>(length))) {
		return false;
	}
	wrote_clock = true;
	return true;
}

bool SamplyCounterWriter::WriteSample(uint64_t monotonic_ns, const char *name, int64_t value) noexcept {
	char record[128];
	auto length = snprintf(record, sizeof(record), "%llu %s %lld\n", static_cast<unsigned long long>(monotonic_ns),
	                       name, static_cast<long long>(value));
	if (length <= 0 || static_cast<idx_t>(length) >= sizeof(record)) {
		failed = true;
		return false;
	}
	return Append(record, static_cast<idx_t>(length));
}

bool SamplyCounterWriter::Flush() noexcept {
#if defined(__linux__) || defined(__APPLE__)
	if (buffer.empty()) {
		return !failed;
	}
	if (!Initialize()) {
		return false;
	}
	idx_t offset = 0;
	while (offset < buffer.size()) {
		auto written = write(file_descriptor, buffer.data() + offset, buffer.size() - offset);
		if (written < 0 && errno == EINTR) {
			continue;
		}
		if (written <= 0) {
			failed = true;
			return false;
		}
		offset += static_cast<idx_t>(written);
	}
	buffer.clear();
	return true;
#else
	return false;
#endif
}

const char *SamplyCounterWriter::GetPath() const noexcept {
	return path;
}

#if defined(__linux__) || defined(__APPLE__)

static uint64_t SamplyUnixTimestamp() noexcept {
	struct timespec timestamp;
	if (clock_gettime(CLOCK_REALTIME, &timestamp) != 0) {
		return 0;
	}
	return static_cast<uint64_t>(timestamp.tv_sec) * 1000000000ULL + static_cast<uint64_t>(timestamp.tv_nsec);
}

struct SamplyNetworkInterface {
	string name;
	uint64_t received;
	uint64_t transmitted;
};

static bool SamplyReadRSS(uint64_t &rss) noexcept {
#ifdef __linux__
	FILE *file = fopen("/proc/self/statm", "r");
	if (!file) {
		return false;
	}
	unsigned long long resident_pages = 0;
	auto result = fscanf(file, "%*llu %llu", &resident_pages);
	fclose(file);
	if (result != 1) {
		return false;
	}
	auto page_size = sysconf(_SC_PAGESIZE);
	if (page_size <= 0 || resident_pages > std::numeric_limits<uint64_t>::max() / static_cast<uint64_t>(page_size)) {
		return false;
	}
	rss = resident_pages * static_cast<uint64_t>(page_size);
	return true;
#else
	mach_task_basic_info_data_t info;
	mach_msg_type_number_t count = MACH_TASK_BASIC_INFO_COUNT;
	if (task_info(mach_task_self(), MACH_TASK_BASIC_INFO, reinterpret_cast<task_info_t>(&info), &count) !=
	    KERN_SUCCESS) {
		return false;
	}
	rss = info.resident_size;
	return true;
#endif
}

class SamplyResourceSampler {
public:
	static shared_ptr<SamplyResourceSampler> Get() {
		static auto sampler = make_shared_ptr<SamplyResourceSampler>(nullptr, true);
		return sampler;
	}

	SamplyResourceSampler(const char *directory_p, bool periodic_sampling_p)
	    : memory_references(0), network_references(0), pending_final_tracks(0), completed_final_tracks(0),
	      stopping(false), periodic_sampling(periodic_sampling_p), directory(directory_p ? directory_p : ""),
	      writer(directory_p ? directory.c_str() : nullptr), network_buffer() {
		try {
			network_interfaces.reserve(32);
			network_buffer.reserve(16 * 1024);
		} catch (...) {
		}
	}

	~SamplyResourceSampler() {
		std::lock_guard<std::mutex> control_guard(control_mutex);
		{
			std::lock_guard<std::mutex> guard(lock);
			stopping = true;
			condition.notify_all();
		}
		if (worker.joinable()) {
			worker.join();
		}
	}

	void Add(uint8_t tracks) noexcept {
		std::lock_guard<std::mutex> control_guard(control_mutex);
		std::lock_guard<std::mutex> guard(lock);
		if (SamplyTrackEnabled(tracks, SamplyTrack::MEMORY)) {
			memory_references++;
		}
		if (SamplyTrackEnabled(tracks, SamplyTrack::NETWORK)) {
			network_references++;
		}
		if (!worker.joinable()) {
			stopping = false;
			try {
				worker = std::thread(&SamplyResourceSampler::Run, this);
			} catch (...) {
			}
		}
		condition.notify_all();
	}

	void Remove(uint8_t tracks) noexcept {
		std::lock_guard<std::mutex> control_guard(control_mutex);
		bool should_join;
		uint8_t final_tracks = 0;
		{
			std::lock_guard<std::mutex> guard(lock);
			if (SamplyTrackEnabled(tracks, SamplyTrack::MEMORY) && memory_references > 0) {
				memory_references--;
				if (memory_references == 0) {
					final_tracks |= static_cast<uint8_t>(SamplyTrack::MEMORY);
				}
			}
			if (SamplyTrackEnabled(tracks, SamplyTrack::NETWORK) && network_references > 0) {
				network_references--;
				if (network_references == 0) {
					final_tracks |= static_cast<uint8_t>(SamplyTrack::NETWORK);
				}
			}
			pending_final_tracks |= final_tracks;
			completed_final_tracks &= ~final_tracks;
			stopping = memory_references == 0 && network_references == 0;
			should_join = stopping;
			condition.notify_all();
		}
		if (should_join && worker.joinable()) {
			worker.join();
		} else if (final_tracks != 0 && worker.joinable()) {
			std::unique_lock<std::mutex> guard(lock);
			condition.wait(guard,
			               [this, final_tracks] { return (completed_final_tracks & final_tracks) == final_tracks; });
			completed_final_tracks &= ~final_tracks;
		}
	}

private:
	void SetLowPriority() noexcept {
#ifdef __APPLE__
		pthread_set_qos_class_self_np(QOS_CLASS_UTILITY, 0);
#else
		setpriority(PRIO_PROCESS, static_cast<id_t>(SamplyThreadId()), 10);
#endif
	}

	bool ReadNetwork() noexcept {
		network_interfaces.clear();
#ifdef __linux__
		FILE *file = fopen("/proc/net/dev", "r");
		if (!file) {
			return false;
		}
		char line[1024];
		while (fgets(line, sizeof(line), file)) {
			char name[64];
			unsigned long long received;
			unsigned long long transmitted;
			if (sscanf(line, " %63[^:]: %llu %*llu %*llu %*llu %*llu %*llu %*llu %*llu %llu", name, &received,
			           &transmitted) != 3 ||
			    strcmp(name, "lo") == 0) {
				continue;
			}
			try {
				network_interfaces.push_back({name, received, transmitted});
			} catch (...) {
				fclose(file);
				return false;
			}
		}
		fclose(file);
		return true;
#else
		int mib[6] = {CTL_NET, PF_ROUTE, 0, 0, NET_RT_IFLIST2, 0};
		size_t needed = 0;
		if (sysctl(mib, 6, nullptr, &needed, nullptr, 0) != 0) {
			return false;
		}
		try {
			if (network_buffer.size() < needed) {
				network_buffer.resize(needed);
			}
		} catch (...) {
			return false;
		}
		if (sysctl(mib, 6, network_buffer.data(), &needed, nullptr, 0) != 0) {
			return false;
		}
		char *current = network_buffer.data();
		char *end = current + needed;
		while (current + sizeof(if_msghdr) <= end) {
			auto message = reinterpret_cast<if_msghdr *>(current);
			if (message->ifm_msglen == 0 || current + message->ifm_msglen > end) {
				return false;
			}
			if (message->ifm_type == RTM_IFINFO2 && message->ifm_msglen >= sizeof(if_msghdr2)) {
				auto info = reinterpret_cast<if_msghdr2 *>(current);
				if ((info->ifm_flags & IFF_LOOPBACK) == 0) {
					char name[IF_NAMESIZE];
					if (if_indextoname(info->ifm_index, name)) {
						try {
							network_interfaces.push_back({name, info->ifm_data.ifi_ibytes, info->ifm_data.ifi_obytes});
						} catch (...) {
							return false;
						}
					}
				}
			}
			current += message->ifm_msglen;
		}
		return true;
#endif
	}

	void SampleNetwork(uint64_t timestamp) noexcept {
		if (!ReadNetwork()) {
			return;
		}
		uint64_t received_delta = 0;
		uint64_t transmitted_delta = 0;
		std::unordered_map<string, std::pair<uint64_t, uint64_t>> next_baselines;
		try {
			next_baselines.reserve(network_interfaces.size());
			for (const auto &interface : network_interfaces) {
				auto previous = network_baselines.find(interface.name);
				if (previous != network_baselines.end()) {
					if (interface.received >= previous->second.first) {
						received_delta += interface.received - previous->second.first;
					}
					if (interface.transmitted >= previous->second.second) {
						transmitted_delta += interface.transmitted - previous->second.second;
					}
				}
				next_baselines.emplace(interface.name, std::make_pair(interface.received, interface.transmitted));
			}
			network_baselines.swap(next_baselines);
		} catch (...) {
			return;
		}
		if (received_delta <= static_cast<uint64_t>(std::numeric_limits<int64_t>::max())) {
			writer.WriteSample(timestamp, "network-rx", static_cast<int64_t>(received_delta));
		}
		if (transmitted_delta <= static_cast<uint64_t>(std::numeric_limits<int64_t>::max())) {
			writer.WriteSample(timestamp, "network-tx", static_cast<int64_t>(transmitted_delta));
		}
	}

	void Sample(uint8_t tracks) noexcept {
		auto timestamp = SamplyTimestamp();
		if (SamplyTrackEnabled(tracks, SamplyTrack::MEMORY)) {
			uint64_t rss;
			if (SamplyReadRSS(rss) && rss <= static_cast<uint64_t>(std::numeric_limits<int64_t>::max())) {
				writer.WriteSample(timestamp, "rss", static_cast<int64_t>(rss));
			}
		}
		if (SamplyTrackEnabled(tracks, SamplyTrack::NETWORK)) {
			SampleNetwork(timestamp);
		}
	}

	void Run() noexcept {
		SetLowPriority();
		writer.WriteClock(SamplyTimestamp(), SamplyUnixTimestamp());
		auto next_sample = std::chrono::steady_clock::now();
		auto last_flush = next_sample;
		while (true) {
			uint8_t tracks;
			uint8_t final_tracks;
			{
				std::unique_lock<std::mutex> guard(lock);
				if (periodic_sampling) {
					condition.wait_until(guard, next_sample, [this] { return stopping || pending_final_tracks != 0; });
				} else {
					condition.wait(guard, [this] { return stopping || pending_final_tracks != 0; });
				}
				final_tracks = pending_final_tracks;
				pending_final_tracks = 0;
				if (final_tracks == 0 && stopping) {
					break;
				}
				tracks = final_tracks;
				if (tracks == 0) {
					if (memory_references > 0) {
						tracks |= static_cast<uint8_t>(SamplyTrack::MEMORY);
					}
					if (network_references > 0) {
						tracks |= static_cast<uint8_t>(SamplyTrack::NETWORK);
					}
				}
			}

			Sample(tracks);
			if (SamplyTrackEnabled(final_tracks, SamplyTrack::NETWORK)) {
				network_baselines.clear();
			}
			if (final_tracks != 0) {
				writer.Flush();
				std::lock_guard<std::mutex> guard(lock);
				completed_final_tracks |= final_tracks;
				condition.notify_all();
				continue;
			}

			auto now = std::chrono::steady_clock::now();
			if (now - last_flush >= std::chrono::seconds(1)) {
				writer.Flush();
				last_flush = now;
			}
			next_sample += std::chrono::milliseconds(5);
			while (next_sample <= now) {
				next_sample += std::chrono::milliseconds(5);
			}
		}
		writer.Flush();
	}

private:
	std::mutex control_mutex;
	std::mutex lock;
	std::condition_variable condition;
	idx_t memory_references;
	idx_t network_references;
	uint8_t pending_final_tracks;
	uint8_t completed_final_tracks;
	bool stopping;
	bool periodic_sampling;
	std::thread worker;
	string directory;
	SamplyCounterWriter writer;
	vector<SamplyNetworkInterface> network_interfaces;
	vector<char> network_buffer;
	std::unordered_map<string, std::pair<uint64_t, uint64_t>> network_baselines;
};

#endif

class SamplyResourceSubscription {
public:
#if defined(__linux__) || defined(__APPLE__)
	explicit SamplyResourceSubscription(uint8_t tracks_p)
	    : SamplyResourceSubscription(tracks_p, SamplyResourceSampler::Get()) {
	}

	SamplyResourceSubscription(uint8_t tracks_p, shared_ptr<SamplyResourceSampler> sampler_p) noexcept
	    : tracks(tracks_p), sampler(std::move(sampler_p)) {
		sampler->Add(tracks);
	}
#else
	explicit SamplyResourceSubscription(uint8_t tracks_p) noexcept : tracks(tracks_p) {
	}
#endif

	~SamplyResourceSubscription() {
#if defined(__linux__) || defined(__APPLE__)
		sampler->Remove(tracks);
#endif
	}

private:
	uint8_t tracks;
#if defined(__linux__) || defined(__APPLE__)
	shared_ptr<SamplyResourceSampler> sampler;
#endif
};

shared_ptr<SamplyResourceSubscription> StartSamplyResourceSampling(uint8_t tracks) noexcept {
	if (tracks == 0) {
		return nullptr;
	}
	try {
		return make_shared_ptr<SamplyResourceSubscription>(tracks);
	} catch (...) {
		return nullptr;
	}
}

shared_ptr<SamplyResourceSubscription> StartSamplyResourceSamplingForTesting(uint8_t tracks,
                                                                             const char *directory) noexcept {
#if defined(__linux__) || defined(__APPLE__)
	if (tracks == 0) {
		return nullptr;
	}
	try {
		auto sampler = make_shared_ptr<SamplyResourceSampler>(directory, false);
		return make_shared_ptr<SamplyResourceSubscription>(tracks, std::move(sampler));
	} catch (...) {
		return nullptr;
	}
#else
	return nullptr;
#endif
}

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
