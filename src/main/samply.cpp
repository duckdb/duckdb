#include "duckdb/main/profiler/samply.hpp"

#include "duckdb/common/allocator.hpp"
#include "duckdb/common/atomic.hpp"
#include "duckdb/common/http_util.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/types/blob.hpp"
#include "duckdb/common/types/string_type.hpp"

#include <cerrno>
#include <chrono>
#include <condition_variable>
#include <cstdio>
#include <cstring>
#include <limits>
#include <mutex>
#include <thread>

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
#include <sys/syscall.h>
#endif

#ifdef __APPLE__
#include <mach/mach_time.h>
#include <pthread.h>
#endif

namespace duckdb {

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
		buffer.reserve(idx_t(64) * 1024);
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

class SamplyResourceSampler {
public:
	static shared_ptr<SamplyResourceSampler> Get() {
		static auto sampler = make_shared_ptr<SamplyResourceSampler>(nullptr, true);
		return sampler;
	}

	SamplyResourceSampler(const char *directory_p, bool periodic_sampling_p)
	    : memory_references(0), network_references(0), http_references(0), pending_final_tracks(0),
	      completed_final_tracks(0), stopping(false), periodic_sampling(periodic_sampling_p),
	      directory(directory_p ? directory_p : ""), writer(directory_p ? directory.c_str() : nullptr),
	      received_baseline(HTTPUtil::GetTotalBytesReceived()), sent_baseline(HTTPUtil::GetTotalBytesSent()) {
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
			if (network_references == 0) {
				received_baseline.store(HTTPUtil::GetTotalBytesReceived(), std::memory_order_relaxed);
				sent_baseline.store(HTTPUtil::GetTotalBytesSent(), std::memory_order_relaxed);
			}
			network_references++;
		}
		if (SamplyTrackEnabled(tracks, SamplyTrack::HTTP)) {
			http_references++;
		}
		if ((memory_references > 0 || network_references > 0) && !worker.joinable()) {
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
			if (SamplyTrackEnabled(tracks, SamplyTrack::HTTP) && http_references > 0) {
				http_references--;
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

	bool HTTPEnabled() noexcept {
		std::lock_guard<std::mutex> guard(lock);
		return http_references > 0;
	}

private:
	void SetLowPriority() noexcept {
#ifdef __APPLE__
		pthread_set_qos_class_self_np(QOS_CLASS_UTILITY, 0);
#else
		setpriority(PRIO_PROCESS, static_cast<id_t>(SamplyThreadId()), 10);
#endif
	}

	void SampleNetwork(uint64_t timestamp) noexcept {
		auto received = HTTPUtil::GetTotalBytesReceived();
		auto sent = HTTPUtil::GetTotalBytesSent();
		auto previous_received = received_baseline.exchange(received, std::memory_order_relaxed);
		auto previous_sent = sent_baseline.exchange(sent, std::memory_order_relaxed);
		writer.WriteSample(timestamp, "http-download",
		                   static_cast<int64_t>(received >= previous_received ? received - previous_received : 0));
		writer.WriteSample(timestamp, "http-upload",
		                   static_cast<int64_t>(sent >= previous_sent ? sent - previous_sent : 0));
	}

	void Sample(uint8_t tracks) noexcept {
		auto timestamp = SamplyTimestamp();
		if (SamplyTrackEnabled(tracks, SamplyTrack::MEMORY)) {
			auto memory = Allocator::GetTrackedMemory();
			if (memory <= static_cast<idx_t>(std::numeric_limits<int64_t>::max())) {
				writer.WriteSample(timestamp, "tracked-memory", static_cast<int64_t>(memory));
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
	idx_t http_references;
	uint8_t pending_final_tracks;
	uint8_t completed_final_tracks;
	bool stopping;
	bool periodic_sampling;
	std::thread worker;
	string directory;
	SamplyCounterWriter writer;
	atomic<idx_t> received_baseline;
	atomic<idx_t> sent_baseline;
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

bool SamplyHTTPTrackEnabled() noexcept {
#if defined(__linux__) || defined(__APPLE__)
	return SamplyResourceSampler::Get()->HTTPEnabled();
#else
	return false;
#endif
}

SamplyHTTPWriter::SamplyHTTPWriter(const char *directory_p) noexcept
    : directory(directory_p), file_descriptor(-1), failed(false), path {0} {
}

SamplyHTTPWriter::~SamplyHTTPWriter() {
#if defined(__linux__) || defined(__APPLE__)
	if (file_descriptor >= 0) {
		close(file_descriptor);
	}
#endif
}

bool SamplyHTTPWriter::Initialize() noexcept {
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
	    snprintf(path, sizeof(path), "%s/http-%llu-%llu.txt", directory, static_cast<unsigned long long>(getpid()),
	             static_cast<unsigned long long>(SamplyThreadId()));
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

bool SamplyHTTPWriter::Write(const char *data, idx_t size) noexcept {
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

bool SamplyHTTPWriter::WriteAttempt(uint64_t start_unix_ns, uint64_t duration_ns, const char *method, const string &url,
                                    int32_t status_code, uint64_t bytes_received, int64_t time_to_first_byte_ns,
                                    const string &request_range, const string &response_content_range) noexcept {
#if defined(__linux__) || defined(__APPLE__)
	try {
		auto record = StringUtil::Format("1\t%llu\t%llu\t%d\t%llu\t%lld\t%s\t%s\t%s\t%s\n", start_unix_ns, duration_ns,
		                                 status_code, bytes_received, time_to_first_byte_ns, method,
		                                 Blob::ToBase64(string_t(url)), Blob::ToBase64(string_t(request_range)),
		                                 Blob::ToBase64(string_t(response_content_range)));
		return Write(record.data(), record.size());
	} catch (...) {
		failed = true;
		return false;
	}
#else
	return false;
#endif
}

const char *SamplyHTTPWriter::GetPath() const noexcept {
	return path;
}

static SamplyHTTPWriter &ThreadHTTPWriter() noexcept {
	thread_local SamplyHTTPWriter writer;
	return writer;
}

void WriteSamplyHTTPAttempt(uint64_t start_unix_ns, uint64_t duration_ns, const char *method, const string &url,
                            int32_t status_code, uint64_t bytes_received, int64_t time_to_first_byte_ns,
                            const string &request_range, const string &response_content_range) noexcept {
	ThreadHTTPWriter().WriteAttempt(start_unix_ns, duration_ns, method, url, status_code, bytes_received,
	                                time_to_first_byte_ns, request_range, response_content_range);
}

SamplyQueryWriter::SamplyQueryWriter(const char *directory_p) noexcept
    : directory(directory_p), file_descriptor(-1), failed(false), path {0} {
}

SamplyQueryWriter::~SamplyQueryWriter() {
#if defined(__linux__) || defined(__APPLE__)
	if (file_descriptor >= 0) {
		close(file_descriptor);
	}
#endif
}

bool SamplyQueryWriter::Initialize() noexcept {
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
	    snprintf(path, sizeof(path), "%s/query-%llu-%llu.jsonl", directory, static_cast<unsigned long long>(getpid()),
	             static_cast<unsigned long long>(SamplyThreadId()));
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

bool SamplyQueryWriter::Write(const char *data, idx_t size) noexcept {
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

bool SamplyQueryWriter::WriteProfile(uint64_t start_unix_ns, uint64_t duration_ns,
                                     const string &profile_json) noexcept {
	try {
		auto record = StringUtil::Format(
		    "{\"version\":1,\"start_unix_ns\":%llu,\"duration_ns\":%llu,\"profile\":", start_unix_ns, duration_ns);
		record += profile_json;
		record += "}\n";
		return Write(record.data(), record.size());
	} catch (...) {
		failed = true;
		return false;
	}
}

const char *SamplyQueryWriter::GetPath() const noexcept {
	return path;
}

static SamplyQueryWriter &ThreadQueryWriter() noexcept {
	thread_local SamplyQueryWriter writer;
	return writer;
}

void WriteSamplyQueryProfile(uint64_t start_unix_ns, uint64_t duration_ns, const string &profile_json) noexcept {
	ThreadQueryWriter().WriteProfile(start_unix_ns, duration_ns, profile_json);
}

} // namespace duckdb
