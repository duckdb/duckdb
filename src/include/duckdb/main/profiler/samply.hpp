//===----------------------------------------------------------------------===//
//
//                         DuckDB
//
// duckdb/main/profiler/samply.hpp
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/constants.hpp"
#include "duckdb/common/shared_ptr.hpp"

namespace duckdb {

enum class SamplyTrack : uint8_t {
	QUERY = 1,
	MEMORY = 2,
	NETWORK = 4,
	HTTP = 8,
};

static inline bool SamplyTrackEnabled(uint8_t tracks, SamplyTrack track) {
	return (tracks & static_cast<uint8_t>(track)) != 0;
}

class SamplyResourceSubscription;

//! Starts process-wide resource sampling for the requested resource bits.
shared_ptr<SamplyResourceSubscription> StartSamplyResourceSampling(uint8_t tracks) noexcept;

//! Starts an isolated resource sampler without periodic sampling. Public only for unit testing.
shared_ptr<SamplyResourceSubscription> StartSamplyResourceSamplingForTesting(uint8_t tracks,
                                                                             const char *directory) noexcept;

//! Returns whether process-wide HTTP request tracking is currently active.
bool SamplyHTTPTrackEnabled() noexcept;

//! Writes one HTTP request attempt to the sidecar consumed by scripts/samply_profile.py.
void WriteSamplyHTTPAttempt(uint64_t start_unix_ns, uint64_t duration_ns, const char *method, const string &url,
                            int32_t status_code, uint64_t bytes_received, int64_t time_to_first_byte_ns,
                            const string &request_range, const string &response_content_range) noexcept;

//! Writes one completed query profile to the sidecar consumed by scripts/samply_profile.py.
void WriteSamplyQueryProfile(uint64_t start_unix_ns, uint64_t duration_ns, const string &profile_json) noexcept;

class SamplyQueryWriter {
public:
	explicit SamplyQueryWriter(const char *directory = nullptr) noexcept;
	~SamplyQueryWriter();

	SamplyQueryWriter(const SamplyQueryWriter &) = delete;
	SamplyQueryWriter &operator=(const SamplyQueryWriter &) = delete;

	bool WriteProfile(uint64_t start_unix_ns, uint64_t duration_ns, const string &profile_json) noexcept;
	const char *GetPath() const noexcept;

private:
	bool Initialize() noexcept;
	bool Write(const char *data, idx_t size) noexcept;

private:
	const char *directory;
	int file_descriptor;
	bool failed;
	char path[4096];
};

//! Writes the counter sidecar format used by scripts/samply_profile.py. Public only for unit testing.
class SamplyCounterWriter {
public:
	explicit SamplyCounterWriter(const char *directory = nullptr) noexcept;
	~SamplyCounterWriter();

	SamplyCounterWriter(const SamplyCounterWriter &) = delete;
	SamplyCounterWriter &operator=(const SamplyCounterWriter &) = delete;

	bool WriteClock(uint64_t monotonic_ns, uint64_t unix_ns) noexcept;
	bool WriteSample(uint64_t monotonic_ns, const char *name, int64_t value) noexcept;
	bool Flush() noexcept;
	const char *GetPath() const noexcept;

private:
	bool Initialize() noexcept;
	bool Append(const char *data, idx_t size) noexcept;

private:
	const char *directory;
	int file_descriptor;
	bool failed;
	bool wrote_clock;
	string buffer;
	char path[4096];
};

//! Writes the HTTP sidecar format used by scripts/samply_profile.py. Public only for unit testing.
class SamplyHTTPWriter {
public:
	explicit SamplyHTTPWriter(const char *directory = nullptr) noexcept;
	~SamplyHTTPWriter();

	SamplyHTTPWriter(const SamplyHTTPWriter &) = delete;
	SamplyHTTPWriter &operator=(const SamplyHTTPWriter &) = delete;

	bool WriteAttempt(uint64_t start_unix_ns, uint64_t duration_ns, const char *method, const string &url,
	                  int32_t status_code, uint64_t bytes_received, int64_t time_to_first_byte_ns,
	                  const string &request_range, const string &response_content_range) noexcept;
	const char *GetPath() const noexcept;

private:
	bool Initialize() noexcept;
	bool Write(const char *data, idx_t size) noexcept;

private:
	const char *directory;
	int file_descriptor;
	bool failed;
	char path[4096];
};

} // namespace duckdb
