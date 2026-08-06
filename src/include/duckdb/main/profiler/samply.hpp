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

//! Creates a single-line query label that fits in a Samply marker record.
string SamplyQueryMarkerName(const string &query);

//! Writes the marker-file format consumed by Samply. This is public only for unit testing.
class SamplyMarkerWriter {
public:
	explicit SamplyMarkerWriter(const char *directory = nullptr) noexcept;
	~SamplyMarkerWriter();

	SamplyMarkerWriter(const SamplyMarkerWriter &) = delete;
	SamplyMarkerWriter &operator=(const SamplyMarkerWriter &) = delete;

	bool WriteMarker(uint64_t start_ns, uint64_t end_ns, const char *name) noexcept;
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

//! A movable timer whose completed span can be written as a named marker.
class SamplyMarker {
public:
	SamplyMarker() noexcept;
	explicit SamplyMarker(bool enabled) noexcept;
	SamplyMarker(SamplyMarker &&other) noexcept;
	SamplyMarker &operator=(SamplyMarker &&other) noexcept;

	SamplyMarker(const SamplyMarker &) = delete;
	SamplyMarker &operator=(const SamplyMarker &) = delete;

	void End(const char *name) noexcept;
	void Reset() noexcept;

private:
	uint64_t start_ns;
	bool active;
};

class SamplyMarkerScope {
public:
	explicit SamplyMarkerScope(const char *name_p) noexcept : marker(name_p && name_p[0] != '\0'), name(name_p) {
	}
	~SamplyMarkerScope() {
		marker.End(name);
	}

	SamplyMarkerScope(const SamplyMarkerScope &) = delete;
	SamplyMarkerScope &operator=(const SamplyMarkerScope &) = delete;

private:
	SamplyMarker marker;
	const char *name;
};

} // namespace duckdb
