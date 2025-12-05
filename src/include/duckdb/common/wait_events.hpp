#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/common/optional_idx.hpp"

namespace duckdb {

class ClientContext;
struct DBConfig;

enum class WaitEventType : uint16_t {
	INVALID = 0,
	IO_READ,
	IO_WRITE,
	SCHEDULER_IDLE,
	TIMEOUT,
	// Extend with more events as needed
};

struct WaitEventRecord {
	uint64_t connection_id; // 0 if unknown
	optional_idx query_id;  // invalid if unknown
	string query_string;
	uint64_t thread_id; // OS-estimated logical id
	WaitEventType type;
	int64_t duration_us; // microseconds
	string metadata;     // optional metadata (e.g., file path)
};

class WaitEvents {
public:
	// Called once to register settings on DB startup
	static void RegisterSettings(struct DBConfig &config);

	// Update global config based on settings callbacks
	static void SetEnabled(bool enabled);
	static void SetMinDurationUs(uint64_t min_duration_us);

	// Query-time hooks
	static void OnThreadStart(ClientContext &context);
	static void OnQueryStart(ClientContext &context);

	// Recording API (RAII helper below)
	static void Record(WaitEventType type, int64_t start_us, int64_t end_us, const string &metadata = string());

	// Access API for table function
	static vector<WaitEventRecord> CollectForCurrentConnection(ClientContext &context);

	// Utility
	static const char *TypeToString(WaitEventType type);

public:
	static bool IsEnabled();
	static uint64_t NowMicros();
};

// RAII scope to measure waits
class WaitEventScope {
public:
	WaitEventScope(WaitEventType type, const string &metadata = string()) : type(type), metadata(metadata) {
		start_us = WaitEvents::IsEnabled() ? WaitEvents::NowMicros() : 0;
	}
	~WaitEventScope() {
		if (start_us) {
			auto end_us = WaitEvents::NowMicros();
			WaitEvents::Record(type, static_cast<int64_t>(start_us), static_cast<int64_t>(end_us), metadata);
		}
	}

private:
	WaitEventType type;
	string metadata;
	uint64_t start_us; // 0 if disabled
};

} // namespace duckdb
