//===----------------------------------------------------------------------===//
//
//                         DuckDB
//
// duckdb/main/profiler/profiling_utils.hpp
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/enums/metric_type.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/array.hpp"
#include "duckdb/common/limits.hpp"
#include "duckdb/common/optional_idx.hpp"
#include "duckdb/common/mutex.hpp"
#include "duckdb/main/profiler/gathered_metrics.hpp"
#include "duckdb/main/profiler/metric_info.hpp"
#include "duckdb/main/profiler/profiling_node.hpp"
#include "duckdb/common/profiler.hpp"
#include "duckdb/common/types/timestamp.hpp"

namespace duckdb_yyjson {
struct yyjson_mut_doc;
struct yyjson_mut_val;
} // namespace duckdb_yyjson

namespace duckdb {

struct MetricsTimer;

//! The timing or counter of a metric with an index, which any thread updates with atomics
struct MetricSlot {
	//! A timing's nanoseconds or a counter's count
	atomic<uint64_t> value {0};
	//! The earliest start and latest end of a timing, in microseconds since the epoch
	atomic<int64_t> start {NumericLimits<int64_t>::Maximum()};
	atomic<int64_t> end {NumericLimits<int64_t>::Minimum()};
	atomic<bool> set {false};
	//! Whether the metric is a "uint64" counter or a "double" timing; set before the slot is published
	bool is_counter = false;
	bool is_timing = false;

	void Reset() {
		value.store(0, std::memory_order_relaxed);
		start.store(NumericLimits<int64_t>::Maximum(), std::memory_order_relaxed);
		end.store(NumericLimits<int64_t>::Minimum(), std::memory_order_relaxed);
		set.store(false, std::memory_order_relaxed);
	}
	bool HasBounds() const {
		return start.load(std::memory_order_relaxed) != NumericLimits<int64_t>::Maximum();
	}
};

//! The slots of consecutive metric indexes, allocated together so that they never move
struct MetricSlotChunk {
	static constexpr idx_t SLOT_COUNT = 64;
	array<MetricSlot, SLOT_COUNT> slots;
};

// Top level query metrics
struct QueryMetrics {
public:
	QueryMetrics();
	~QueryMetrics();

	idx_t system_peak_buffer_memory;
	idx_t system_peak_streaming_buffer_size;
	idx_t system_peak_temp_dir_size;
	double blocked_thread_time;

	std::string query_sql;

	// Always-tracked byte counters (used by progress bar even when profiling is disabled)
	atomic<idx_t> bytes_read;
	atomic<idx_t> read_operations;
	atomic<idx_t> read_time_us;
	atomic<idx_t> bytes_written;
	atomic<idx_t> write_operations;
	atomic<idx_t> write_time_us;
	// Cumulative bytes written to the temporary (spill) directory
	atomic<idx_t> bytes_spilled;
	// Always tracked as well, so it can be read while the query runs: the compressed size of the column data scanned.
	// Not reported - query.total_bytes_scanned is summed over the operator tree, so that secure views hide theirs
	atomic<idx_t> bytes_scanned;
	// Thread-safe memory allocation counter (updated from allocator callbacks on any thread)
	atomic<idx_t> total_memory_allocated;

public:
	//! Add to a timing or counter by name: one of the profiler's own is updated by its index, without a lock
	DUCKDB_API void UpdateMetric(const string &key, idx_t addition);
	DUCKDB_API void UpdateMetric(const string &key, idx_t addition, timestamp_t start_time, timestamp_t end_time);
	DUCKDB_API void UpdateMetricCounter(const string &key, idx_t addition);

	//! Add to the timing or counter of a metric index, without a lock
	DUCKDB_API void AddToMetricTiming(idx_t metric_index, idx_t elapsed_ns);
	DUCKDB_API void AddToMetricTiming(idx_t metric_index, idx_t elapsed_ns, timestamp_t start_time,
	                                  timestamp_t end_time);
	DUCKDB_API void AddToMetricCounter(idx_t metric_index, idx_t addition);

	void SetMetricValue(const string &key, Value value) {
		annotated_lock_guard<annotated_mutex> guard(string_metrics_lock);
		string_values[key] = std::move(value);
	}

	void UpdateBytesRead(idx_t n, idx_t elapsed_us) {
		bytes_read += n;
		read_operations++;
		read_time_us += elapsed_us;
	}

	void UpdateBytesWritten(idx_t n, idx_t elapsed_us) {
		bytes_written += n;
		write_operations++;
		write_time_us += elapsed_us;
	}

	void UpdateBytesSpilled(idx_t n) {
		bytes_spilled += n;
	}
	void UpdateBytesScanned(idx_t n) {
		bytes_scanned += n;
	}

	void UpdateTotalMemoryAllocated(idx_t n) {
		total_memory_allocated += n;
	}

	//! A timing by name, in seconds, or 0 when the query has not timed it
	DUCKDB_API double GetStringMetricInSeconds(const string &key) const;
	//! A counter by name, or 0 when the query has not counted it
	DUCKDB_API idx_t GetStringCounter(const string &key) const;

	idx_t GetBytesRead() const {
		return bytes_read.load();
	}

	idx_t GetReadOperations() const {
		return read_operations.load();
	}

	double GetReadTime() const {
		return static_cast<double>(read_time_us.load()) / 1e6;
	}

	idx_t GetBytesWritten() const {
		return bytes_written.load();
	}
	idx_t GetBytesScanned() const {
		return bytes_scanned.load();
	}

	idx_t GetWriteOperations() const {
		return write_operations.load();
	}

	double GetWriteTime() const {
		return static_cast<double>(write_time_us.load()) / 1e6;
	}

	idx_t GetBytesSpilled() const {
		return bytes_spilled.load();
	}

	idx_t GetTotalMemoryAllocated() const {
		return total_memory_allocated.load();
	}

	//! A copy, as timers and counters may still be updated from other threads
	DUCKDB_API unordered_map<string, MetricTiming> GetMetricTimings() const;
	DUCKDB_API unordered_map<string, idx_t> GetMetricCounters() const;

	//! Reset the counters that are tracked without profiling; Reset keeps them, so they outlive their query.
	void ResetLiveMetrics() {
		bytes_read = 0;
		read_operations = 0;
		read_time_us = 0;
		bytes_written = 0;
		write_operations = 0;
		write_time_us = 0;
		bytes_spilled = 0;
		// bytes_scanned is not reset here, so it can be read after a failed query; StartQuery resets it
		total_memory_allocated = 0;
	}

	void Reset() {
		// ended first, as the timer adds its time to the metrics reset below
		latency_timer.reset();
		{
			annotated_lock_guard<annotated_mutex> guard(string_metrics_lock);
			string_timings.clear();
			string_counters.clear();
			string_values.clear();
		}
		// a late update from another thread may land in the next query
		ResetMetricSlots();

		query_sql = "";
		system_peak_buffer_memory = 0;
		system_peak_streaming_buffer_size = 0;
		system_peak_temp_dir_size = 0;
		blocked_thread_time = 0;
	}

	//! Write the values set with QueryProfiler::SetMetric into the given GatheredMetrics, before the metrics the
	//! profiler computes itself, which take precedence.
	void FinalizeMetricValues(GatheredMetrics &info);
	//! Write all query-level metrics into the given GatheredMetrics.
	void FinalizeMetrics(GatheredMetrics &info);
	//! The metrics backed by atomic counters, which can be read from any thread while the query runs
	profiler_metrics_t GetLiveMetrics() const;

	void Merge(const QueryMetrics &other) {
		unordered_map<string, MetricTiming> other_timings;
		unordered_map<string, idx_t> other_counters;
		{
			annotated_lock_guard<annotated_mutex> other_guard(other.string_metrics_lock);
			other_timings = other.string_timings;
			other_counters = other.string_counters;
		}
		{
			annotated_lock_guard<annotated_mutex> guard(string_metrics_lock);
			for (const auto &entry : other_timings) {
				string_timings[entry.first].Merge(entry.second);
			}
			for (const auto &entry : other_counters) {
				string_counters[entry.first] += entry.second;
			}
		}
		MergeMetricSlots(other);
		bytes_read += other.bytes_read.load();
		read_operations += other.read_operations.load();
		read_time_us += other.read_time_us.load();
		bytes_written += other.bytes_written.load();
		write_operations += other.write_operations.load();
		write_time_us += other.write_time_us.load();
		bytes_spilled += other.bytes_spilled.load();
		bytes_scanned += other.bytes_scanned.load();
		total_memory_allocated += other.total_memory_allocated.load();
	}

private:
	static constexpr idx_t MAX_SLOT_CHUNKS = MAX_METRIC_COUNT / MetricSlotChunk::SLOT_COUNT;

	//! The index of `key` when it is one of the profiler's own timings (or counters)
	static optional_idx TryGetInternalMetricIndex(const string &key, bool timing);
	//! The slot of a known metric index, or nullptr for an index the process does not know
	optional_ptr<MetricSlot> GetSlot(idx_t metric_index);
	optional_ptr<MetricSlot> GrowSlots(idx_t metric_index);
	optional_ptr<const MetricSlot> TryGetSlot(idx_t metric_index) const;
	//! Calls `callback(name, slot)` for every slot the query updated
	template <class CALLBACK>
	void ForEachSetSlot(CALLBACK &&callback) const;
	DUCKDB_API void ResetMetricSlots();
	DUCKDB_API void MergeMetricSlots(const QueryMetrics &other);

	// Chunks below slot_limit never move and live as long as the metrics, so updates need no lock
	array<unique_ptr<MetricSlotChunk>, MAX_SLOT_CHUNKS> slot_chunks;
	atomic<idx_t> slot_limit {0};
	mutex slot_chunks_lock;
	// Guards the string-keyed metrics below, which extensions update from any thread
	mutable annotated_mutex string_metrics_lock;
	unordered_map<string, MetricTiming> string_timings DUCKDB_GUARDED_BY(string_metrics_lock);
	unordered_map<string, idx_t> string_counters DUCKDB_GUARDED_BY(string_metrics_lock);
	// Values set by QueryProfiler::SetMetric, kept until the query's metrics are gathered
	profiler_metrics_t string_values DUCKDB_GUARDED_BY(string_metrics_lock);

public:
	// Declared last so it is destroyed first, as its destructor adds its time to the metrics above
	unique_ptr<MetricsTimer> latency_timer;
};

struct MetricsTimer {
public:
	MetricsTimer() : metric_name(""), is_active(false) {
	}
	MetricsTimer(QueryMetrics &query_metrics, string key, const bool is_active = true)
	    : query_metrics(query_metrics), metric_name(std::move(key)), is_active(is_active) {
		if (!is_active) {
			return;
		}
		start_time = Timestamp::GetCurrentTimestamp();
		profiler.Start();
	}
	//! A timer of a metric index
	MetricsTimer(QueryMetrics &query_metrics, idx_t metric_index, const bool is_active = true)
	    : query_metrics(query_metrics), metric_index(metric_index), is_active(is_active) {
		if (!is_active) {
			return;
		}
		start_time = Timestamp::GetCurrentTimestamp();
		profiler.Start();
	}
	~MetricsTimer() {
		if (is_active && !Exception::UncaughtException()) {
			EndTimer();
		}
	}
	// disable copy constructors
	MetricsTimer(const MetricsTimer &other) = delete;
	MetricsTimer &operator=(const MetricsTimer &) = delete;
	//! enable move constructors
	MetricsTimer(MetricsTimer &&other) noexcept : is_active(false) {
		std::swap(query_metrics, other.query_metrics);
		std::swap(metric_name, other.metric_name);
		std::swap(metric_index, other.metric_index);
		std::swap(profiler, other.profiler);
		std::swap(start_time, other.start_time);
		std::swap(is_active, other.is_active);
	}
	MetricsTimer &operator=(MetricsTimer &&other) noexcept {
		std::swap(query_metrics, other.query_metrics);
		std::swap(metric_name, other.metric_name);
		std::swap(metric_index, other.metric_index);
		std::swap(profiler, other.profiler);
		std::swap(start_time, other.start_time);
		std::swap(is_active, other.is_active);
		return *this;
	}

	// Automatically called in the destructor.
	void EndTimer() {
		if (!is_active) {
			return;
		}
		is_active = false;
		profiler.End();
		auto end_time = Timestamp::GetCurrentTimestamp();
		if (metric_index.IsValid()) {
			query_metrics->AddToMetricTiming(metric_index.GetIndex(), profiler.ElapsedNanos(), start_time, end_time);
		} else {
			query_metrics->UpdateMetric(metric_name, profiler.ElapsedNanos(), start_time, end_time);
		}
	}

	void Reset() {
		if (!is_active) {
			return;
		}
		profiler.Reset();
		is_active = false;
	}

private:
	optional_ptr<QueryMetrics> query_metrics;
	string metric_name;
	//! Valid for a timer of a metric index, which then has no metric_name
	optional_idx metric_index;
	Profiler profiler;
	timestamp_t start_time = timestamp_t::epoch();
	bool is_active;
};

} // namespace duckdb
