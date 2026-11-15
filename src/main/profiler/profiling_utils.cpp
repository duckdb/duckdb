#include "duckdb/main/profiler/profiling_utils.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/main/profiler/metrics_manager.hpp"

namespace duckdb {

optional_ptr<MetricSlot> QueryMetrics::GetSlot(idx_t metric_index) {
	if (metric_index < slot_limit.load(std::memory_order_acquire)) {
		return &slot_chunks[metric_index / MetricSlotChunk::SLOT_COUNT]
		            ->slots[metric_index % MetricSlotChunk::SLOT_COUNT];
	}
	return GrowSlots(metric_index);
}

optional_ptr<MetricSlot> QueryMetrics::GrowSlots(idx_t metric_index) {
	auto known_count = MetricsManager::KnownMetricCount();
	if (metric_index >= known_count) {
		// an index no database of the process has handed out
		return nullptr;
	}
	lock_guard<mutex> guard(slot_chunks_lock);
	auto limit = slot_limit.load(std::memory_order_relaxed);
	for (idx_t i = limit; i < known_count; i++) {
		auto chunk_index = i / MetricSlotChunk::SLOT_COUNT;
		if (!slot_chunks[chunk_index]) {
			slot_chunks[chunk_index] = make_uniq<MetricSlotChunk>();
		}
		auto &slot = slot_chunks[chunk_index]->slots[i % MetricSlotChunk::SLOT_COUNT];
		auto metric_type = MetricsManager::GetMetric(i).metric_type;
		slot.is_counter = metric_type == "uint64";
		slot.is_timing = metric_type == "double";
	}
	if (known_count > limit) {
		slot_limit.store(known_count, std::memory_order_release);
	}
	return &slot_chunks[metric_index / MetricSlotChunk::SLOT_COUNT]->slots[metric_index % MetricSlotChunk::SLOT_COUNT];
}

optional_ptr<const MetricSlot> QueryMetrics::TryGetSlot(idx_t metric_index) const {
	if (metric_index >= slot_limit.load(std::memory_order_acquire)) {
		return nullptr;
	}
	return &slot_chunks[metric_index / MetricSlotChunk::SLOT_COUNT]->slots[metric_index % MetricSlotChunk::SLOT_COUNT];
}

template <class CALLBACK>
void QueryMetrics::ForEachSetSlot(CALLBACK &&callback) const {
	auto limit = slot_limit.load(std::memory_order_acquire);
	for (idx_t i = 0; i < limit; i++) {
		auto &slot = slot_chunks[i / MetricSlotChunk::SLOT_COUNT]->slots[i % MetricSlotChunk::SLOT_COUNT];
		if (!slot.set.load(std::memory_order_relaxed)) {
			continue;
		}
		if (i < MetricsManager::InternalMetricCount()) {
			callback(MetricsManager::GetInternalMetric(i).name, slot);
		} else {
			callback(MetricsManager::GetMetric(i).name, slot);
		}
	}
}

void QueryMetrics::ResetMetricSlots() {
	auto limit = slot_limit.load(std::memory_order_acquire);
	for (idx_t i = 0; i < limit; i++) {
		slot_chunks[i / MetricSlotChunk::SLOT_COUNT]->slots[i % MetricSlotChunk::SLOT_COUNT].Reset();
	}
}

optional_idx QueryMetrics::TryGetInternalMetricIndex(const string &key, bool timing) {
	auto index = MetricsManager::TryGetInternalMetricIndex(key);
	if (!index.IsValid()) {
		return index;
	}
	auto is_double = MetricsManager::GetInternalMetric(index.GetIndex()).metric_type == "double";
	return is_double == timing ? index : optional_idx();
}

void QueryMetrics::AddToMetricTiming(idx_t metric_index, idx_t elapsed_ns) {
	auto slot = GetSlot(metric_index);
	if (!slot || !slot->is_timing) {
		return;
	}
	slot->value.fetch_add(elapsed_ns, std::memory_order_relaxed);
	slot->set.store(true, std::memory_order_relaxed);
}

void QueryMetrics::AddToMetricTiming(idx_t metric_index, idx_t elapsed_ns, timestamp_t start_time,
                                     timestamp_t end_time) {
	auto slot = GetSlot(metric_index);
	if (!slot || !slot->is_timing) {
		return;
	}
	slot->value.fetch_add(elapsed_ns, std::memory_order_relaxed);
	auto start = start_time.value;
	auto current_start = slot->start.load(std::memory_order_relaxed);
	while (start < current_start && !slot->start.compare_exchange_weak(current_start, start, std::memory_order_relaxed,
	                                                                   std::memory_order_relaxed)) {
	}
	auto end = end_time.value;
	auto current_end = slot->end.load(std::memory_order_relaxed);
	while (end > current_end &&
	       !slot->end.compare_exchange_weak(current_end, end, std::memory_order_relaxed, std::memory_order_relaxed)) {
	}
	slot->set.store(true, std::memory_order_relaxed);
}

void QueryMetrics::AddToMetricCounter(idx_t metric_index, idx_t addition) {
	auto slot = GetSlot(metric_index);
	if (!slot || !slot->is_counter) {
		return;
	}
	slot->value.fetch_add(addition, std::memory_order_relaxed);
	slot->set.store(true, std::memory_order_relaxed);
}

void QueryMetrics::UpdateMetric(const string &key, idx_t addition) {
	auto index = TryGetInternalMetricIndex(key, true);
	if (index.IsValid()) {
		AddToMetricTiming(index.GetIndex(), addition);
		return;
	}
	annotated_lock_guard<annotated_mutex> guard(string_metrics_lock);
	string_timings[key].Add(addition);
}

void QueryMetrics::UpdateMetric(const string &key, idx_t addition, timestamp_t start_time, timestamp_t end_time) {
	auto index = TryGetInternalMetricIndex(key, true);
	if (index.IsValid()) {
		AddToMetricTiming(index.GetIndex(), addition, start_time, end_time);
		return;
	}
	annotated_lock_guard<annotated_mutex> guard(string_metrics_lock);
	string_timings[key].Add(addition, start_time, end_time);
}

void QueryMetrics::UpdateMetricCounter(const string &key, idx_t addition) {
	auto index = TryGetInternalMetricIndex(key, false);
	if (index.IsValid()) {
		AddToMetricCounter(index.GetIndex(), addition);
		return;
	}
	annotated_lock_guard<annotated_mutex> guard(string_metrics_lock);
	string_counters[key] += addition;
}

double QueryMetrics::GetStringMetricInSeconds(const string &key) const {
	idx_t duration = 0;
	auto index = MetricsManager::TryGetMetricIndex(key);
	auto slot = index.IsValid() ? TryGetSlot(index.GetIndex()) : nullptr;
	if (slot && slot->is_timing) {
		duration += slot->value.load(std::memory_order_relaxed);
	}
	annotated_lock_guard<annotated_mutex> guard(string_metrics_lock);
	auto it = string_timings.find(key);
	if (it != string_timings.end()) {
		duration += it->second.duration;
	}
	return static_cast<double>(duration) / 1e9;
}

idx_t QueryMetrics::GetStringCounter(const string &key) const {
	idx_t count = 0;
	auto index = MetricsManager::TryGetMetricIndex(key);
	auto slot = index.IsValid() ? TryGetSlot(index.GetIndex()) : nullptr;
	if (slot && slot->is_counter) {
		count += slot->value.load(std::memory_order_relaxed);
	}
	annotated_lock_guard<annotated_mutex> guard(string_metrics_lock);
	auto it = string_counters.find(key);
	if (it != string_counters.end()) {
		count += it->second;
	}
	return count;
}

// A metric updated both by name and by index adds up, here and when the metrics are gathered
static void AddSlotTiming(MetricTiming &timing, const MetricSlot &slot) {
	auto duration = slot.value.load(std::memory_order_relaxed);
	if (slot.HasBounds()) {
		timing.Add(duration, timestamp_t(slot.start.load(std::memory_order_relaxed)),
		           timestamp_t(slot.end.load(std::memory_order_relaxed)));
	} else {
		timing.Add(duration);
	}
}

unordered_map<string, MetricTiming> QueryMetrics::GetMetricTimings() const {
	unordered_map<string, MetricTiming> result;
	{
		annotated_lock_guard<annotated_mutex> guard(string_metrics_lock);
		result = string_timings;
	}
	ForEachSetSlot([&](const string &name, const MetricSlot &slot) {
		if (slot.is_timing) {
			AddSlotTiming(result[name], slot);
		}
	});
	return result;
}

unordered_map<string, idx_t> QueryMetrics::GetMetricCounters() const {
	unordered_map<string, idx_t> result;
	{
		annotated_lock_guard<annotated_mutex> guard(string_metrics_lock);
		result = string_counters;
	}
	ForEachSetSlot([&](const string &name, const MetricSlot &slot) {
		if (slot.is_counter) {
			result[name] += slot.value.load(std::memory_order_relaxed);
		}
	});
	return result;
}

void QueryMetrics::MergeMetricSlots(const QueryMetrics &other) {
	auto limit = other.slot_limit.load(std::memory_order_acquire);
	for (idx_t i = 0; i < limit; i++) {
		auto &slot = other.slot_chunks[i / MetricSlotChunk::SLOT_COUNT]->slots[i % MetricSlotChunk::SLOT_COUNT];
		if (!slot.set.load(std::memory_order_relaxed)) {
			continue;
		}
		auto value = slot.value.load(std::memory_order_relaxed);
		if (slot.HasBounds()) {
			AddToMetricTiming(i, value, timestamp_t(slot.start.load(std::memory_order_relaxed)),
			                  timestamp_t(slot.end.load(std::memory_order_relaxed)));
		} else if (slot.is_timing) {
			AddToMetricTiming(i, value);
		} else {
			AddToMetricCounter(i, value);
		}
	}
}

void QueryMetrics::FinalizeMetricValues(GatheredMetrics &info) {
	annotated_lock_guard<annotated_mutex> guard(string_metrics_lock);
	for (const auto &[key, value] : string_values) {
		info.SetMetric(key, value);
	}
}

void QueryMetrics::FinalizeMetrics(GatheredMetrics &info) {
	info.SetMetric<MetricQuerySQL>(query_sql);
	{
		annotated_lock_guard<annotated_mutex> guard(string_metrics_lock);
		for (const auto &[key, timing] : string_timings) {
			info.SetMetric(key, static_cast<double>(timing.duration) / 1e9);
		}
		for (const auto &[key, count] : string_counters) {
			info.SetMetric(key, count);
		}
		// a metric updated both by name and by index adds up
		auto limit = slot_limit.load(std::memory_order_acquire);
		for (idx_t i = 0; i < limit; i++) {
			auto &slot = slot_chunks[i / MetricSlotChunk::SLOT_COUNT]->slots[i % MetricSlotChunk::SLOT_COUNT];
			if (!slot.set.load(std::memory_order_relaxed)) {
				continue;
			}
			auto name = i < MetricsManager::InternalMetricCount() ? MetricsManager::GetInternalMetric(i).name
			                                                      : MetricsManager::GetMetric(i).name;
			auto value = slot.value.load(std::memory_order_relaxed);
			if (slot.is_timing) {
				auto entry = string_timings.find(name);
				value += entry == string_timings.end() ? 0 : entry->second.duration;
				info.SetMetric(name, static_cast<double>(value) / 1e9);
			} else if (slot.is_counter) {
				auto entry = string_counters.find(name);
				value += entry == string_counters.end() ? 0 : entry->second;
				info.SetMetric(name, value);
			}
		}
	}
	for (auto &entry : GetLiveMetrics()) {
		info.SetMetric(entry.first, std::move(entry.second));
	}
	info.SetMetric<MetricSystemBlockedThreadTime>(blocked_thread_time);
	info.SetMetric<MetricSystemPeakBufferMemory>(system_peak_buffer_memory);
	info.SetMetric<MetricSystemPeakStreamingBufferSize>(system_peak_streaming_buffer_size);
	info.SetMetric<MetricSystemPeakTempDirSize>(system_peak_temp_dir_size);
}

template <class METRIC>
static typename METRIC::METRIC_TYPE ReadLiveMetric(const QueryMetrics &metrics) {
	static_assert(sizeof(METRIC) == 0, "a metric marked \"live\" in metrics.json has no counter to read while the "
	                                   "query runs: add one here");
	return {};
}

template <>
uint64_t ReadLiveMetric<MetricIOTotalBytesRead>(const QueryMetrics &metrics) {
	return metrics.GetBytesRead();
}

template <>
uint64_t ReadLiveMetric<MetricIOTotalBytesWritten>(const QueryMetrics &metrics) {
	return metrics.GetBytesWritten();
}

template <>
uint64_t ReadLiveMetric<MetricSystemTotalMemoryAllocated>(const QueryMetrics &metrics) {
	return metrics.GetTotalMemoryAllocated();
}

template <>
uint64_t ReadLiveMetric<MetricIOTotalReadOperations>(const QueryMetrics &metrics) {
	return metrics.GetReadOperations();
}

template <>
double ReadLiveMetric<MetricIOTotalReadTime>(const QueryMetrics &metrics) {
	return metrics.GetReadTime();
}

template <>
uint64_t ReadLiveMetric<MetricIOTotalWriteOperations>(const QueryMetrics &metrics) {
	return metrics.GetWriteOperations();
}

template <>
double ReadLiveMetric<MetricIOTotalWriteTime>(const QueryMetrics &metrics) {
	return metrics.GetWriteTime();
}

template <>
uint64_t ReadLiveMetric<MetricSystemTotalBytesSpilled>(const QueryMetrics &metrics) {
	return metrics.GetBytesSpilled();
}

template <class... METRICS>
static profiler_metrics_t ReadLiveMetrics(const QueryMetrics &metrics, MetricList<METRICS...>) {
	profiler_metrics_t result;
	((result[METRICS::Name] = Value::CreateValue(ReadLiveMetric<METRICS>(metrics))), ...);
	return result;
}

profiler_metrics_t QueryMetrics::GetLiveMetrics() const {
	return ReadLiveMetrics(*this, LiveMetrics());
}

QueryMetrics::QueryMetrics()
    : bytes_read(0), read_operations(0), read_time_us(0), bytes_written(0), write_operations(0), write_time_us(0),
      bytes_spilled(0), bytes_scanned(0), total_memory_allocated(0) {
	// the slots of the metrics known so far are there from the start
	GrowSlots(0);
	Reset();
}

QueryMetrics::~QueryMetrics() = default;

} // namespace duckdb
