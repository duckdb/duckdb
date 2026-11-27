#include "duckdb/main/profiler/profiling_utils.hpp"
#include "duckdb/common/bit_utils.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/main/profiler/metrics_manager.hpp"

namespace duckdb {

void QueryMetrics::SetMetricsManager(const MetricsManager &manager) {
	metrics_manager = &manager;
}

MetricSlot &QueryMetrics::SlotAt(idx_t metric_index) const {
	// segment s starts at index FIRST_SEGMENT_SIZE * (2^s - 1)
	auto position = metric_index + FIRST_SEGMENT_SIZE;
	auto high_bit = 63 - CountZeros<uint64_t>::Leading(position);
	auto segment = high_bit - CountZeros<uint64_t>::Trailing(FIRST_SEGMENT_SIZE);
	return slot_segments[segment][position - (idx_t(1) << high_bit)];
}

optional_ptr<MetricSlot> QueryMetrics::GetSlot(idx_t metric_index) {
	if (metric_index < slot_limit.load(std::memory_order_acquire)) {
		return &SlotAt(metric_index);
	}
	return GrowSlots(metric_index);
}

optional_ptr<MetricSlot> QueryMetrics::GrowSlots(idx_t metric_index) {
	auto known_count = metrics_manager ? metrics_manager->GetMetricCount() : MetricsManager::InternalMetricCount();
	if (metric_index >= known_count) {
		// an index the database has not handed out
		return nullptr;
	}
	lock_guard<mutex> guard(slot_segments_lock);
	auto limit = slot_limit.load(std::memory_order_relaxed);
	if (metric_index < limit) {
		return &SlotAt(metric_index);
	}
	for (idx_t segment = 0, start = 0; start < known_count; segment++) {
		auto size = FIRST_SEGMENT_SIZE << segment;
		if (!slot_segments[segment]) {
			slot_segments[segment] = make_uniq_array<MetricSlot>(size);
		}
		start += size;
	}
	auto kinds = metrics_manager ? metrics_manager->GetMetricKinds(limit, known_count)
	                             : MetricsManager::GetInternalMetricKinds(limit, known_count);
	for (idx_t i = limit; i < known_count; i++) {
		SlotAt(i).kind = kinds[i - limit];
	}
	slot_limit.store(known_count, std::memory_order_release);
	return &SlotAt(metric_index);
}

optional_ptr<const MetricSlot> QueryMetrics::TryGetSlot(idx_t metric_index) const {
	if (metric_index >= slot_limit.load(std::memory_order_acquire)) {
		return nullptr;
	}
	return &SlotAt(metric_index);
}

optional_idx QueryMetrics::TryGetMetricIndex(const string &key) const {
	return metrics_manager ? metrics_manager->TryGetMetricIndex(key) : MetricsManager::TryGetInternalMetricIndex(key);
}

string QueryMetrics::GetMetricName(idx_t metric_index) const {
	if (metric_index < MetricsManager::InternalMetricCount()) {
		return MetricsManager::GetInternalMetric(metric_index).name;
	}
	D_ASSERT(metrics_manager);
	return metrics_manager->GetMetric(metric_index).name;
}

template <class FUNC>
void QueryMetrics::ForEachSetSlot(FUNC &&callback) const {
	auto limit = slot_limit.load(std::memory_order_acquire);
	for (idx_t i = 0; i < limit; i++) {
		auto &slot = SlotAt(i);
		if (slot.set.load(std::memory_order_relaxed)) {
			callback(GetMetricName(i), slot);
		}
	}
}

void QueryMetrics::ResetMetricSlots() {
	auto limit = slot_limit.load(std::memory_order_acquire);
	for (idx_t i = 0; i < limit; i++) {
		SlotAt(i).Reset();
	}
}

optional_ptr<MetricSlot> QueryMetrics::GetSlotByName(const string &key, MetricSlotKind kind) {
	auto index = TryGetMetricIndex(key);
	if (!index.IsValid()) {
		return nullptr;
	}
	auto slot = GetSlot(index.GetIndex());
	return slot && slot->kind == kind ? slot : nullptr;
}

void MetricSlot::Add(idx_t amount) {
	value.fetch_add(amount, std::memory_order_relaxed);
	set.store(true, std::memory_order_relaxed);
}

void MetricSlot::AddTiming(idx_t elapsed_ns, timestamp_t start_time, timestamp_t end_time) {
	value.fetch_add(elapsed_ns, std::memory_order_relaxed);
	auto start_value = start_time.value;
	auto current_start = start.load(std::memory_order_relaxed);
	while (start_value < current_start &&
	       !start.compare_exchange_weak(current_start, start_value, std::memory_order_relaxed,
	                                    std::memory_order_relaxed)) {
	}
	auto end_value = end_time.value;
	auto current_end = end.load(std::memory_order_relaxed);
	while (end_value > current_end &&
	       !end.compare_exchange_weak(current_end, end_value, std::memory_order_relaxed, std::memory_order_relaxed)) {
	}
	set.store(true, std::memory_order_relaxed);
}

void QueryMetrics::AddToMetricTiming(idx_t metric_index, idx_t elapsed_ns) {
	auto slot = GetSlot(metric_index);
	if (slot && slot->kind == MetricSlotKind::TIMING) {
		slot->Add(elapsed_ns);
	}
}

void QueryMetrics::AddToMetricTiming(idx_t metric_index, idx_t elapsed_ns, timestamp_t start_time,
                                     timestamp_t end_time) {
	auto slot = GetSlot(metric_index);
	if (slot && slot->kind == MetricSlotKind::TIMING) {
		slot->AddTiming(elapsed_ns, start_time, end_time);
	}
}

void QueryMetrics::AddToMetricCounter(idx_t metric_index, idx_t addition) {
	auto slot = GetSlot(metric_index);
	if (slot && slot->kind == MetricSlotKind::COUNTER) {
		slot->Add(addition);
	}
}

void QueryMetrics::UpdateMetric(const string &key, idx_t addition) {
	auto slot = GetSlotByName(key, MetricSlotKind::TIMING);
	if (slot) {
		slot->Add(addition);
		return;
	}
	annotated_lock_guard<annotated_mutex> guard(string_metrics_lock);
	string_timings[key].Add(addition);
}

void QueryMetrics::UpdateMetric(const string &key, idx_t addition, timestamp_t start_time, timestamp_t end_time) {
	auto slot = GetSlotByName(key, MetricSlotKind::TIMING);
	if (slot) {
		slot->AddTiming(addition, start_time, end_time);
		return;
	}
	annotated_lock_guard<annotated_mutex> guard(string_metrics_lock);
	string_timings[key].Add(addition, start_time, end_time);
}

void QueryMetrics::UpdateMetricCounter(const string &key, idx_t addition) {
	auto slot = GetSlotByName(key, MetricSlotKind::COUNTER);
	if (slot) {
		slot->Add(addition);
		return;
	}
	annotated_lock_guard<annotated_mutex> guard(string_metrics_lock);
	string_counters[key] += addition;
}

double QueryMetrics::GetStringMetricInSeconds(const string &key) const {
	idx_t duration = 0;
	auto index = TryGetMetricIndex(key);
	auto slot = index.IsValid() ? TryGetSlot(index.GetIndex()) : nullptr;
	if (slot && slot->kind == MetricSlotKind::TIMING) {
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
	auto index = TryGetMetricIndex(key);
	auto slot = index.IsValid() ? TryGetSlot(index.GetIndex()) : nullptr;
	if (slot && slot->kind == MetricSlotKind::COUNTER) {
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
		if (slot.kind == MetricSlotKind::TIMING) {
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
		if (slot.kind == MetricSlotKind::COUNTER) {
			result[name] += slot.value.load(std::memory_order_relaxed);
		}
	});
	return result;
}

void QueryMetrics::MergeMetricSlots(const QueryMetrics &other) {
	auto limit = other.slot_limit.load(std::memory_order_acquire);
	for (idx_t i = 0; i < limit; i++) {
		auto &slot = other.SlotAt(i);
		if (!slot.set.load(std::memory_order_relaxed)) {
			continue;
		}
		auto value = slot.value.load(std::memory_order_relaxed);
		if (slot.HasBounds()) {
			AddToMetricTiming(i, value, timestamp_t(slot.start.load(std::memory_order_relaxed)),
			                  timestamp_t(slot.end.load(std::memory_order_relaxed)));
		} else if (slot.kind == MetricSlotKind::TIMING) {
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
			auto &slot = SlotAt(i);
			if (!slot.set.load(std::memory_order_relaxed)) {
				continue;
			}
			auto name = GetMetricName(i);
			auto value = slot.value.load(std::memory_order_relaxed);
			if (slot.kind == MetricSlotKind::TIMING) {
				auto entry = string_timings.find(name);
				value += entry == string_timings.end() ? 0 : entry->second.duration;
				info.SetMetric(name, static_cast<double>(value) / 1e9);
			} else if (slot.kind == MetricSlotKind::COUNTER) {
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
