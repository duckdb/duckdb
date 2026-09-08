#include "duckdb/common/time_point.hpp"
#include "duckdb/common/assert.hpp"

namespace duckdb {

TimePoint::TimePoint() : value() {
}

TimePoint TimePoint::Tick() {
	return TimePoint(steady_clock::now());
}

int64_t TimePoint::GetTickMs() {
	return std::chrono::duration_cast<std::chrono::milliseconds>(steady_clock::now().time_since_epoch()).count();
}

double TimePoint::ElapsedSeconds(const TimePoint &start, const TimePoint &end) {
	D_ASSERT(start.value <= end.value);
	return std::chrono::duration_cast<std::chrono::duration<double>>(end.value - start.value).count();
}

int64_t TimePoint::ElapsedMillis(const TimePoint &start, const TimePoint &end) {
	D_ASSERT(start.value <= end.value);
	return std::chrono::duration_cast<std::chrono::milliseconds>(end.value - start.value).count();
}

int64_t TimePoint::ElapsedMicros(const TimePoint &start, const TimePoint &end) {
	D_ASSERT(start.value <= end.value);
	return std::chrono::duration_cast<std::chrono::microseconds>(end.value - start.value).count();
}

int64_t TimePoint::ElapsedNanos(const TimePoint &start, const TimePoint &end) {
	D_ASSERT(start.value <= end.value);
	return std::chrono::duration_cast<std::chrono::nanoseconds>(end.value - start.value).count();
}

double TimePoint::ElapsedSeconds() const {
	auto now = steady_clock::now();
	D_ASSERT(value <= now);
	return std::chrono::duration_cast<std::chrono::duration<double>>(now - value).count();
}

int64_t TimePoint::ElapsedMillis() const {
	auto now = steady_clock::now();
	D_ASSERT(value <= now);
	return std::chrono::duration_cast<std::chrono::milliseconds>(now - value).count();
}

int64_t TimePoint::ElapsedMicros() const {
	auto now = steady_clock::now();
	D_ASSERT(value <= now);
	return std::chrono::duration_cast<std::chrono::microseconds>(now - value).count();
}

int64_t TimePoint::ElapsedNanos() const {
	auto now = steady_clock::now();
	D_ASSERT(value <= now);
	return std::chrono::duration_cast<std::chrono::nanoseconds>(now - value).count();
}

TimePoint::TimePoint(time_point<steady_clock> value_p) : value(value_p) {
}

} // namespace duckdb
