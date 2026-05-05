//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/types/timestamp.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/limits.hpp"

namespace duckdb {

template<int64_t P, bool Z>
struct timebase_t { // NOLINT
	// NOTE: The unit of value is microseconds for timestamp_t, but it can be
	// different for subclasses (e.g. it's nanos for timestamp_ns, etc).
	static constexpr int64_t PRECISION = P;
	static constexpr bool TIMEZONED = Z;

	int64_t value;

	timebase_t() = default;
	explicit inline constexpr timebase_t(int64_t value) : value(value) {
	}
	inline timebase_t &operator=(int64_t micros) {
		value = micros;
		return *this;
	}

	// explicit conversion
	explicit inline operator int64_t() const {
		return value;
	}

	// comparison operators
	inline bool operator==(const timebase_t &rhs) const {
		return value == rhs.value;
	};
	inline bool operator!=(const timebase_t &rhs) const {
		return value != rhs.value;
	};
	inline bool operator<=(const timebase_t &rhs) const {
		return value <= rhs.value;
	};
	inline bool operator<(const timebase_t &rhs) const {
		return value < rhs.value;
	};
	inline bool operator>(const timebase_t &rhs) const {
		return value > rhs.value;
	};
	inline bool operator>=(const timebase_t &rhs) const {
		return value >= rhs.value;
	};

	// special values
	static constexpr timebase_t infinity() { // NOLINT
		return timebase_t(NumericLimits<int64_t>::Maximum());
	}                                          // NOLINT
	static constexpr timebase_t ninfinity() { // NOLINT
		return timebase_t(-NumericLimits<int64_t>::Maximum());
	}                                             // NOLINT
	static constexpr inline timebase_t epoch() { // NOLINT
		return timebase_t(0);
	} // NOLINT

	//! True, if the timestamp is finite, else false.
	inline bool IsFinite() const {
		return *this != infinity() && *this != ninfinity();
	}

};

//! Type used to represent TIMESTAMP_S. timestamp_sec_t holds the seconds since 1970-01-01.
using timestamp_sec_t = timebase_t<1, false>;

//! Type used to represent TIMESTAMP_MS. timestamp_ms_t holds the milliseconds since 1970-01-01.
using timestamp_ms_t = timebase_t<1000, false>;

//! Type used to represent a TIMESTAMP. timestamp_t holds the microseconds since 1970-01-01.
using timestamp_t = timebase_t<1000000, false>;

//! Type used to represent TIMESTAMP_NS. timestamp_ns_t holds the nanoseconds since 1970-01-01.
using timestamp_ns_t = timebase_t<1000000000, false>;

//! Type used to represent TIMESTAMPTZ. timestamp_tz_t holds the microseconds since 1970-01-01 (UTC).
//! It is physically the same as timestamp_t, both hold microseconds since epoch.
using timestamp_tz_t = timebase_t<1000000, true>;

//! Type used to represent TIMESTAMPTZ_NS. timestamp_tz_ns_t holds the nanooseconds since 1970-01-01 (UTC).
//! It is physically the same as timestamp_ns_t, both hold nanoseconds since epoch.
using timestamp_tz_ns_t = timebase_t<1000000000, true>;

} // namespace duckdb

namespace std {

//! Timestamp
template <>
struct hash<duckdb::timestamp_t> {
	std::size_t operator()(const duckdb::timestamp_t &k) const {
		using std::hash;
		return hash<int64_t>()((int64_t)k);
	}
};
template <>
struct hash<duckdb::timestamp_ms_t> {
	std::size_t operator()(const duckdb::timestamp_ms_t &k) const {
		using std::hash;
		return hash<int64_t>()((int64_t)k);
	}
};
template <>
struct hash<duckdb::timestamp_ns_t> {
	std::size_t operator()(const duckdb::timestamp_ns_t &k) const {
		using std::hash;
		return hash<int64_t>()((int64_t)k);
	}
};
template <>
struct hash<duckdb::timestamp_sec_t> {
	std::size_t operator()(const duckdb::timestamp_sec_t &k) const {
		using std::hash;
		return hash<int64_t>()((int64_t)k);
	}
};
template <>
struct hash<duckdb::timestamp_tz_t> {
	std::size_t operator()(const duckdb::timestamp_tz_t &k) const {
		using std::hash;
		return hash<int64_t>()((int64_t)k);
	}
};
template <>
struct hash<duckdb::timestamp_tz_ns_t> {
	std::size_t operator()(const duckdb::timestamp_tz_ns_t &k) const {
		using std::hash;
		return hash<int64_t>()((int64_t)k);
	}
};

} // namespace std
