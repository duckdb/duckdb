//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/types/datetime.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/numeric_utils.hpp"

#include <functional>

namespace duckdb {

template <int64_t P>
struct dtime_base_t { // NOLINT
	// NOTE: The unit of value is microseconds for timestamp_t, but it can be
	// different for subclasses (e.g. it's nanos for timestamp_ns, etc).
	static constexpr int64_t PRECISION = P;

	int64_t value;

	dtime_base_t() = default;
	explicit inline constexpr dtime_base_t(int64_t value) : value(value) {
	}
	inline dtime_base_t &operator=(int64_t value) {
		this->value = value;
		return *this;
	}

	// explicit conversion
	explicit inline operator int64_t() const {
		return value;
	}
	explicit inline operator double() const {
		return static_cast<double>(value);
	}

	// comparison operators
	inline bool operator==(const dtime_base_t &rhs) const {
		return value == rhs.value;
	};
	inline bool operator!=(const dtime_base_t &rhs) const {
		return value != rhs.value;
	};
	inline bool operator<=(const dtime_base_t &rhs) const {
		return value <= rhs.value;
	};
	inline bool operator<(const dtime_base_t &rhs) const {
		return value < rhs.value;
	};
	inline bool operator>(const dtime_base_t &rhs) const {
		return value > rhs.value;
	};
	inline bool operator>=(const dtime_base_t &rhs) const {
		return value >= rhs.value;
	};

	// arithmetic operators
	inline dtime_base_t operator+(const int64_t &value) const {
		return dtime_base_t(this->value + value);
	};
	inline dtime_base_t operator+(const double &value) const {
		return dtime_base_t(this->value + int64_t(value));
	};
	inline dtime_base_t operator-(const int64_t &value) const {
		return dtime_base_t(this->value - value);
	};
	inline dtime_base_t operator*(const idx_t &copies) const {
		return dtime_base_t(this->value * UnsafeNumericCast<int64_t>(copies));
	};
	inline dtime_base_t operator/(const idx_t &copies) const {
		return dtime_base_t(this->value / UnsafeNumericCast<int64_t>(copies));
	};
	inline int64_t operator-(const dtime_base_t &other) const {
		return this->value - other.value;
	};

	// in-place operators
	inline dtime_base_t &operator+=(const int64_t &value) {
		this->value += value;
		return *this;
	};
	inline dtime_base_t &operator-=(const int64_t &value) {
		this->value -= value;
		return *this;
	};
	inline dtime_base_t &operator+=(const dtime_base_t &other) {
		this->value += other.value;
		return *this;
	};

	// special values
	static inline dtime_base_t allballs() { // NOLINT
		return dtime_base_t(0);
	} // NOLINT

	inline bool IsFinite() const {
		return true;
	}
};

//! Type used to represent time (microseconds)
using dtime_t = dtime_base_t<1000000>;
using dtime_us_t = dtime_base_t<1000000>;

//! Type used to represent TIME_NS. dtime_ns_t holds the nanoseconds since midnight.
using dtime_ns_t = dtime_base_t<1000000000>;

struct dtime_tz_t { // NOLINT
	static constexpr const int TIME_BITS = 40;
	static constexpr const int OFFSET_BITS = 24;
	static constexpr const uint64_t OFFSET_MASK = ~uint64_t(0) >> TIME_BITS;
	static constexpr const int32_t MAX_OFFSET = 16 * 60 * 60 - 1; // ±15:59:59
	static constexpr const int32_t MIN_OFFSET = -MAX_OFFSET;
	static constexpr const uint64_t OFFSET_MICROS = 1000000;

	uint64_t bits;

	//	Offsets are reverse ordered e.g., 13:00:00+01 < 12:00:00+00 < 11:00:00-01
	//	Because we encode them as the low order bits,
	//	they are also biased into an unsigned integer: (-16, 16) => (32, 0)
	static inline uint64_t encode_offset(int32_t offset) { // NOLINT
		return uint64_t(MAX_OFFSET - offset);
	}
	static inline int32_t decode_offset(uint64_t bits) { // NOLINT
		return MAX_OFFSET - int32_t(bits & OFFSET_MASK);
	}

	static inline uint64_t encode_micros(int64_t micros) { // NOLINT
		return encode_micros(UnsafeNumericCast<uint64_t>(micros));
	}
	static inline uint64_t encode_micros(uint64_t micros) { // NOLINT
		return micros << OFFSET_BITS;
	}
	static inline int64_t decode_micros(uint64_t bits) { // NOLINT
		return int64_t(bits >> OFFSET_BITS);
	}

	dtime_tz_t() = default;

	inline dtime_tz_t(dtime_t t, int32_t offset) : bits(encode_micros(t.value) | encode_offset(offset)) {
	}
	explicit inline dtime_tz_t(uint64_t bits_p) : bits(bits_p) {
	}

	inline dtime_t time() const { // NOLINT
		return dtime_t(decode_micros(bits));
	}

	inline int32_t offset() const { // NOLINT
		return decode_offset(bits);
	}

	//	Times are compared after adjusting to offset +00:00:00, e.g., 13:01:00+01 > 12:00:00+00
	//	Because we encode them as the high order bits,
	//	they are biased by the maximum offset: (0, 24) => (0, 56)
	inline uint64_t sort_key() const { // NOLINT
		return bits + encode_micros((bits & OFFSET_MASK) * OFFSET_MICROS);
	}

	// comparison operators
	inline bool operator==(const dtime_tz_t &rhs) const {
		return bits == rhs.bits;
	};
	inline bool operator!=(const dtime_tz_t &rhs) const {
		return bits != rhs.bits;
	};
	inline bool operator<=(const dtime_tz_t &rhs) const {
		return sort_key() <= rhs.sort_key();
	};
	inline bool operator<(const dtime_tz_t &rhs) const {
		return sort_key() < rhs.sort_key();
	};
	inline bool operator>(const dtime_tz_t &rhs) const {
		return sort_key() > rhs.sort_key();
	};
	inline bool operator>=(const dtime_tz_t &rhs) const {
		return sort_key() >= rhs.sort_key();
	};

	inline bool IsFinite() const {
		return true;
	}
};

} // namespace duckdb

namespace std {

//! Time
template <>
struct hash<duckdb::dtime_t> {
	std::size_t operator()(const duckdb::dtime_t &k) const {
		using std::hash;
		return hash<int64_t>()((int64_t)k);
	}
};

template <>
struct hash<duckdb::dtime_ns_t> {
	std::size_t operator()(const duckdb::dtime_t &k) const {
		using std::hash;
		return hash<int64_t>()((int64_t)k);
	}
};

template <>
struct hash<duckdb::dtime_tz_t> {
	std::size_t operator()(const duckdb::dtime_tz_t &k) const {
		using std::hash;
		return hash<uint64_t>()(k.bits);
	}
};
} // namespace std
