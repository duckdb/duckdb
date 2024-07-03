#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/common/numeric_utils.hpp"

#include <functional>

namespace duckdb {

//! Type used to represent time (microseconds)
struct dtime_t { // NOLINT
	int64_t micros;

	dtime_t() = default;
	explicit inline dtime_t(int64_t micros_p) : micros(micros_p) {
	}
	inline dtime_t &operator=(int64_t micros_p) {
		micros = micros_p;
		return *this;
	}

	// explicit conversion
	explicit inline operator int64_t() const {
		return micros;
	}
	explicit inline operator double() const {
		return static_cast<double>(micros);
	}

	// comparison operators
	inline bool operator==(const dtime_t &rhs) const {
		return micros == rhs.micros;
	};
	inline bool operator!=(const dtime_t &rhs) const {
		return micros != rhs.micros;
	};
	inline bool operator<=(const dtime_t &rhs) const {
		return micros <= rhs.micros;
	};
	inline bool operator<(const dtime_t &rhs) const {
		return micros < rhs.micros;
	};
	inline bool operator>(const dtime_t &rhs) const {
		return micros > rhs.micros;
	};
	inline bool operator>=(const dtime_t &rhs) const {
		return micros >= rhs.micros;
	};

	// arithmetic operators
	inline dtime_t operator+(const int64_t &micros) const {
		return dtime_t(this->micros + micros);
	};
	inline dtime_t operator+(const double &micros) const {
		return dtime_t(this->micros + int64_t(micros));
	};
	inline dtime_t operator-(const int64_t &micros) const {
		return dtime_t(this->micros - micros);
	};
	inline dtime_t operator*(const idx_t &copies) const {
		return dtime_t(this->micros * UnsafeNumericCast<int64_t>(copies));
	};
	inline dtime_t operator/(const idx_t &copies) const {
		return dtime_t(this->micros / UnsafeNumericCast<int64_t>(copies));
	};
	inline int64_t operator-(const dtime_t &other) const {
		return this->micros - other.micros;
	};

	// in-place operators
	inline dtime_t &operator+=(const int64_t &micros) {
		this->micros += micros;
		return *this;
	};
	inline dtime_t &operator-=(const int64_t &micros) {
		this->micros -= micros;
		return *this;
	};
	inline dtime_t &operator+=(const dtime_t &other) {
		this->micros += other.micros;
		return *this;
	};

	// special values
	static inline dtime_t allballs() { // NOLINT
		return dtime_t(0);
	} // NOLINT
};

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
		return uint64_t(micros) << OFFSET_BITS;
	}
	static inline int64_t decode_micros(uint64_t bits) { // NOLINT
		return int64_t(bits >> OFFSET_BITS);
	}

	dtime_tz_t() = default;

	inline dtime_tz_t(dtime_t t, int32_t offset) : bits(encode_micros(t.micros) | encode_offset(offset)) {
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
struct hash<duckdb::dtime_tz_t> {
	std::size_t operator()(const duckdb::dtime_tz_t &k) const {
		using std::hash;
		return hash<uint64_t>()(k.bits);
	}
};
} // namespace std
