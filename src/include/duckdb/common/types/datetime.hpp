#pragma once

#include "duckdb/common/common.hpp"

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
		return micros;
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
		return dtime_t(this->micros * copies);
	};
	inline dtime_t operator/(const idx_t &copies) const {
		return dtime_t(this->micros / copies);
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
	static inline dtime_t allballs() {
		return dtime_t(0);
	} // NOLINT
};

struct dtime_tz_t : public dtime_t {};

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
		return hash<int64_t>()((int64_t)k);
	}
};
} // namespace std
