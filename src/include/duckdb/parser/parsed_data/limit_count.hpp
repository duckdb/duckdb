//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/parser/parsed_data/limit_count.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include <string>
#include "duckdb/common/common.hpp"
#include "duckdb/common/limits.hpp"

namespace duckdb {

struct LimitCount {
	double limit_value;
	bool is_percentage;

public:
	LimitCount() {
		is_percentage = false;
		limit_value = (double)NumericLimits<int64_t>::Maximum();
	}

	idx_t GetLimitValue(idx_t count) const {
		if (is_percentage) {
			return MinValue((idx_t)(limit_value * count / 100), count);
		}

		return (idx_t)limit_value;
	}

	void SetLimitValue(bool is_percentage, double limit_value) {
		this->is_percentage = is_percentage;
		this->limit_value = limit_value;
	}

	bool IsMaximum() {
		if (!is_percentage && limit_value == (double)NumericLimits<int64_t>::Maximum()) {
			return true;
		}

		return false;
	}

	bool IsZero() const {
		return (limit_value == 0.0);
	}

	bool IsInvalid() {
		return (limit_value == (double)-1);
	}

	void SetInvalid() {
		is_percentage = false;
		limit_value = (double)-1;
	}

	string ToString() const {
		if (is_percentage) {
			return std::to_string(limit_value) + "%";
		}

		return std::to_string((idx_t)limit_value);
	}
};

} // namespace duckdb
