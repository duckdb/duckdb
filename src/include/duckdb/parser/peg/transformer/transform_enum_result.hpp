#pragma once

#include "duckdb/common/common.hpp"

namespace duckdb {
struct TransformEnumValue {
	virtual ~TransformEnumValue() = default;
};

template <class T>
struct TypedTransformEnumResult : public TransformEnumValue {
	explicit TypedTransformEnumResult(T value_p) : value(std::move(value_p)) {
	}
	T value;
};

} // namespace duckdb
