#pragma once

namespace duckdb {

struct TransformResultValue {
	virtual ~TransformResultValue() = default;
};

template <class T>
struct TypedTransformResult : public TransformResultValue {
	explicit TypedTransformResult(T value_p) : value(std::move(value_p)) {
	}
	T value;
};

} // namespace duckdb
