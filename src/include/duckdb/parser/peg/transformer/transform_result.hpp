#pragma once

#include "duckdb/common/common.hpp"

#include <cstring>

namespace duckdb {

//! A per-type name, used to identify transform results without RTTI.
//! The address of a static member cannot be used for this: a loadable extension links its own copy of
//! DuckDB, so the same instantiation exists at a different address in each image. Comparing the name
//! keeps a transform result created in one image castable in the other.
template <class T>
const char *TransformResultTypeName() {
#ifdef _MSC_VER
	return __FUNCSIG__;
#else
	return __PRETTY_FUNCTION__;
#endif
}

struct TransformResultValue {
	virtual ~TransformResultValue() = default;

	//! Identifies the concrete TypedTransformResult<T> without relying on RTTI
	virtual const char *TypeTag() const = 0;
};

template <class T>
struct TypedTransformResult : public TransformResultValue {
	explicit TypedTransformResult(T value_p) : value(std::move(value_p)) {
	}

	const char *TypeTag() const override {
		return TransformResultTypeName<T>();
	}

	T value;
};

//! Casts to TypedTransformResult<T> if the result holds exactly that type, and returns nullptr otherwise
template <class T>
TypedTransformResult<T> *TryCastTransformResult(TransformResultValue *result) {
	if (!result) {
		return nullptr;
	}
	auto tag = result->TypeTag();
	auto expected = TransformResultTypeName<T>();
	// the pointers are equal whenever both sides come from the same image, which is the common case
	if (tag != expected && std::strcmp(tag, expected) != 0) {
		return nullptr;
	}
	return static_cast<TypedTransformResult<T> *>(result);
}

template <class T>
TypedTransformResult<T> *TryCastTransformResult(TransformResultValue &result) {
	return TryCastTransformResult<T>(&result);
}

} // namespace duckdb
