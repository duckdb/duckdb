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

struct DUCKDB_API TransformResultValue {
	virtual ~TransformResultValue() = default;

	//! Returns a pointer to the value if its type matches, without relying on RTTI
	virtual void *GetValuePointer(const char *type_name) = 0;
};

template <class T>
struct DUCKDB_API TypedTransformResult : public TransformResultValue {
	explicit TypedTransformResult(T value_p) : value(std::move(value_p)) {
	}
	TypedTransformResult(const TypedTransformResult &) = delete;
	TypedTransformResult &operator=(const TypedTransformResult &) = delete;

	void *GetValuePointer(const char *type_name) override {
		auto expected = TransformResultTypeName<T>();
		return type_name == expected || std::strcmp(type_name, expected) == 0 ? &value : nullptr;
	}

	T value;
};

//! Returns a pointer to the contained value if the result holds exactly T, and nullptr otherwise
template <class T>
T *TryGetTransformResult(TransformResultValue &result) {
	return reinterpret_cast<T *>(result.GetValuePointer(TransformResultTypeName<T>()));
}

} // namespace duckdb
