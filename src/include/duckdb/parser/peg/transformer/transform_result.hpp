#pragma once

#include "duckdb/common/common.hpp"

#include <cstring>

namespace duckdb {

template <class T>
struct TransformResultTypeIdentifier {
	static const char *GetName() {
		static_assert(AlwaysFalse<T>::VALUE,
		              "Transform result types must be registered with DUCKDB_REGISTER_TRANSFORM_RESULT_TYPE");
		return nullptr;
	}
};

//! Registers a stable name for a transform result type. Invoke this macro from namespace duckdb.
#define DUCKDB_REGISTER_TRANSFORM_RESULT_TYPE(NAME, ...)                                                               \
	template <>                                                                                                        \
	struct TransformResultTypeIdentifier<__VA_ARGS__> {                                                                \
		static constexpr const char *GetName() {                                                                       \
			return NAME;                                                                                               \
		}                                                                                                              \
	};

//! A stable per-type name, used to identify transform results across loadable extension boundaries without RTTI.
template <class T>
const char *TransformResultTypeName() {
	return TransformResultTypeIdentifier<T>::GetName();
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
