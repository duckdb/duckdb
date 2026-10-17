#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/common/arena_containers/arena_ptr.hpp"

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

//! FNV-1a hash of a transform result type name, so most mismatches are rejected without a string compare
constexpr uint64_t TransformResultTypeHash(const char *type_name) {
	uint64_t hash = 14695981039346656037ULL;
	for (; *type_name; type_name++) {
		hash = (hash ^ static_cast<uint8_t>(*type_name)) * 1099511628211ULL;
	}
	return hash;
}

//! Registers a stable name for a transform result type. Invoke this macro from namespace duckdb.
#define DUCKDB_REGISTER_TRANSFORM_RESULT_TYPE(NAME, ...)                                                               \
	template <>                                                                                                        \
	struct TransformResultTypeIdentifier<__VA_ARGS__> {                                                                \
		static constexpr const char *GetName() {                                                                       \
			return NAME;                                                                                               \
		}                                                                                                              \
		static constexpr uint64_t HASH = TransformResultTypeHash(NAME);                                                \
	};

//! A stable per-type name, used to identify transform results across loadable extension boundaries without RTTI.
template <class T>
const char *TransformResultTypeName() {
	return TransformResultTypeIdentifier<T>::GetName();
}

struct DUCKDB_API TransformResultValue {
	virtual ~TransformResultValue() = default;

	//! Returns a pointer to the value if its type matches, without relying on RTTI
	virtual void *GetValuePointer(const char *type_name, uint64_t type_hash) = 0;
	void *GetValuePointer(const char *type_name) {
		return GetValuePointer(type_name, TransformResultTypeHash(type_name));
	}
};

template <class T>
struct DUCKDB_API TypedTransformResult : public TransformResultValue {
	explicit TypedTransformResult(T value_p) : value(std::move(value_p)) {
	}
	TypedTransformResult(const TypedTransformResult &) = delete;
	TypedTransformResult &operator=(const TypedTransformResult &) = delete;

	using TransformResultValue::GetValuePointer;
	void *GetValuePointer(const char *type_name, uint64_t type_hash) override {
		auto expected = TransformResultTypeName<T>();
		if (type_name == expected) {
			return &value;
		}
		if (type_hash != TransformResultTypeIdentifier<T>::HASH) {
			return nullptr;
		}
		return std::strcmp(type_name, expected) == 0 ? &value : nullptr;
	}

	T value;
};

//! Returns a pointer to the contained value if the result holds exactly T, and nullptr otherwise
template <class T>
T *TryGetTransformResult(TransformResultValue &result) {
	return reinterpret_cast<T *>(
	    result.GetValuePointer(TransformResultTypeName<T>(), TransformResultTypeIdentifier<T>::HASH));
}

} // namespace duckdb
