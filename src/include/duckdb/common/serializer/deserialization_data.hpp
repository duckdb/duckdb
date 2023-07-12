//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/serializer/deserialization_data.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/common/stack.hpp"

namespace duckdb {
class ClientContext;
class Catalog;
enum class ExpressionType : uint8_t;

struct DeserializationData {
	stack<reference<ClientContext>> contexts;
	stack<ExpressionType> types;

	template<class T>
	void Set(T entry) = delete;

	template<class T>
	T Get() = delete;

	template<class T>
	void Unset() = delete;

	template<class T>
	inline void AssertNotEmpty(const stack<T> &e) {
		if (e.empty()) {
			throw InternalException("DeserializationData - unexpected empty stack");
		}
	}
};

template<>
inline void DeserializationData::Set(ExpressionType type) {
	types.push(type);
}

template<>
inline ExpressionType DeserializationData::Get() {
	AssertNotEmpty(types);
	return types.top();
}

template<>
inline void DeserializationData::Unset<ExpressionType>() {
	AssertNotEmpty(types);
	types.pop();
}

template<>
inline void DeserializationData::Set(ClientContext &context) {
	contexts.push(context);
}

template<>
inline ClientContext &DeserializationData::Get() {
	AssertNotEmpty(contexts);
	return contexts.top();
}

template<>
inline void DeserializationData::Unset<ClientContext>() {
	AssertNotEmpty(contexts);
	contexts.pop();
}

} // namespace duckdb
