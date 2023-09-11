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
#include "duckdb/planner/bound_parameter_map.hpp"

namespace duckdb {
class ClientContext;
class Catalog;
enum class ExpressionType : uint8_t;

struct DeserializationData {
	stack<reference<ClientContext>> contexts;
	stack<idx_t> enums;
	stack<reference<bound_parameter_map_t>> parameter_data;
	stack<reference<LogicalType>> types;

	template <class T>
	void Set(T entry) = delete;

	template <class T>
	T Get() = delete;

	template <class T>
	void Unset() = delete;

	template <class T>
	inline void AssertNotEmpty(const stack<T> &e) {
		if (e.empty()) {
			throw InternalException("DeserializationData - unexpected empty stack");
		}
	}
};

template <>
inline void DeserializationData::Set(ExpressionType type) {
	enums.push(idx_t(type));
}

template <>
inline ExpressionType DeserializationData::Get() {
	AssertNotEmpty(enums);
	return ExpressionType(enums.top());
}

template <>
inline void DeserializationData::Unset<ExpressionType>() {
	AssertNotEmpty(enums);
	enums.pop();
}

template <>
inline void DeserializationData::Set(LogicalOperatorType type) {
	enums.push(idx_t(type));
}

template <>
inline LogicalOperatorType DeserializationData::Get() {
	AssertNotEmpty(enums);
	return LogicalOperatorType(enums.top());
}

template <>
inline void DeserializationData::Unset<LogicalOperatorType>() {
	AssertNotEmpty(enums);
	enums.pop();
}

template <>
inline void DeserializationData::Set(CatalogType type) {
	enums.push(idx_t(type));
}

template <>
inline CatalogType DeserializationData::Get() {
	AssertNotEmpty(enums);
	return CatalogType(enums.top());
}

template <>
inline void DeserializationData::Unset<CatalogType>() {
	AssertNotEmpty(enums);
	enums.pop();
}

template <>
inline void DeserializationData::Set(ClientContext &context) {
	contexts.push(context);
}

template <>
inline ClientContext &DeserializationData::Get() {
	AssertNotEmpty(contexts);
	return contexts.top();
}

template <>
inline void DeserializationData::Unset<ClientContext>() {
	AssertNotEmpty(contexts);
	contexts.pop();
}

template <>
inline void DeserializationData::Set(bound_parameter_map_t &context) {
	parameter_data.push(context);
}

template <>
inline bound_parameter_map_t &DeserializationData::Get() {
	AssertNotEmpty(parameter_data);
	return parameter_data.top();
}

template <>
inline void DeserializationData::Unset<bound_parameter_map_t>() {
	AssertNotEmpty(parameter_data);
	parameter_data.pop();
}

template <>
inline void DeserializationData::Set(LogicalType &type) {
	types.emplace(type);
}

template <>
inline LogicalType &DeserializationData::Get() {
	AssertNotEmpty(types);
	return types.top();
}

template <>
inline void DeserializationData::Unset<LogicalType>() {
	AssertNotEmpty(types);
	types.pop();
}

} // namespace duckdb
