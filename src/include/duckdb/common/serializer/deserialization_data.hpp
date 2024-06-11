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
class DatabaseInstance;
class CompressionInfo;
enum class ExpressionType : uint8_t;

struct DeserializationData {
	struct CustomData {
		virtual ~CustomData() = default;
	};

	stack<reference<ClientContext>> contexts;
	stack<reference<DatabaseInstance>> databases;
	stack<idx_t> enums;
	stack<reference<bound_parameter_map_t>> parameter_data;
	stack<const_reference<LogicalType>> types;
	stack<const_reference<CompressionInfo>> compression_infos;
	duckdb::unordered_map<std::string, duckdb::stack<duckdb::reference<CustomData>>> customs;

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

	template <typename T>
	typename std::enable_if<std::is_base_of<CustomData, T>::value, T &>::type GetCustom() const {
		std::string type = T::GetType();
		auto iter = customs.find(type);
		if (iter == customs.end()) {
			throw duckdb::InternalException("SeserializationData - no stack for %s", type);
		}
		auto &stack = iter->second;
		if (stack.empty()) {
			throw duckdb::InternalException("DeserializationData - unexpected empty stack for %s", type);
		}
		return dynamic_cast<T &>(stack.top().get());
	}

	template <typename T>
	typename std::enable_if<std::is_base_of<CustomData, T>::value, void>::type SetCustom(T &data) {
		std::string type = T::GetType();
		auto iter = customs.find(type);
		if (iter == customs.end()) {
			iter = customs.emplace(type, duckdb::stack<duckdb::reference<CustomData>> {}).first;
		}
		auto &stack = iter->second;
		stack.push(data);
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
inline void DeserializationData::Set(CompressionType type) {
	enums.push(idx_t(type));
}

template <>
inline CompressionType DeserializationData::Get() {
	AssertNotEmpty(enums);
	return CompressionType(enums.top());
}

template <>
inline void DeserializationData::Unset<CompressionType>() {
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
	contexts.emplace(context);
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
inline void DeserializationData::Set(DatabaseInstance &db) {
	databases.emplace(db);
}

template <>
inline DatabaseInstance &DeserializationData::Get() {
	AssertNotEmpty(databases);
	return databases.top();
}

template <>
inline void DeserializationData::Unset<DatabaseInstance>() {
	AssertNotEmpty(databases);
	databases.pop();
}

template <>
inline void DeserializationData::Set(bound_parameter_map_t &context) {
	parameter_data.emplace(context);
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
inline void DeserializationData::Unset<LogicalType>() {
	AssertNotEmpty(types);
	types.pop();
}

template <>
inline void DeserializationData::Set(const LogicalType &type) {
	types.emplace(type);
}

template <>
inline const LogicalType &DeserializationData::Get() {
	AssertNotEmpty(types);
	return types.top();
}

template <>
inline void DeserializationData::Unset<const LogicalType>() {
	AssertNotEmpty(types);
	types.pop();
}

template <>
inline void DeserializationData::Set(const CompressionInfo &compression_info) {
	compression_infos.emplace(compression_info);
}

template <>
inline const CompressionInfo &DeserializationData::Get() {
	AssertNotEmpty(compression_infos);
	return compression_infos.top();
}

template <>
inline void DeserializationData::Unset<const CompressionInfo>() {
	AssertNotEmpty(compression_infos);
	compression_infos.pop();
}

} // namespace duckdb
