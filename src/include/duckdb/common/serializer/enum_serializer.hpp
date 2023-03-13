#pragma once
#include <stdint.h>

namespace duckdb {

struct EnumSerializer {
	// String -> Enum
	template <class T>
	static T StringToEnum(const char *value) = delete;

	// Enum -> String
	template <class T>
	static const char *EnumToString(T value) = delete;
};

enum class OrderType : uint8_t;
enum class OrderByNullType : uint8_t;
enum class ResultModifierType : uint8_t;
enum class ExtraTypeInfoType : uint8_t;
enum class TableReferenceType : uint8_t;
enum class JoinRefType : uint8_t;
enum class JoinType : uint8_t;
enum class AggregateHandling : uint8_t;
enum class QueryNodeType : uint8_t;
enum class SetOperationType : uint8_t;
enum class WindowBoundary : uint8_t;
enum class SubqueryType : uint8_t;
enum class ExpressionType : uint8_t;
enum class ExpressionClass : uint8_t;
enum class SampleMethod : uint8_t;
enum class LogicalTypeId : uint8_t;

template <>
OrderType EnumSerializer::StringToEnum<OrderType>(const char *value);
template <>
const char *EnumSerializer::EnumToString<OrderType>(OrderType value);

template <>
OrderByNullType EnumSerializer::StringToEnum<OrderByNullType>(const char *value);
template <>
const char *EnumSerializer::EnumToString<OrderByNullType>(OrderByNullType value);

template <>
ResultModifierType EnumSerializer::StringToEnum<ResultModifierType>(const char *value);
template <>
const char *EnumSerializer::EnumToString<ResultModifierType>(ResultModifierType value);

template <>
ExtraTypeInfoType EnumSerializer::StringToEnum<ExtraTypeInfoType>(const char *value);
template <>
const char *EnumSerializer::EnumToString<ExtraTypeInfoType>(ExtraTypeInfoType value);

template <>
TableReferenceType EnumSerializer::StringToEnum<TableReferenceType>(const char *value);
template <>
const char *EnumSerializer::EnumToString<TableReferenceType>(TableReferenceType value);

template <>
JoinRefType EnumSerializer::StringToEnum<JoinRefType>(const char *value);
template <>
const char *EnumSerializer::EnumToString<JoinRefType>(JoinRefType value);

template <>
JoinType EnumSerializer::StringToEnum<JoinType>(const char *value);
template <>
const char *EnumSerializer::EnumToString<JoinType>(JoinType value);

template <>
AggregateHandling EnumSerializer::StringToEnum<AggregateHandling>(const char *value);
template <>
const char *EnumSerializer::EnumToString<AggregateHandling>(AggregateHandling value);

template <>
QueryNodeType EnumSerializer::StringToEnum<QueryNodeType>(const char *value);
template <>
const char *EnumSerializer::EnumToString<QueryNodeType>(QueryNodeType value);

template <>
SetOperationType EnumSerializer::StringToEnum<SetOperationType>(const char *value);
template <>
const char *EnumSerializer::EnumToString<SetOperationType>(SetOperationType value);

template <>
WindowBoundary EnumSerializer::StringToEnum<WindowBoundary>(const char *value);
template <>
const char *EnumSerializer::EnumToString<WindowBoundary>(WindowBoundary value);

template <>
SubqueryType EnumSerializer::StringToEnum<SubqueryType>(const char *value);
template <>
const char *EnumSerializer::EnumToString<SubqueryType>(SubqueryType value);

template <>
ExpressionType EnumSerializer::StringToEnum<ExpressionType>(const char *value);
template <>
const char *EnumSerializer::EnumToString<ExpressionType>(ExpressionType value);

template <>
ExpressionClass EnumSerializer::StringToEnum<ExpressionClass>(const char *value);
template <>
const char *EnumSerializer::EnumToString<ExpressionClass>(ExpressionClass value);

template <>
SampleMethod EnumSerializer::StringToEnum<SampleMethod>(const char *value);
template <>
const char *EnumSerializer::EnumToString<SampleMethod>(SampleMethod value);

template <>
LogicalTypeId EnumSerializer::StringToEnum<LogicalTypeId>(const char *value);
template <>
const char *EnumSerializer::EnumToString<LogicalTypeId>(LogicalTypeId value);

} // namespace duckdb
