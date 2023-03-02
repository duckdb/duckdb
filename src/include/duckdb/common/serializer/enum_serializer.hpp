#pragma once
#include <stdint.h>

namespace duckdb {

enum class OrderType : uint8_t ;
enum class OrderByNullType : uint8_t;
enum class ExtraTypeInfoType : uint8_t;
enum class TableReferenceType : uint8_t;
enum class JoinType : uint8_t;
enum class JoinRefType : uint8_t;
enum class ResultModifierType : uint8_t;
enum class AggregateHandling : uint8_t;
enum class QueryNodeType : uint8_t;
enum class SetOperationType : uint8_t;
enum class WindowBoundary : uint8_t;
enum class SubqueryType : uint8_t;
enum class ExpressionType : uint8_t;
enum class ExpressionClass : uint8_t;
enum class SampleMethod : uint8_t;
enum class LogicalTypeId : uint8_t;

struct EnumSerializer {
	// String -> Enum
	template <class T>
	static T StringToEnum(const char *value) = delete;

	template<>
	OrderType StringToEnum(const char* value);

	template<>
	OrderByNullType StringToEnum(const char* value);

	template <>
	ResultModifierType StringToEnum(const char* value);

	template<>
	ExtraTypeInfoType StringToEnum(const char* value);

	template<>
	TableReferenceType StringToEnum(const char* value);

	template<>
	JoinRefType StringToEnum(const char* value);

	template<>
	JoinType StringToEnum(const char* value);

	template <>
	AggregateHandling StringToEnum(const char* value);

	template <>
	QueryNodeType StringToEnum(const char* value);

	template <>
	SetOperationType StringToEnum(const char* value);

	template <>
	WindowBoundary StringToEnum(const char* value);

	template <>
	SubqueryType StringToEnum(const char* value);

	template <>
	ExpressionType StringToEnum(const char* value);

	template <>
	ExpressionClass StringToEnum(const char* value);

	template <>
	SampleMethod StringToEnum(const char* value);

	template <>
	LogicalTypeId StringToEnum(const char* value);


	// Enum -> String
	template <class T>
	static const char* EnumToString(T value) = delete;

	template<>
	const char* EnumToString(OrderType value);

	template<>
	const char* EnumToString(OrderByNullType value);

	template <>
	const char* EnumToString(ResultModifierType value);

	template<>
	const char* EnumToString(ExtraTypeInfoType value);

	template<>
	const char* EnumToString(TableReferenceType value);

	template<>
	const char* EnumToString(JoinRefType value);

	template<>
	const char* EnumToString(JoinType value);

	template <>
	const char* EnumToString(AggregateHandling value);

	template <>
	const char* EnumToString(QueryNodeType value);

	template <>
	const char* EnumToString(SetOperationType value);

	template <>
	const char* EnumToString(WindowBoundary value);

	template <>
	const char* EnumToString(SubqueryType value);

	template <>
	const char* EnumToString(ExpressionType value);

	template <>
	const char* EnumToString(ExpressionClass value);

	template <>
	const char* EnumToString(SampleMethod value);
};

}

/*
 * :'(
 *
#define ENUM_NAME Colors
#define ENUM_MEMBERS \
    MEMBER(RED, 1)    \
    MEMBER(BLUE, 2)   \
    MEMBER(GREEN, 3)

// Impl
#define FROM_STRING_NAME_IMPL(name) name##FromString
#define FROM_STRING_NAME(name) FROM_STRING_NAME_IMPL(name)

#define TO_STRING_NAME_IMPL(name) name##ToString
#define TO_STRING_NAME(name) TO_STRING_NAME_IMPL(name)

enum class ENUM_NAME : uint8_t {
#define MEMBER(name, val) name = val,
	ENUM_MEMBERS
#undef MEMBER
};

const char* TO_STRING_NAME(ENUM_NAME)(ENUM_NAME value) {
#define MEMBER(name, val) case ENUM_NAME::name: return #name;
	switch (value) {
		ENUM_MEMBERS
	default: throw InternalException("Unreachable");
	}
#undef MEMBER
}

ENUM_NAME FROM_STRING_NAME(ENUM_NAME)(const char* value) {
#define MEMBER(name, val) else if (strcmp(value, #name) == 0) { return ENUM_NAME::name; }
	if(false) {}
	ENUM_MEMBERS
	else { throw InternalException("Unreachable"); }
#undef MEMBER
}
*/
