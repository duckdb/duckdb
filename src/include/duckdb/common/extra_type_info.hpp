//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/extra_type_info.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/common/enums/order_type.hpp"
#include "duckdb/common/types/vector.hpp"

namespace duckdb {

//! Extra Type Info Type
enum class ExtraTypeInfoType : uint8_t {
	INVALID_TYPE_INFO = 0,
	GENERIC_TYPE_INFO = 1,
	DECIMAL_TYPE_INFO = 2,
	STRING_TYPE_INFO = 3,
	LIST_TYPE_INFO = 4,
	STRUCT_TYPE_INFO = 5,
	ENUM_TYPE_INFO = 6,
	USER_TYPE_INFO = 7,
	AGGREGATE_STATE_TYPE_INFO = 8,
	ARRAY_TYPE_INFO = 9,
	ANY_TYPE_INFO = 10,
	INTEGER_LITERAL_TYPE_INFO = 11,
	SORT_KEY_TYPE_INFO = 12
};

struct ExtraTypeInfo {
	explicit ExtraTypeInfo(ExtraTypeInfoType type);
	explicit ExtraTypeInfo(ExtraTypeInfoType type, string alias);
	virtual ~ExtraTypeInfo();

	ExtraTypeInfoType type;
	string alias;

public:
	bool Equals(ExtraTypeInfo *other_p) const;

	virtual void Serialize(Serializer &serializer) const;
	static shared_ptr<ExtraTypeInfo> Deserialize(Deserializer &source);

	template <class TARGET>
	TARGET &Cast() {
		D_ASSERT(dynamic_cast<TARGET *>(this));
		return reinterpret_cast<TARGET &>(*this);
	}
	template <class TARGET>
	const TARGET &Cast() const {
		D_ASSERT(dynamic_cast<const TARGET *>(this));
		return reinterpret_cast<const TARGET &>(*this);
	}

protected:
	virtual bool EqualsInternal(ExtraTypeInfo *other_p) const;
};

struct DecimalTypeInfo : public ExtraTypeInfo {
	DecimalTypeInfo(uint8_t width_p, uint8_t scale_p);

	uint8_t width;
	uint8_t scale;

public:
	void Serialize(Serializer &serializer) const override;
	static shared_ptr<ExtraTypeInfo> Deserialize(Deserializer &source);

protected:
	bool EqualsInternal(ExtraTypeInfo *other_p) const override;

private:
	DecimalTypeInfo();
};

struct StringTypeInfo : public ExtraTypeInfo {
	explicit StringTypeInfo(string collation_p);

	string collation;

public:
	void Serialize(Serializer &serializer) const override;
	static shared_ptr<ExtraTypeInfo> Deserialize(Deserializer &source);

protected:
	bool EqualsInternal(ExtraTypeInfo *other_p) const override;

private:
	StringTypeInfo();
};

struct ListTypeInfo : public ExtraTypeInfo {
	explicit ListTypeInfo(LogicalType child_type_p);

	LogicalType child_type;

public:
	void Serialize(Serializer &serializer) const override;
	static shared_ptr<ExtraTypeInfo> Deserialize(Deserializer &source);

protected:
	bool EqualsInternal(ExtraTypeInfo *other_p) const override;

private:
	ListTypeInfo();
};

struct StructTypeInfo : public ExtraTypeInfo {
	explicit StructTypeInfo(child_list_t<LogicalType> child_types_p);

	child_list_t<LogicalType> child_types;

public:
	void Serialize(Serializer &serializer) const override;
	static shared_ptr<ExtraTypeInfo> Deserialize(Deserializer &deserializer);

protected:
	bool EqualsInternal(ExtraTypeInfo *other_p) const override;

private:
	StructTypeInfo();
};

struct AggregateStateTypeInfo : public ExtraTypeInfo {
	explicit AggregateStateTypeInfo(aggregate_state_t state_type_p);

	aggregate_state_t state_type;

public:
	void Serialize(Serializer &serializer) const override;
	static shared_ptr<ExtraTypeInfo> Deserialize(Deserializer &source);

protected:
	bool EqualsInternal(ExtraTypeInfo *other_p) const override;

private:
	AggregateStateTypeInfo();
};

struct UserTypeInfo : public ExtraTypeInfo {
	explicit UserTypeInfo(string name_p);
	UserTypeInfo(string catalog_p, string schema_p, string name_p);

	string catalog;
	string schema;
	string user_type_name;

public:
	void Serialize(Serializer &serializer) const override;
	static shared_ptr<ExtraTypeInfo> Deserialize(Deserializer &source);

protected:
	bool EqualsInternal(ExtraTypeInfo *other_p) const override;

private:
	UserTypeInfo();
};

// If this type is primarily stored in the catalog or not. Enums from Pandas/Factors are not in the catalog.
enum EnumDictType : uint8_t { INVALID = 0, VECTOR_DICT = 1 };

struct EnumTypeInfo : public ExtraTypeInfo {
	explicit EnumTypeInfo(Vector &values_insert_order_p, idx_t dict_size_p);
	EnumTypeInfo(const EnumTypeInfo &) = delete;
	EnumTypeInfo &operator=(const EnumTypeInfo &) = delete;

public:
	const EnumDictType &GetEnumDictType() const;
	const Vector &GetValuesInsertOrder() const;
	const idx_t &GetDictSize() const;
	static PhysicalType DictType(idx_t size);

	static LogicalType CreateType(Vector &ordered_data, idx_t size);

	void Serialize(Serializer &serializer) const override;
	static shared_ptr<ExtraTypeInfo> Deserialize(Deserializer &source);

protected:
	// Equalities are only used in enums with different catalog entries
	bool EqualsInternal(ExtraTypeInfo *other_p) const override;

	Vector values_insert_order;

private:
	EnumDictType dict_type;
	idx_t dict_size;
};

struct ArrayTypeInfo : public ExtraTypeInfo {
	LogicalType child_type;
	idx_t size;
	explicit ArrayTypeInfo(LogicalType child_type_p, idx_t size_p);

public:
	void Serialize(Serializer &serializer) const override;
	static shared_ptr<ExtraTypeInfo> Deserialize(Deserializer &reader);

protected:
	bool EqualsInternal(ExtraTypeInfo *other_p) const override;
};

struct AnyTypeInfo : public ExtraTypeInfo {
	AnyTypeInfo(LogicalType target_type, idx_t cast_score);

	LogicalType target_type;
	idx_t cast_score;

public:
	void Serialize(Serializer &serializer) const override;
	static shared_ptr<ExtraTypeInfo> Deserialize(Deserializer &source);

protected:
	bool EqualsInternal(ExtraTypeInfo *other_p) const override;

private:
	AnyTypeInfo();
};

struct IntegerLiteralTypeInfo : public ExtraTypeInfo {
	explicit IntegerLiteralTypeInfo(Value constant_value);

	Value constant_value;

public:
	void Serialize(Serializer &serializer) const override;
	static shared_ptr<ExtraTypeInfo> Deserialize(Deserializer &source);

protected:
	bool EqualsInternal(ExtraTypeInfo *other_p) const override;

private:
	IntegerLiteralTypeInfo();
};

struct OrderBySpec {
	OrderBySpec(OrderType type, OrderByNullType null_order, const LogicalType &expr_type, bool has_null)
	    : type(type), null_order(null_order), expr_type(expr_type), has_null(has_null) {
	}

	bool operator==(const OrderBySpec &other) const {
		return type == other.type && null_order == other.null_order && expr_type == other.expr_type &&
		       has_null == other.has_null;
	}
	bool operator!=(const OrderBySpec &other) const {
		return !(*this == other);
	}

	//! Sort order, ASC or DESC
	OrderType type;
	//! The NULL sort order, NULLS_FIRST or NULLS_LAST
	OrderByNullType null_order;
	//! Type to order by
	LogicalType expr_type;
	//! Does the type's column have NULL values?
	bool has_null;

public:
	string ToString() const;

	void Serialize(Serializer &serializer) const;
	static OrderBySpec Deserialize(Deserializer &deserializer);
};

struct SortKeyTypeInfo : public ExtraTypeInfo {
	explicit SortKeyTypeInfo(vector<OrderBySpec> orders);

	vector<OrderBySpec> order_bys;

public:
	void Serialize(Serializer &serializer) const override;
	static shared_ptr<ExtraTypeInfo> Deserialize(Deserializer &source);

protected:
	bool EqualsInternal(ExtraTypeInfo *other_p) const override;

private:
	SortKeyTypeInfo();
};

} // namespace duckdb
