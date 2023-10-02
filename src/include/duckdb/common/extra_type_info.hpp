//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/extra_type_info.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"
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
	AGGREGATE_STATE_TYPE_INFO = 8
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

} // namespace duckdb
