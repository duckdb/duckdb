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
#include "duckdb/common/extension_type_info.hpp"

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
	TEMPLATE_TYPE_INFO = 12,
	GEO_TYPE_INFO = 13
};

struct ExtraTypeInfo {
	ExtraTypeInfoType type;
	string alias;
	unique_ptr<ExtensionTypeInfo> extension_info;

	explicit ExtraTypeInfo(ExtraTypeInfoType type);
	explicit ExtraTypeInfo(ExtraTypeInfoType type, string alias);
	virtual ~ExtraTypeInfo();

protected:
	// copy	constructor (protected)
	ExtraTypeInfo(const ExtraTypeInfo &other);
	ExtraTypeInfo &operator=(const ExtraTypeInfo &other);

public:
	bool Equals(ExtraTypeInfo *other_p) const;

	virtual void Serialize(Serializer &serializer) const;
	static shared_ptr<ExtraTypeInfo> Deserialize(Deserializer &source);
	virtual shared_ptr<ExtraTypeInfo> Copy() const;
	virtual shared_ptr<ExtraTypeInfo> DeepCopy() const;

	template <class TARGET>
	TARGET &Cast() {
		DynamicCastCheck<TARGET>(this);
		return reinterpret_cast<TARGET &>(*this);
	}
	template <class TARGET>
	const TARGET &Cast() const {
		DynamicCastCheck<TARGET>(this);
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
	shared_ptr<ExtraTypeInfo> Copy() const override;

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
	shared_ptr<ExtraTypeInfo> Copy() const override;

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
	shared_ptr<ExtraTypeInfo> Copy() const override;
	shared_ptr<ExtraTypeInfo> DeepCopy() const override;

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
	shared_ptr<ExtraTypeInfo> Copy() const override;
	shared_ptr<ExtraTypeInfo> DeepCopy() const override;

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
	shared_ptr<ExtraTypeInfo> Copy() const override;

protected:
	bool EqualsInternal(ExtraTypeInfo *other_p) const override;

private:
	AggregateStateTypeInfo();
};

struct UserTypeInfo : public ExtraTypeInfo {
	explicit UserTypeInfo(string name_p);
	UserTypeInfo(string name_p, vector<Value> modifiers_p);
	UserTypeInfo(string catalog_p, string schema_p, string name_p, vector<Value> modifiers_p);

	string catalog;
	string schema;
	string user_type_name;
	vector<Value> user_type_modifiers;

public:
	void Serialize(Serializer &serializer) const override;
	static shared_ptr<ExtraTypeInfo> Deserialize(Deserializer &source);
	shared_ptr<ExtraTypeInfo> Copy() const override;

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
	shared_ptr<ExtraTypeInfo> Copy() const override;

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
	uint32_t size;
	explicit ArrayTypeInfo(LogicalType child_type_p, uint32_t size_p);

public:
	void Serialize(Serializer &serializer) const override;
	static shared_ptr<ExtraTypeInfo> Deserialize(Deserializer &reader);
	shared_ptr<ExtraTypeInfo> Copy() const override;
	shared_ptr<ExtraTypeInfo> DeepCopy() const override;

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
	shared_ptr<ExtraTypeInfo> Copy() const override;
	shared_ptr<ExtraTypeInfo> DeepCopy() const override;

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
	shared_ptr<ExtraTypeInfo> Copy() const override;

protected:
	bool EqualsInternal(ExtraTypeInfo *other_p) const override;

private:
	IntegerLiteralTypeInfo();
};

struct TemplateTypeInfo : public ExtraTypeInfo {
	explicit TemplateTypeInfo(string name_p);

	// The name of the template, e.g. `T`, or `KEY_TYPE`. Used to distinguish between different template types within
	// the same function. The binder tries to resolve all templates with the same name to the same concrete type.
	string name;

public:
	void Serialize(Serializer &serializer) const override;
	static shared_ptr<ExtraTypeInfo> Deserialize(Deserializer &source);
	shared_ptr<ExtraTypeInfo> Copy() const override;

protected:
	bool EqualsInternal(ExtraTypeInfo *other_p) const override;
	TemplateTypeInfo();
};

struct GeoTypeInfo : public ExtraTypeInfo {
public:
	GeoTypeInfo();

	void Serialize(Serializer &serializer) const override;
	static shared_ptr<ExtraTypeInfo> Deserialize(Deserializer &source);
	shared_ptr<ExtraTypeInfo> Copy() const override;

protected:
	bool EqualsInternal(ExtraTypeInfo *other_p) const override;
};

} // namespace duckdb
