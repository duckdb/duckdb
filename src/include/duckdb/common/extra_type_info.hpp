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
#include "duckdb/common/types/geometry_crs.hpp"

namespace duckdb {

class ParsedExpression;

struct DecimalTypeInfo : public ExtraTypeInfo {
	DecimalTypeInfo(uint8_t width_p, uint8_t scale_p);

	uint8_t width;
	uint8_t scale;

public:
	void Serialize(Serializer &serializer) const override;
	static unique_ptr<ExtraTypeInfo> Deserialize(Deserializer &source);
	unique_ptr<ExtraTypeInfo> Copy() const override;

protected:
	bool EqualsInternal(const ExtraTypeInfo *other_p) const override;

private:
	DecimalTypeInfo();
};

struct StringTypeInfo : public ExtraTypeInfo {
	explicit StringTypeInfo(string collation_p);

	string collation;

public:
	void Serialize(Serializer &serializer) const override;
	static unique_ptr<ExtraTypeInfo> Deserialize(Deserializer &source);
	unique_ptr<ExtraTypeInfo> Copy() const override;

protected:
	bool EqualsInternal(const ExtraTypeInfo *other_p) const override;

private:
	StringTypeInfo();
};

struct ListTypeInfo : public ExtraTypeInfo {
	explicit ListTypeInfo(LogicalType child_type_p);

	LogicalType child_type;

public:
	void Serialize(Serializer &serializer) const override;
	static unique_ptr<ExtraTypeInfo> Deserialize(Deserializer &source);
	unique_ptr<ExtraTypeInfo> Copy() const override;

protected:
	bool EqualsInternal(const ExtraTypeInfo *other_p) const override;

private:
	ListTypeInfo();
};

struct StructTypeInfo : public ExtraTypeInfo {
	explicit StructTypeInfo(child_list_t<LogicalType> child_types_p);

	child_list_t<LogicalType> child_types;

public:
	void Serialize(Serializer &serializer) const override;
	static unique_ptr<ExtraTypeInfo> Deserialize(Deserializer &deserializer);
	unique_ptr<ExtraTypeInfo> Copy() const override;

protected:
	bool EqualsInternal(const ExtraTypeInfo *other_p) const override;

private:
	StructTypeInfo();
};

struct LegacyAggregateStateTypeInfo : public ExtraTypeInfo {
public:
	void Serialize(Serializer &serializer) const override;
	// Legacy deserialize method kept only for compatibility with old database files
	static unique_ptr<ExtraTypeInfo> Deserialize(Deserializer &source);

	static unique_ptr<ExtraTypeInfo> LegacyDeserialize();

protected:
	bool EqualsInternal(const ExtraTypeInfo *other_p) const override;

private:
	LegacyAggregateStateTypeInfo();
};

// If this type is primarily stored in the catalog or not. Enums from Pandas/Factors are not in the catalog.
enum EnumDictType : uint8_t { INVALID = 0, VECTOR_DICT = 1 };

struct EnumTypeInfo : public ExtraTypeInfo {
	explicit EnumTypeInfo(const Vector &values_insert_order_p, idx_t dict_size_p);
	EnumTypeInfo(const EnumTypeInfo &) = delete;
	EnumTypeInfo &operator=(const EnumTypeInfo &) = delete;

public:
	const EnumDictType &GetEnumDictType() const;
	const Vector &GetValuesInsertOrder() const;
	const idx_t &GetDictSize() const;
	static PhysicalType DictType(idx_t size);

	static LogicalType CreateType(const Vector &ordered_data, idx_t size);
	static unique_ptr<ExtraTypeInfo> CreateTypeInfo(const Vector &ordered_data, idx_t size);

	void Serialize(Serializer &serializer) const override;
	static unique_ptr<ExtraTypeInfo> Deserialize(Deserializer &source);
	unique_ptr<ExtraTypeInfo> Copy() const override;

protected:
	// Equalities are only used in enums with different catalog entries
	bool EqualsInternal(const ExtraTypeInfo *other_p) const override;

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
	static unique_ptr<ExtraTypeInfo> Deserialize(Deserializer &reader);
	unique_ptr<ExtraTypeInfo> Copy() const override;

protected:
	bool EqualsInternal(const ExtraTypeInfo *other_p) const override;
};

struct AnyTypeInfo : public ExtraTypeInfo {
	AnyTypeInfo(LogicalType target_type, idx_t cast_score);

	LogicalType target_type;
	idx_t cast_score;

public:
	void Serialize(Serializer &serializer) const override;
	static unique_ptr<ExtraTypeInfo> Deserialize(Deserializer &source);
	unique_ptr<ExtraTypeInfo> Copy() const override;

protected:
	bool EqualsInternal(const ExtraTypeInfo *other_p) const override;

private:
	AnyTypeInfo();
};

struct IntegerLiteralTypeInfo : public ExtraTypeInfo {
	explicit IntegerLiteralTypeInfo(Value constant_value);

	Value constant_value;

public:
	void Serialize(Serializer &serializer) const override;
	static unique_ptr<ExtraTypeInfo> Deserialize(Deserializer &source);
	unique_ptr<ExtraTypeInfo> Copy() const override;

protected:
	bool EqualsInternal(const ExtraTypeInfo *other_p) const override;

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
	static unique_ptr<ExtraTypeInfo> Deserialize(Deserializer &source);
	unique_ptr<ExtraTypeInfo> Copy() const override;

protected:
	bool EqualsInternal(const ExtraTypeInfo *other_p) const override;
	TemplateTypeInfo();
};

struct GeoTypeInfo : public ExtraTypeInfo {
public:
	GeoTypeInfo();

	void Serialize(Serializer &serializer) const override;
	static unique_ptr<ExtraTypeInfo> Deserialize(Deserializer &source);
	unique_ptr<ExtraTypeInfo> Copy() const override;

	// The Coordinate Reference System associated with this geometry type
	CoordinateReferenceSystem crs;

protected:
	bool EqualsInternal(const ExtraTypeInfo *other_p) const override;
};

struct UnboundTypeInfo : public ExtraTypeInfo {
	explicit UnboundTypeInfo(unique_ptr<ParsedExpression> expr_p);

	unique_ptr<ParsedExpression> expr;

	void Serialize(Serializer &serializer) const override;
	static unique_ptr<ExtraTypeInfo> Deserialize(Deserializer &source);
	unique_ptr<ExtraTypeInfo> Copy() const override;

protected:
	bool EqualsInternal(const ExtraTypeInfo *other_p) const override;

private:
	UnboundTypeInfo();
};

} // namespace duckdb
