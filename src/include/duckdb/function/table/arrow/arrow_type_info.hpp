//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/function/table/arrow/arrow_type_info.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/types.hpp"
#include "duckdb/common/unordered_map.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/unique_ptr.hpp"
#include "duckdb/function/table/arrow/enum/arrow_type_info_type.hpp"
#include "duckdb/function/table/arrow/enum/arrow_datetime_type.hpp"
#include "duckdb/function/table/arrow/enum/arrow_variable_size_type.hpp"
#include "duckdb/common/enum_util.hpp"

namespace duckdb {

class ArrowType;

struct ArrowTypeInfo {
public:
	explicit ArrowTypeInfo(ArrowTypeInfoType type);
	virtual ~ArrowTypeInfo();

	ArrowTypeInfoType type;

public:
	template <class TARGET>
	TARGET &Cast() {
		D_ASSERT(dynamic_cast<TARGET *>(this));
		if (type != TARGET::TYPE) {
			throw InternalException("Failed to cast ArrowTypeInfo, type mismatch (expected: %s, got: %s)",
			                        EnumUtil::ToString(TARGET::TYPE), EnumUtil::ToString(type));
		}
		return reinterpret_cast<TARGET &>(*this);
	}
	template <class TARGET>
	const TARGET &Cast() const {
		D_ASSERT(dynamic_cast<const TARGET *>(this));
		if (type != TARGET::TYPE) {
			throw InternalException("Failed to cast ArrowTypeInfo, type mismatch (expected: %s, got: %s)",
			                        EnumUtil::ToString(TARGET::TYPE), EnumUtil::ToString(type));
		}
		return reinterpret_cast<const TARGET &>(*this);
	}
};

struct ArrowStructInfo : public ArrowTypeInfo {
public:
	static constexpr const ArrowTypeInfoType TYPE = ArrowTypeInfoType::STRUCT;

public:
	explicit ArrowStructInfo(vector<unique_ptr<ArrowType>> children);
	~ArrowStructInfo() override;

public:
	idx_t ChildCount() const;
	const ArrowType &GetChild(idx_t index) const;
	const vector<unique_ptr<ArrowType>> &GetChildren() const;

private:
	vector<unique_ptr<ArrowType>> children;
};

struct ArrowDateTimeInfo : public ArrowTypeInfo {
public:
	static constexpr const ArrowTypeInfoType TYPE = ArrowTypeInfoType::DATE_TIME;

public:
	explicit ArrowDateTimeInfo(ArrowDateTimeType size);
	~ArrowDateTimeInfo() override;

public:
	ArrowDateTimeType GetDateTimeType() const;

private:
	ArrowDateTimeType size_type;
};

struct ArrowStringInfo : public ArrowTypeInfo {
public:
	static constexpr const ArrowTypeInfoType TYPE = ArrowTypeInfoType::STRING;

public:
	explicit ArrowStringInfo(ArrowVariableSizeType size);
	explicit ArrowStringInfo(idx_t fixed_size);
	~ArrowStringInfo() override;

public:
	ArrowVariableSizeType GetSizeType() const;
	idx_t FixedSize() const;

private:
	ArrowVariableSizeType size_type;
	idx_t fixed_size;
};

struct ArrowListInfo : public ArrowTypeInfo {
public:
	static constexpr const ArrowTypeInfoType TYPE = ArrowTypeInfoType::LIST;

public:
	static unique_ptr<ArrowListInfo> ListView(unique_ptr<ArrowType> child, ArrowVariableSizeType size);
	static unique_ptr<ArrowListInfo> List(unique_ptr<ArrowType> child, ArrowVariableSizeType size);
	~ArrowListInfo() override;

public:
	ArrowVariableSizeType GetSizeType() const;
	bool IsView() const;
	ArrowType &GetChild() const;

private:
	explicit ArrowListInfo(unique_ptr<ArrowType> child, ArrowVariableSizeType size);

private:
	ArrowVariableSizeType size_type;
	bool is_view = false;
	unique_ptr<ArrowType> child;
};

struct ArrowArrayInfo : public ArrowTypeInfo {
public:
	static constexpr const ArrowTypeInfoType TYPE = ArrowTypeInfoType::ARRAY;

public:
	explicit ArrowArrayInfo(unique_ptr<ArrowType> child, idx_t fixed_size);
	~ArrowArrayInfo() override;

public:
	idx_t FixedSize() const;
	ArrowType &GetChild() const;

private:
	unique_ptr<ArrowType> child;
	idx_t fixed_size;
};

} // namespace duckdb
