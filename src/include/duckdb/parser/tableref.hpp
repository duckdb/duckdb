//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/parser/tableref.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/common/enums/tableref_type.hpp"
#include "duckdb/parser/parsed_data/sample_options.hpp"

namespace duckdb {

//! Represents a generic expression that returns a table.
class TableRef {
public:
	static constexpr const TableReferenceType TYPE = TableReferenceType::INVALID;

public:
	explicit TableRef(TableReferenceType type) : type(type) {
	}
	virtual ~TableRef() {
	}

	TableReferenceType type;
	string alias;
	//! Sample options (if any)
	unique_ptr<SampleOptions> sample;
	//! The location in the query (if any)
	idx_t query_location = DConstants::INVALID_INDEX;

public:
	//! Convert the object to a string
	virtual string ToString() const = 0;
	string BaseToString(string result) const;
	string BaseToString(string result, const vector<string> &column_name_alias) const;
	void Print();

	virtual bool Equals(const TableRef &other) const;
	static bool Equals(const unique_ptr<TableRef> &left, const unique_ptr<TableRef> &right);

	virtual unique_ptr<TableRef> Copy() = 0;

	//! Copy the properties of this table ref to the target
	void CopyProperties(TableRef &target) const;

	virtual void Serialize(Serializer &serializer) const;
	static unique_ptr<TableRef> Deserialize(Deserializer &deserializer);

public:
	template <class TARGET>
	TARGET &Cast() {
		if (type != TARGET::TYPE && TARGET::TYPE != TableReferenceType::INVALID) {
			throw InternalException("Failed to cast constraint to type - constraint type mismatch");
		}
		return reinterpret_cast<TARGET &>(*this);
	}

	template <class TARGET>
	const TARGET &Cast() const {
		if (type != TARGET::TYPE && TARGET::TYPE != TableReferenceType::INVALID) {
			throw InternalException("Failed to cast constraint to type - constraint type mismatch");
		}
		return reinterpret_cast<const TARGET &>(*this);
	}
};
} // namespace duckdb
