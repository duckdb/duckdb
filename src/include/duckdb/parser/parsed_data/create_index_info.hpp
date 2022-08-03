//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/parser/parsed_data/create_index_info.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/parser/parsed_data/create_info.hpp"
#include "duckdb/common/enums/index_type.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/parser/tableref/basetableref.hpp"
#include "duckdb/parser/parsed_expression.hpp"

namespace duckdb {

struct CreateIndexInfo : public CreateInfo {
	CreateIndexInfo() : CreateInfo(CatalogType::INDEX_ENTRY) {
	}

	//! Index Type (e.g., B+-tree, Skip-List, ...)
	IndexType index_type;
	//! Name of the Index
	string index_name;
	//! Index Constraint Type
	IndexConstraintType constraint_type;
	//! The table to create the index on
	unique_ptr<BaseTableRef> table;
	//! Set of expressions to index by
	vector<unique_ptr<ParsedExpression>> expressions;
	vector<unique_ptr<ParsedExpression>> parsed_expressions;

	vector<idx_t> column_ids;

public:
	unique_ptr<CreateInfo> Copy() const override {
		auto result = make_unique<CreateIndexInfo>();
		CopyProperties(*result);
		result->index_type = index_type;
		result->index_name = index_name;
		result->constraint_type = constraint_type;
		result->table = unique_ptr_cast<TableRef, BaseTableRef>(table->Copy());
		for (auto &expr : expressions) {
			result->expressions.push_back(expr->Copy());
		}
		result->column_ids = column_ids;
		return move(result);
	}

protected:
	void SerializeInternal(Serializer &serializer) const override {
		FieldWriter writer(serializer);
		writer.WriteField(index_type);
		writer.WriteString(index_name);
		writer.WriteField(constraint_type);
		writer.WriteSerializableList<ParsedExpression>(expressions);
		writer.WriteSerializableList<ParsedExpression>(parsed_expressions);
		table->Serialize(writer);
		writer.Finalize();
	}

public:
	static unique_ptr<CreateIndexInfo> Deserialize(Deserializer &deserializer) {
		auto result = make_unique<CreateIndexInfo>();
		// result->DeserializeBase(deserializer);

		FieldReader reader(deserializer);
		result->index_type = reader.ReadRequired<IndexType>();
		// TODO: something is wrong below
		result->index_name = reader.ReadRequired<std::string>();
		result->constraint_type = reader.ReadRequired<IndexConstraintType>();
		// TODO: something is wrong below
		result->expressions = reader.ReadRequiredSerializableList<ParsedExpression>();
		result->parsed_expressions = reader.ReadRequiredSerializableList<ParsedExpression>();

		// TODO(stephwang): review below for unique_ptr<BaseTableRef> table
		unique_ptr<TableRef> table;
		table = BaseTableRef::Deserialize(reader);
		result->table = unique_ptr_cast<TableRef, BaseTableRef>(move(table));

		reader.Finalize();

		return result;
	}
};

} // namespace duckdb
