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
};

} // namespace duckdb
