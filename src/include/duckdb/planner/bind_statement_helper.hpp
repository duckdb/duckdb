//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/planner/bind_statement_helper.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/vector.hpp"
#include "duckdb/parser/column_list.hpp"
#include "duckdb/parser/constraint.hpp"
#include "duckdb/parser/constraints/foreign_key_constraint.hpp"

namespace duckdb {

void FindForeignKeyIndexes(const ColumnList &columns, const vector<string> &names,vector<PhysicalIndex> &indexes);
void FindMatchingPrimaryKeyColumns(const ColumnList &columns, const vector<unique_ptr<Constraint>> &constraints, ForeignKeyConstraint &fk);
void CheckForeignKeyTypes(const ColumnList &pk_columns, const ColumnList &fk_columns, ForeignKeyConstraint &fk);
void ExpressionContainsGeneratedColumn(const ParsedExpression &root_expr, const unordered_set<string> &gcols,bool &contains_gcol);
bool AnyConstraintReferencesGeneratedColumn(CreateTableInfo &table_info);
} // namespace duckdb
