#pragma once
#include "duckdb/planner/logical_operator_visitor.hpp"
#include "duckdb/planner/column_binding.hpp"
#include "duckdb/planner/column_binding_map.hpp"
#include "duckdb/common/identifier.hpp"
#include "duckdb/common/unordered_map.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/unordered_set.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"

namespace duckdb {

struct ForeignKeyReference {
	Identifier referenced_schema;
	Identifier referenced_table;
	column_binding_set_t fk_columns;

	ForeignKeyReference() = default;
	ForeignKeyReference(Identifier schema, Identifier table, column_binding_set_t cols)
	    : referenced_schema(std::move(schema)), referenced_table(std::move(table)), fk_columns(std::move(cols)) {
	}
};

struct ConstraintProperties {
	vector<column_binding_set_t> unique_sets;
	vector<ForeignKeyReference> foreign_keys;
	optional_ptr<TableCatalogEntry> base_table = nullptr;

	bool IsUnique(const vector<ColumnBinding> &cols) const;
	bool IsForeignKey(const vector<ColumnBinding> &cols, const Identifier &referenced_table) const;
};

class ConstraintPropagator : public LogicalOperatorVisitor {
public:
	unordered_map<TableIndex, ConstraintProperties> properties_map;

	void VisitOperator(LogicalOperator &op) override;

	bool IsJoinKeyUnique(const vector<ColumnBinding> &join_keys);
	bool IsForeignKey(const vector<ColumnBinding> &join_keys, const Identifier &referenced_table);
};

} // namespace duckdb
