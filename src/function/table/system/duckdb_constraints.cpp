#include "duckdb/function/table/system_functions.hpp"

#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_entry/schema_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/client_data.hpp"
#include "duckdb/parser/constraint.hpp"
#include "duckdb/parser/constraints/check_constraint.hpp"
#include "duckdb/parser/constraints/unique_constraint.hpp"
#include "duckdb/planner/constraints/bound_unique_constraint.hpp"
#include "duckdb/planner/constraints/bound_check_constraint.hpp"
#include "duckdb/planner/constraints/bound_not_null_constraint.hpp"
#include "duckdb/planner/constraints/bound_foreign_key_constraint.hpp"
#include "duckdb/storage/data_table.hpp"

namespace duckdb {

struct DuckDBConstraintsData : public GlobalTableFunctionState {
	DuckDBConstraintsData() : offset(0), constraint_offset(0) {
	}

	vector<CatalogEntry *> entries;
	idx_t offset;
	idx_t constraint_offset;
};

static unique_ptr<FunctionData> DuckDBConstraintsBind(ClientContext &context, TableFunctionBindInput &input,
                                                      vector<LogicalType> &return_types, vector<string> &names) {
	names.emplace_back("schema_name");
	return_types.emplace_back(LogicalType::VARCHAR);

	names.emplace_back("schema_oid");
	return_types.emplace_back(LogicalType::BIGINT);

	names.emplace_back("table_name");
	return_types.emplace_back(LogicalType::VARCHAR);

	names.emplace_back("table_oid");
	return_types.emplace_back(LogicalType::BIGINT);

	names.emplace_back("constraint_index");
	return_types.emplace_back(LogicalType::BIGINT);

	// CHECK, PRIMARY KEY or UNIQUE
	names.emplace_back("constraint_type");
	return_types.emplace_back(LogicalType::VARCHAR);

	names.emplace_back("constraint_text");
	return_types.emplace_back(LogicalType::VARCHAR);

	names.emplace_back("expression");
	return_types.emplace_back(LogicalType::VARCHAR);

	names.emplace_back("constraint_column_indexes");
	;
	return_types.push_back(LogicalType::LIST(LogicalType::BIGINT));

	names.emplace_back("constraint_column_names");
	return_types.push_back(LogicalType::LIST(LogicalType::VARCHAR));

	return nullptr;
}

unique_ptr<GlobalTableFunctionState> DuckDBConstraintsInit(ClientContext &context, TableFunctionInitInput &input) {
	auto result = make_unique<DuckDBConstraintsData>();

	// scan all the schemas for tables and collect themand collect them
	auto schemas = Catalog::GetCatalog(context).schemas->GetEntries<SchemaCatalogEntry>(context);
	for (auto &schema : schemas) {
		schema->Scan(context, CatalogType::TABLE_ENTRY, [&](CatalogEntry *entry) { result->entries.push_back(entry); });
	};

	// check the temp schema as well
	ClientData::Get(context).temporary_objects->Scan(context, CatalogType::TABLE_ENTRY,
	                                                 [&](CatalogEntry *entry) { result->entries.push_back(entry); });
	return move(result);
}

void DuckDBConstraintsFunction(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &data = (DuckDBConstraintsData &)*data_p.global_state;
	if (data.offset >= data.entries.size()) {
		// finished returning values
		return;
	}
	// start returning values
	// either fill up the chunk or return all the remaining columns
	idx_t count = 0;
	while (data.offset < data.entries.size() && count < STANDARD_VECTOR_SIZE) {
		auto &entry = data.entries[data.offset];

		if (entry->type != CatalogType::TABLE_ENTRY) {
			data.offset++;
			continue;
		}

		auto &table = (TableCatalogEntry &)*entry;
		for (; data.constraint_offset < table.constraints.size() && count < STANDARD_VECTOR_SIZE;
		     data.constraint_offset++) {
			auto &constraint = table.constraints[data.constraint_offset];
			// return values:
			// schema_name, LogicalType::VARCHAR
			output.SetValue(0, count, Value(table.schema->name));
			// schema_oid, LogicalType::BIGINT
			output.SetValue(1, count, Value::BIGINT(table.schema->oid));
			// table_name, LogicalType::VARCHAR
			output.SetValue(2, count, Value(table.name));
			// table_oid, LogicalType::BIGINT
			output.SetValue(3, count, Value::BIGINT(table.oid));

			// constraint_index, BIGINT
			output.SetValue(4, count, Value::BIGINT(data.constraint_offset));

			// constraint_type, VARCHAR
			string constraint_type;
			switch (constraint->type) {
			case ConstraintType::CHECK:
				constraint_type = "CHECK";
				break;
			case ConstraintType::UNIQUE: {
				auto &unique = (UniqueConstraint &)*constraint;
				constraint_type = unique.is_primary_key ? "PRIMARY KEY" : "UNIQUE";
				break;
			}
			case ConstraintType::NOT_NULL:
				constraint_type = "NOT NULL";
				break;
			case ConstraintType::FOREIGN_KEY:
				constraint_type = "FOREIGN KEY";
				break;
			default:
				throw NotImplementedException("Unimplemented constraint for duckdb_constraints");
			}
			output.SetValue(5, count, Value(constraint_type));

			// constraint_text, VARCHAR
			output.SetValue(6, count, Value(constraint->ToString()));

			// expression, VARCHAR
			Value expression_text;
			if (constraint->type == ConstraintType::CHECK) {
				auto &check = (CheckConstraint &)*constraint;
				expression_text = Value(check.expression->ToString());
			}
			output.SetValue(7, count, expression_text);

			auto &bound_constraint = (BoundConstraint &)*table.bound_constraints[data.constraint_offset];
			vector<column_t> column_index_list;
			switch (bound_constraint.type) {
			case ConstraintType::CHECK: {
				auto &bound_check = (BoundCheckConstraint &)bound_constraint;
				for (auto &col_idx : bound_check.bound_columns) {
					column_index_list.push_back(col_idx);
				}
				break;
			}
			case ConstraintType::UNIQUE: {
				auto &bound_unique = (BoundUniqueConstraint &)bound_constraint;
				for (auto &col_idx : bound_unique.keys) {
					column_index_list.push_back(column_t(col_idx));
				}
				break;
			}
			case ConstraintType::NOT_NULL: {
				auto &bound_not_null = (BoundNotNullConstraint &)bound_constraint;
				column_index_list.push_back(bound_not_null.index);
				break;
			}
			case ConstraintType::FOREIGN_KEY: {
				auto &bound_foreign_key = (BoundForeignKeyConstraint &)bound_constraint;
				for (auto &col_idx : bound_foreign_key.info.fk_keys) {
					column_index_list.push_back(column_t(col_idx));
				}
				break;
			}
			default:
				throw NotImplementedException("Unimplemented constraint for duckdb_constraints");
			}

			vector<Value> index_list;
			vector<Value> column_name_list;
			for (auto column_index : column_index_list) {
				index_list.push_back(Value::BIGINT(column_index));
				column_name_list.emplace_back(table.columns[column_index].Name());
			}

			// constraint_column_indexes, LIST
			output.SetValue(8, count, Value::LIST(move(index_list)));

			// constraint_column_names, LIST
			output.SetValue(9, count, Value::LIST(move(column_name_list)));

			count++;
		}
		if (data.constraint_offset >= table.constraints.size()) {
			data.constraint_offset = 0;
			data.offset++;
		}
	}
	output.SetCardinality(count);
}

void DuckDBConstraintsFun::RegisterFunction(BuiltinFunctions &set) {
	set.AddFunction(TableFunction("duckdb_constraints", {}, DuckDBConstraintsFunction, DuckDBConstraintsBind,
	                              DuckDBConstraintsInit));
}

} // namespace duckdb
