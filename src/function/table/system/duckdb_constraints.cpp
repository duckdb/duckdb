#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_entry/duck_table_entry.hpp"
#include "duckdb/catalog/catalog_entry/schema_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/function/table/system_functions.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/client_data.hpp"
#include "duckdb/parser/constraint.hpp"
#include "duckdb/parser/constraints/check_constraint.hpp"
#include "duckdb/parser/constraints/foreign_key_constraint.hpp"
#include "duckdb/parser/constraints/not_null_constraint.hpp"
#include "duckdb/parser/constraints/unique_constraint.hpp"
#include "duckdb/planner/binder.hpp"
#include "duckdb/planner/constraints/bound_check_constraint.hpp"
#include "duckdb/parser/parsed_expression_iterator.hpp"

namespace duckdb {

struct ConstraintEntry {
	ConstraintEntry(ClientContext &context, TableCatalogEntry &table) : table(table) {
		if (!table.IsDuckTable()) {
			return;
		}
		auto binder = Binder::CreateBinder(context);
		bound_constraints = binder->BindConstraints(table.GetConstraints(), table.name, table.GetColumns());
	}

	TableCatalogEntry &table;
	vector<unique_ptr<BoundConstraint>> bound_constraints;
};

struct DuckDBConstraintsData : public GlobalTableFunctionState {
	DuckDBConstraintsData() : offset(0), constraint_offset(0), unique_constraint_offset(0) {
	}

	vector<ConstraintEntry> entries;
	idx_t offset;
	idx_t constraint_offset;
	idx_t unique_constraint_offset;
	case_insensitive_set_t constraint_names;
};

static unique_ptr<FunctionData> DuckDBConstraintsBind(ClientContext &context, TableFunctionBindInput &input,
                                                      vector<LogicalType> &return_types, vector<string> &names) {
	names.emplace_back("database_name");
	return_types.emplace_back(LogicalType::VARCHAR);

	names.emplace_back("database_oid");
	return_types.emplace_back(LogicalType::BIGINT);

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
	return_types.push_back(LogicalType::LIST(LogicalType::BIGINT));

	names.emplace_back("constraint_column_names");
	return_types.push_back(LogicalType::LIST(LogicalType::VARCHAR));

	names.emplace_back("constraint_name");
	return_types.emplace_back(LogicalType::VARCHAR);

	// FOREIGN KEY
	names.emplace_back("referenced_table");
	return_types.emplace_back(LogicalType::VARCHAR);

	names.emplace_back("referenced_column_names");
	return_types.push_back(LogicalType::LIST(LogicalType::VARCHAR));

	return nullptr;
}

unique_ptr<GlobalTableFunctionState> DuckDBConstraintsInit(ClientContext &context, TableFunctionInitInput &input) {
	auto result = make_uniq<DuckDBConstraintsData>();

	// scan all the schemas for tables and collect them
	auto schemas = Catalog::GetAllSchemas(context);

	for (auto &schema : schemas) {
		vector<reference<CatalogEntry>> entries;

		schema.get().Scan(context, CatalogType::TABLE_ENTRY, [&](CatalogEntry &entry) {
			if (entry.type == CatalogType::TABLE_ENTRY) {
				entries.push_back(entry);
			}
		});

		sort(entries.begin(), entries.end(), [&](CatalogEntry &x, CatalogEntry &y) { return (x.name < y.name); });
		for (auto &entry : entries) {
			result->entries.emplace_back(context, entry.get().Cast<TableCatalogEntry>());
		}
	};

	return std::move(result);
}

struct ExtraConstraintInfo {
	vector<LogicalIndex> column_indexes;
	vector<string> column_names;
	string referenced_table;
	vector<string> referenced_columns;
};

void ExtractReferencedColumns(const ParsedExpression &expr, vector<string> &result) {
	if (expr.GetExpressionClass() == ExpressionClass::COLUMN_REF) {
		auto &colref = expr.Cast<ColumnRefExpression>();
		result.push_back(colref.GetColumnName());
	}
	ParsedExpressionIterator::EnumerateChildren(
	    expr, [&](const ParsedExpression &child) { ExtractReferencedColumns(child, result); });
}

ExtraConstraintInfo GetExtraConstraintInfo(const TableCatalogEntry &table, const Constraint &constraint) {
	ExtraConstraintInfo result;
	switch (constraint.type) {
	case ConstraintType::CHECK: {
		auto &check_constraint = constraint.Cast<CheckConstraint>();
		ExtractReferencedColumns(*check_constraint.expression, result.column_names);
		break;
	}
	case ConstraintType::NOT_NULL: {
		auto &not_null_constraint = constraint.Cast<NotNullConstraint>();
		result.column_indexes.push_back(not_null_constraint.index);
		break;
	}
	case ConstraintType::UNIQUE: {
		auto &unique = constraint.Cast<UniqueConstraint>();
		if (unique.HasIndex()) {
			result.column_indexes.push_back(unique.GetIndex());
		} else {
			result.column_names = unique.GetColumnNames();
		}
		break;
	}
	case ConstraintType::FOREIGN_KEY: {
		auto &fk = constraint.Cast<ForeignKeyConstraint>();
		result.referenced_columns = fk.pk_columns;
		result.referenced_table = fk.info.table;
		result.column_names = fk.fk_columns;
		break;
	}
	default:
		throw InternalException("Unsupported type for constraint name");
	}
	if (result.column_indexes.empty()) {
		// generate column indexes from names
		for (auto &name : result.column_names) {
			result.column_indexes.push_back(table.GetColumnIndex(name));
		}
	} else {
		// generate names from column indexes
		for (auto &index : result.column_indexes) {
			result.column_names.push_back(table.GetColumn(index).GetName());
		}
	}
	return result;
}

string GetConstraintName(const TableCatalogEntry &table, Constraint &constraint, const ExtraConstraintInfo &info) {
	string result = table.name + "_";
	for (auto &col : info.column_names) {
		result += StringUtil::Lower(col) + "_";
	}
	for (auto &col : info.referenced_columns) {
		result += StringUtil::Lower(col) + "_";
	}
	switch (constraint.type) {
	case ConstraintType::CHECK:
		result += "check";
		break;
	case ConstraintType::NOT_NULL:
		result += "not_null";
		break;
	case ConstraintType::UNIQUE: {
		auto &unique = constraint.Cast<UniqueConstraint>();
		result += unique.IsPrimaryKey() ? "pkey" : "key";
		break;
	}
	case ConstraintType::FOREIGN_KEY:
		result += "fkey";
		break;
	default:
		throw InternalException("Unsupported type for constraint name");
	}
	return result;
}

void DuckDBConstraintsFunction(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &data = data_p.global_state->Cast<DuckDBConstraintsData>();
	if (data.offset >= data.entries.size()) {
		// finished returning values
		return;
	}
	// start returning values
	// either fill up the chunk or return all the remaining columns
	idx_t count = 0;
	while (data.offset < data.entries.size() && count < STANDARD_VECTOR_SIZE) {
		auto &entry = data.entries[data.offset];

		auto &table = entry.table;
		auto &constraints = table.GetConstraints();
		for (; data.constraint_offset < constraints.size() && count < STANDARD_VECTOR_SIZE; data.constraint_offset++) {
			auto &constraint = constraints[data.constraint_offset];
			// return values:
			// constraint_type, VARCHAR
			// Processing this first due to shortcut (early continue)
			string constraint_type;
			switch (constraint->type) {
			case ConstraintType::CHECK:
				constraint_type = "CHECK";
				break;
			case ConstraintType::UNIQUE: {
				auto &unique = constraint->Cast<UniqueConstraint>();
				constraint_type = unique.IsPrimaryKey() ? "PRIMARY KEY" : "UNIQUE";
				break;
			}
			case ConstraintType::NOT_NULL:
				constraint_type = "NOT NULL";
				break;
			case ConstraintType::FOREIGN_KEY: {
				auto &fk = constraint->Cast<ForeignKeyConstraint>();
				if (fk.info.type == ForeignKeyType::FK_TYPE_PRIMARY_KEY_TABLE) {
					// Those are already covered by PRIMARY KEY and UNIQUE entries
					continue;
				}
				constraint_type = "FOREIGN KEY";
				break;
			}
			default:
				throw NotImplementedException("Unimplemented constraint for duckdb_constraints");
			}

			idx_t col = 0;
			// database_name, LogicalType::VARCHAR
			output.SetValue(col++, count, Value(table.schema.catalog.GetName()));
			// database_oid, LogicalType::BIGINT
			output.SetValue(col++, count, Value::BIGINT(NumericCast<int64_t>(table.schema.catalog.GetOid())));
			// schema_name, LogicalType::VARCHAR
			output.SetValue(col++, count, Value(table.schema.name));
			// schema_oid, LogicalType::BIGINT
			output.SetValue(col++, count, Value::BIGINT(NumericCast<int64_t>(table.schema.oid)));
			// table_name, LogicalType::VARCHAR
			output.SetValue(col++, count, Value(table.name));
			// table_oid, LogicalType::BIGINT
			output.SetValue(col++, count, Value::BIGINT(NumericCast<int64_t>(table.oid)));

			auto info = GetExtraConstraintInfo(table, *constraint);
			auto constraint_name = GetConstraintName(table, *constraint, info);
			if (data.constraint_names.find(constraint_name) != data.constraint_names.end()) {
				// duplicate constraint name
				idx_t index = 2;
				while (data.constraint_names.find(constraint_name + "_" + to_string(index)) !=
				       data.constraint_names.end()) {
					index++;
				}
				constraint_name += "_" + to_string(index);
			}
			// constraint_index, BIGINT
			output.SetValue(col++, count, Value::BIGINT(NumericCast<int64_t>(data.unique_constraint_offset++)));

			// constraint_type, VARCHAR
			output.SetValue(col++, count, Value(constraint_type));

			// constraint_text, VARCHAR
			output.SetValue(col++, count, Value(constraint->ToString()));

			// expression, VARCHAR
			Value expression_text;
			if (constraint->type == ConstraintType::CHECK) {
				auto &check = constraint->Cast<CheckConstraint>();
				expression_text = Value(check.expression->ToString());
			}
			output.SetValue(col++, count, expression_text);

			vector<Value> column_index_list;
			vector<Value> column_name_list;
			vector<Value> referenced_column_name_list;
			for (auto &col_index : info.column_indexes) {
				column_index_list.push_back(Value::UBIGINT(col_index.index));
			}
			for (auto &name : info.column_names) {
				column_name_list.push_back(Value(std::move(name)));
			}
			for (auto &name : info.referenced_columns) {
				referenced_column_name_list.push_back(Value(std::move(name)));
			}

			// constraint_column_indexes, LIST
			output.SetValue(col++, count, Value::LIST(LogicalType::BIGINT, std::move(column_index_list)));

			// constraint_column_names, LIST
			output.SetValue(col++, count, Value::LIST(LogicalType::VARCHAR, std::move(column_name_list)));

			// constraint_name, VARCHAR
			output.SetValue(col++, count, Value(std::move(constraint_name)));

			// referenced_table, VARCHAR
			output.SetValue(col++, count,
			                info.referenced_table.empty() ? Value() : Value(std::move(info.referenced_table)));

			// referenced_column_names, LIST
			output.SetValue(col++, count, Value::LIST(LogicalType::VARCHAR, std::move(referenced_column_name_list)));
			count++;
		}
		if (data.constraint_offset >= constraints.size()) {
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
