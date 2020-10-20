#include "duckdb/function/table/sqlite_functions.hpp"

#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/view_catalog_entry.hpp"
#include "duckdb/planner/constraints/bound_not_null_constraint.hpp"
#include "duckdb/planner/constraints/bound_unique_constraint.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/limits.hpp"

#include <algorithm>

using namespace std;

namespace duckdb {

struct PragmaTableFunctionData : public TableFunctionData {
	PragmaTableFunctionData(CatalogEntry *entry_) : entry(entry_) {
	}

	CatalogEntry *entry;
};

struct PragmaTableOperatorData : public FunctionOperatorData {
	PragmaTableOperatorData() : offset(0) {
	}
	idx_t offset;
};

static unique_ptr<FunctionData> pragma_table_info_bind(ClientContext &context, vector<Value> &inputs,
                                                       unordered_map<string, Value> &named_parameters,
                                                       vector<LogicalType> &return_types, vector<string> &names) {
	names.push_back("cid");
	return_types.push_back(LogicalType::INTEGER);

	names.push_back("name");
	return_types.push_back(LogicalType::VARCHAR);

	names.push_back("type");
	return_types.push_back(LogicalType::VARCHAR);

	names.push_back("notnull");
	return_types.push_back(LogicalType::BOOLEAN);

	names.push_back("dflt_value");
	return_types.push_back(LogicalType::VARCHAR);

	names.push_back("pk");
	return_types.push_back(LogicalType::BOOLEAN);

	string schema, table_name;
	auto range_var = inputs[0].GetValue<string>();
	Catalog::ParseRangeVar(range_var, schema, table_name);

	// look up the table name in the catalog
	auto &catalog = Catalog::GetCatalog(context);
	auto entry = catalog.GetEntry(context, CatalogType::TABLE_ENTRY, schema, table_name);
	return make_unique<PragmaTableFunctionData>(entry);
}

unique_ptr<FunctionOperatorData> pragma_table_info_init(ClientContext &context, const FunctionData *bind_data,
                                                        vector<column_t> &column_ids, TableFilterSet *table_filters) {
	return make_unique<PragmaTableOperatorData>();
}

static void check_constraints(TableCatalogEntry *table, idx_t oid, bool &out_not_null, bool &out_pk) {
	out_not_null = false;
	out_pk = false;
	// check all constraints
	// FIXME: this is pretty inefficient, it probably doesn't matter
	for (auto &constraint : table->bound_constraints) {
		switch (constraint->type) {
		case ConstraintType::NOT_NULL: {
			auto &not_null = (BoundNotNullConstraint &)*constraint;
			if (not_null.index == oid) {
				out_not_null = true;
			}
			break;
		}
		case ConstraintType::UNIQUE: {
			auto &unique = (BoundUniqueConstraint &)*constraint;
			if (unique.is_primary_key && unique.keys.find(oid) != unique.keys.end()) {
				out_pk = true;
			}
			break;
		}
		default:
			break;
		}
	}
}

static void pragma_table_info_table(PragmaTableOperatorData &data, TableCatalogEntry *table, DataChunk &output) {
	if (data.offset >= table->columns.size()) {
		// finished returning values
		return;
	}
	// start returning values
	// either fill up the chunk or return all the remaining columns
	idx_t next = min(data.offset + STANDARD_VECTOR_SIZE, (idx_t)table->columns.size());
	output.SetCardinality(next - data.offset);

	for (idx_t i = data.offset; i < next; i++) {
		bool not_null, pk;
		auto index = i - data.offset;
		auto &column = table->columns[i];
		assert(column.oid < (idx_t)NumericLimits<int32_t>::Maximum());
		check_constraints(table, column.oid, not_null, pk);

		// return values:
		// "cid", PhysicalType::INT32
		output.SetValue(0, index, Value::INTEGER((int32_t)column.oid));
		// "name", PhysicalType::VARCHAR
		output.SetValue(1, index, Value(column.name));
		// "type", PhysicalType::VARCHAR
		output.SetValue(2, index, Value(column.type.ToString()));
		// "notnull", PhysicalType::BOOL
		output.SetValue(3, index, Value::BOOLEAN(not_null));
		// "dflt_value", PhysicalType::VARCHAR
		Value def_value = column.default_value ? Value(column.default_value->ToString()) : Value();
		output.SetValue(4, index, def_value);
		// "pk", PhysicalType::BOOL
		output.SetValue(5, index, Value::BOOLEAN(pk));
	}
	data.offset = next;
}

static void pragma_table_info_view(PragmaTableOperatorData &data, ViewCatalogEntry *view, DataChunk &output) {
	if (data.offset >= view->types.size()) {
		// finished returning values
		return;
	}
	// start returning values
	// either fill up the chunk or return all the remaining columns
	idx_t next = min(data.offset + STANDARD_VECTOR_SIZE, (idx_t)view->types.size());
	output.SetCardinality(next - data.offset);

	for (idx_t i = data.offset; i < next; i++) {
		auto index = i - data.offset;
		auto type = view->types[index];
		auto &name = view->aliases[index];
		// return values:
		// "cid", PhysicalType::INT32

		output.SetValue(0, index, Value::INTEGER((int32_t)index));
		// "name", PhysicalType::VARCHAR
		output.SetValue(1, index, Value(name));
		// "type", PhysicalType::VARCHAR
		output.SetValue(2, index, Value(type.ToString()));
		// "notnull", PhysicalType::BOOL
		output.SetValue(3, index, Value::BOOLEAN(false));
		// "dflt_value", PhysicalType::VARCHAR
		output.SetValue(4, index, Value());
		// "pk", PhysicalType::BOOL
		output.SetValue(5, index, Value::BOOLEAN(false));
	}
	data.offset = next;
}

static void pragma_table_info(ClientContext &context, const FunctionData *bind_data_,
                              FunctionOperatorData *operator_state, DataChunk &output) {
	auto &bind_data = (PragmaTableFunctionData &)*bind_data_;
	auto &state = (PragmaTableOperatorData &)*operator_state;
	switch (bind_data.entry->type) {
	case CatalogType::TABLE_ENTRY:
		pragma_table_info_table(state, (TableCatalogEntry *)bind_data.entry, output);
		break;
	case CatalogType::VIEW_ENTRY:
		pragma_table_info_view(state, (ViewCatalogEntry *)bind_data.entry, output);
		break;
	default:
		throw NotImplementedException("Unimplemented catalog type for pragma_table_info");
	}
}

void PragmaTableInfo::RegisterFunction(BuiltinFunctions &set) {
	set.AddFunction(TableFunction("pragma_table_info", {LogicalType::VARCHAR}, pragma_table_info,
	                              pragma_table_info_bind, pragma_table_info_init));
}

} // namespace duckdb
