#include "duckdb/function/table/system_functions.hpp"

#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/view_catalog_entry.hpp"
#include "duckdb/parser/qualified_name.hpp"
#include "duckdb/planner/constraints/bound_not_null_constraint.hpp"
#include "duckdb/planner/constraints/bound_unique_constraint.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/limits.hpp"

#include <algorithm>

namespace duckdb {

struct PragmaTableFunctionData : public TableFunctionData {
	explicit PragmaTableFunctionData(CatalogEntry *entry_p) : entry(entry_p) {
	}

	CatalogEntry *entry;
};

struct PragmaTableOperatorData : public GlobalTableFunctionState {
	PragmaTableOperatorData() : offset(0) {
	}
	idx_t offset;
};

static unique_ptr<FunctionData> PragmaTableInfoBind(ClientContext &context, TableFunctionBindInput &input,
                                                    vector<LogicalType> &return_types, vector<string> &names) {
	names.emplace_back("cid");
	return_types.emplace_back(LogicalType::INTEGER);

	names.emplace_back("name");
	return_types.emplace_back(LogicalType::VARCHAR);

	names.emplace_back("type");
	return_types.emplace_back(LogicalType::VARCHAR);

	names.emplace_back("notnull");
	return_types.emplace_back(LogicalType::BOOLEAN);

	names.emplace_back("dflt_value");
	return_types.emplace_back(LogicalType::VARCHAR);

	names.emplace_back("pk");
	return_types.emplace_back(LogicalType::BOOLEAN);

	auto qname = QualifiedName::Parse(input.inputs[0].GetValue<string>());

	// look up the table name in the catalog
	auto &catalog = Catalog::GetCatalog(context);
	auto entry = catalog.GetEntry(context, CatalogType::TABLE_ENTRY, qname.schema, qname.name);
	return make_unique<PragmaTableFunctionData>(entry);
}

unique_ptr<GlobalTableFunctionState> PragmaTableInfoInit(ClientContext &context, TableFunctionInitInput &input) {
	return make_unique<PragmaTableOperatorData>();
}

static void CheckConstraints(TableCatalogEntry *table, idx_t oid, bool &out_not_null, bool &out_pk) {
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
			if (unique.is_primary_key && unique.key_set.find(oid) != unique.key_set.end()) {
				out_pk = true;
			}
			break;
		}
		default:
			break;
		}
	}
}

static void PragmaTableInfoTable(PragmaTableOperatorData &data, TableCatalogEntry *table, DataChunk &output) {
	if (data.offset >= table->columns.size()) {
		// finished returning values
		return;
	}
	// start returning values
	// either fill up the chunk or return all the remaining columns
	idx_t next = MinValue<idx_t>(data.offset + STANDARD_VECTOR_SIZE, table->columns.size());
	output.SetCardinality(next - data.offset);

	for (idx_t i = data.offset; i < next; i++) {
		bool not_null, pk;
		auto index = i - data.offset;
		auto &column = table->columns[i];
		D_ASSERT(column.Oid() < (idx_t)NumericLimits<int32_t>::Maximum());
		CheckConstraints(table, column.Oid(), not_null, pk);

		// return values:
		// "cid", PhysicalType::INT32
		output.SetValue(0, index, Value::INTEGER((int32_t)column.Oid()));
		// "name", PhysicalType::VARCHAR
		output.SetValue(1, index, Value(column.Name()));
		// "type", PhysicalType::VARCHAR
		output.SetValue(2, index, Value(column.Type().ToString()));
		// "notnull", PhysicalType::BOOL
		output.SetValue(3, index, Value::BOOLEAN(not_null));
		// "dflt_value", PhysicalType::VARCHAR
		Value def_value = column.DefaultValue() ? Value(column.DefaultValue()->ToString()) : Value();
		output.SetValue(4, index, def_value);
		// "pk", PhysicalType::BOOL
		output.SetValue(5, index, Value::BOOLEAN(pk));
	}
	data.offset = next;
}

static void PragmaTableInfoView(PragmaTableOperatorData &data, ViewCatalogEntry *view, DataChunk &output) {
	if (data.offset >= view->types.size()) {
		// finished returning values
		return;
	}
	// start returning values
	// either fill up the chunk or return all the remaining columns
	idx_t next = MinValue<idx_t>(data.offset + STANDARD_VECTOR_SIZE, view->types.size());
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

static void PragmaTableInfoFunction(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &bind_data = (PragmaTableFunctionData &)*data_p.bind_data;
	auto &state = (PragmaTableOperatorData &)*data_p.global_state;
	switch (bind_data.entry->type) {
	case CatalogType::TABLE_ENTRY:
		PragmaTableInfoTable(state, (TableCatalogEntry *)bind_data.entry, output);
		break;
	case CatalogType::VIEW_ENTRY:
		PragmaTableInfoView(state, (ViewCatalogEntry *)bind_data.entry, output);
		break;
	default:
		throw NotImplementedException("Unimplemented catalog type for pragma_table_info");
	}
}

void PragmaTableInfo::RegisterFunction(BuiltinFunctions &set) {
	set.AddFunction(TableFunction("pragma_table_info", {LogicalType::VARCHAR}, PragmaTableInfoFunction,
	                              PragmaTableInfoBind, PragmaTableInfoInit));
}

} // namespace duckdb
