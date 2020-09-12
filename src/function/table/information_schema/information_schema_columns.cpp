#include "duckdb/function/table/information_schema_functions.hpp"

#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/view_catalog_entry.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/transaction/transaction.hpp"

using namespace std;

namespace duckdb {

struct InformationSchemaColumnsData : public FunctionOperatorData {
	InformationSchemaColumnsData() : offset(0), column_offset(0) {
	}

	vector<CatalogEntry *> entries;
	idx_t offset;
	idx_t column_offset;
};

static unique_ptr<FunctionData> information_schema_columns_bind(ClientContext &context, vector<Value> &inputs,
                                                                unordered_map<string, Value> &named_parameters,
                                                                vector<LogicalType> &return_types,
                                                                vector<string> &names) {
	names.push_back("table_catalog");
	return_types.push_back(LogicalType::VARCHAR);

	names.push_back("table_schema");
	return_types.push_back(LogicalType::VARCHAR);

	names.push_back("table_name");
	return_types.push_back(LogicalType::VARCHAR);

	names.push_back("column_name");
	return_types.push_back(LogicalType::VARCHAR);

	names.push_back("ordinal_position");
	return_types.push_back(LogicalType::INTEGER);

	names.push_back("column_default");
	return_types.push_back(LogicalType::VARCHAR);

	names.push_back("is_nullable"); // YES/NO
	return_types.push_back(LogicalType::VARCHAR);

	names.push_back("data_type");
	return_types.push_back(LogicalType::VARCHAR);

	names.push_back("character_maximum_length");
	return_types.push_back(LogicalType::INTEGER);

	names.push_back("character_octet_length");
	return_types.push_back(LogicalType::INTEGER);

	names.push_back("numeric_precision");
	return_types.push_back(LogicalType::INTEGER);

	names.push_back("numeric_scale");
	return_types.push_back(LogicalType::INTEGER);

	names.push_back("datetime_precision");
	return_types.push_back(LogicalType::INTEGER);

	names.push_back("character_set_catalog");
	return_types.push_back(LogicalType::VARCHAR);

	names.push_back("character_set_schema");
	return_types.push_back(LogicalType::VARCHAR);

	names.push_back("character_set_name");
	return_types.push_back(LogicalType::VARCHAR);

	names.push_back("collation_catalog");
	return_types.push_back(LogicalType::VARCHAR);

	names.push_back("collation_schema");
	return_types.push_back(LogicalType::VARCHAR);

	names.push_back("collation_name");
	return_types.push_back(LogicalType::VARCHAR);

	names.push_back("is_updatable");
	return_types.push_back(LogicalType::VARCHAR);

	return nullptr;
}

unique_ptr<FunctionOperatorData>
information_schema_columns_init(ClientContext &context, const FunctionData *bind_data, ParallelState *state,
                                vector<column_t> &column_ids,
                                unordered_map<idx_t, vector<TableFilter>> &table_filters) {
	auto result = make_unique<InformationSchemaColumnsData>();

	// scan all the schemas for tables and views and collect them
	auto &transaction = Transaction::GetTransaction(context);
	Catalog::GetCatalog(context).schemas->Scan(transaction, [&](CatalogEntry *entry) {
		auto schema = (SchemaCatalogEntry *)entry;
		schema->tables.Scan(transaction, [&](CatalogEntry *entry) { result->entries.push_back(entry); });
	});

	// check the temp schema as well
	context.temporary_objects->tables.Scan(transaction, [&](CatalogEntry *entry) { result->entries.push_back(entry); });
	return move(result);
}

namespace { // anonymous namespace for the ColumnHelper classes for working with tables/views

class ColumnHelper {
public:
	static unique_ptr<ColumnHelper> Create(CatalogEntry *entry);

	virtual ~ColumnHelper() {
	}

	virtual StandardEntry *Entry() = 0;
	virtual bool IsUpdatable() = 0;
	virtual idx_t NumColumns() = 0;
	virtual const string &ColumnName(idx_t col) = 0;
	virtual const LogicalType &ColumnType(idx_t col) = 0;
	virtual const char *ColumnDefault(idx_t col) = 0;

	void WriteColumns(idx_t index, idx_t start_col, idx_t end_col, DataChunk &output);
};

class TableColumnHelper : public ColumnHelper {
public:
	TableColumnHelper(TableCatalogEntry *entry) : entry(entry) {}

	StandardEntry *Entry() {
		return entry;
	}
	bool IsUpdatable() {
		return true;
	}
	idx_t NumColumns() {
		return entry->columns.size();
	}
	const string &ColumnName(idx_t col) {
		return entry->columns[col].name;
	}
	const LogicalType &ColumnType(idx_t col) {
		return entry->columns[col].type;
	}
	const char *ColumnDefault(idx_t col) {
		if (entry->columns[col].default_value) {
			return entry->columns[col].default_value->ToString().c_str();
		}
		return nullptr;
	}

private:
	TableCatalogEntry *entry;
};

class ViewColumnHelper : public ColumnHelper {
public:
	ViewColumnHelper(ViewCatalogEntry *entry) : entry(entry) {
	}

	StandardEntry *Entry() {
		return entry;
	}
	bool IsUpdatable() {
		return false;
	}
	idx_t NumColumns() {
		return entry->types.size();
	}
	const string &ColumnName(idx_t col) {
		return entry->aliases[col];
	}
	const LogicalType &ColumnType(idx_t col) {
		return entry->types[col];
	}
	const char *ColumnDefault(idx_t col) {
		return nullptr;
	}

private:
	ViewCatalogEntry *entry;
};

unique_ptr<ColumnHelper> ColumnHelper::Create(CatalogEntry *entry) {
	switch (entry->type) {
	case CatalogType::TABLE_ENTRY:
		return make_unique<TableColumnHelper>((TableCatalogEntry *)entry);
	case CatalogType::VIEW_ENTRY:
		return make_unique<ViewColumnHelper>((ViewCatalogEntry *)entry);
	default:
		throw new NotImplementedException("Unsupported catalog type for information_schema_columns");
	}
}

void ColumnHelper::WriteColumns(idx_t start_index, idx_t start_col, idx_t end_col, DataChunk &output) {
	for (idx_t i = start_col; i < end_col; i++) {
		auto index = start_index + (i - start_col);
		// "table_catalog", PhysicalType::VARCHAR
		output.SetValue(0, index, Value());
		// "table_schema", PhysicalType::VARCHAR
		output.SetValue(1, index, Value(Entry()->schema->name));
		// "table_name", PhysicalType::VARCHAR
		output.SetValue(2, index, Value(Entry()->name));
		// "column_name", PhysicalType::VARCHAR
		output.SetValue(3, index, Value(ColumnName(i)));
		// "ordinal_position", PhysicalType::INTEGER
		output.SetValue(4, index, Value::INTEGER(i + 1));
		// "column_default", PhysicalType::VARCHAR
		output.SetValue(5, index, Value(ColumnDefault(i)));
		// "is_nullable", PhysicalType::VARCHAR YES/NO
		// FIXME: Need to check constraints here and in pragma_table_info
		output.SetValue(6, index, Value("YES"));

		// Columns 7 ("data_type") through 12 ("datetime_precision") are determined by the LogicalType
		// of the column.
		// "data_type", PhysicalType::VARCHAR
		output.SetValue(7, index, Value(ColumnType(i).ToString()));
		// "character_maximum_length", PhysicalType::INTEGER
		output.SetValue(8, index, Value());
		// "character_octet_length", PhysicalType::INTEGER
		output.SetValue(9, index, Value());
		// "numeric_precision", PhysicalType::INTEGER
		output.SetValue(10, index, Value());
		// "numeric_scale", PhysicalType::INTEGER
		output.SetValue(11, index, Value());
		// "datetime_precision", PhysicalType::INTEGER
		output.SetValue(12, index, Value());

		// Columns 13 ("character_set_catalog") through 18 ("collation_name") are always NULL for DuckDB.
		for (int value_index = 13; value_index < 19; value_index++) {
			output.SetValue(value_index, index, Value());
		}

		output.SetValue(19, index, IsUpdatable() ? Value("YES") : Value("NO"));
	}
}

} // anonymous namespace

void information_schema_columns(ClientContext &context, const FunctionData *bind_data,
                                FunctionOperatorData *operator_state, DataChunk &output) {
	auto &data = (InformationSchemaColumnsData &)*operator_state;
	if (data.offset >= data.entries.size()) {
		// finished returning values
		return;
	}

	// We need to track the offset of the relation we're writing as well as the last column
	// we wrote from that relation (if any); it's possible that we can fill up the output
	// with a partial list of columns from a relation and will need to pick up processing the
	// next chunk at the same spot.
	idx_t next = data.offset;
	idx_t column_offset = data.column_offset;
	idx_t index = 0;
	while (next < data.entries.size() && index < STANDARD_VECTOR_SIZE) {
		auto column_helper = ColumnHelper::Create(data.entries[next]);
		idx_t columns = column_helper->NumColumns();

		// Check to see if we are going to exceed the maximum index for a DataChunk
		if (index + (columns - column_offset) > STANDARD_VECTOR_SIZE) {
			idx_t column_limit = column_offset + (STANDARD_VECTOR_SIZE - index);
			output.SetCardinality(STANDARD_VECTOR_SIZE);
			column_helper->WriteColumns(index, column_offset, column_limit, output);

			// Make the current column limit the column offset when we process the next chunk
			column_offset = column_limit;
			break;
		} else {
			// Otherwise, write all of the columns from the current relation and
			// then move on to the next one.
			output.SetCardinality(index + (columns - column_offset));
			column_helper->WriteColumns(index, column_offset, columns, output);
			index += columns - column_offset;
			next++;
			column_offset = 0;
		}
	}
	data.offset = next;
	data.column_offset = column_offset;
}

void InformationSchemaColumns::RegisterFunction(BuiltinFunctions &set) {
	set.AddFunction(TableFunction("information_schema_columns", {}, information_schema_columns,
	                              information_schema_columns_bind, information_schema_columns_init));
}

} // namespace duckdb
