#include "duckdb/function/table/information_schema_functions.hpp"

#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/view_catalog_entry.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/parser/constraints/not_null_constraint.hpp"

#include <set>

namespace duckdb {

struct InformationSchemaColumnsData : public FunctionOperatorData {
	InformationSchemaColumnsData() : offset(0), column_offset(0) {
	}

	vector<CatalogEntry *> entries;
	idx_t offset;
	idx_t column_offset;
};

static unique_ptr<FunctionData> InformationSchemaColumnsBind(ClientContext &context, vector<Value> &inputs,
                                                             unordered_map<string, Value> &named_parameters,
                                                             vector<LogicalType> &input_table_types,
                                                             vector<string> &input_table_names,
                                                             vector<LogicalType> &return_types, vector<string> &names) {
	names.emplace_back("table_catalog");
	return_types.push_back(LogicalType::VARCHAR);

	names.emplace_back("table_schema");
	return_types.push_back(LogicalType::VARCHAR);

	names.emplace_back("table_name");
	return_types.push_back(LogicalType::VARCHAR);

	names.emplace_back("column_name");
	return_types.push_back(LogicalType::VARCHAR);

	names.emplace_back("ordinal_position");
	return_types.push_back(LogicalType::INTEGER);

	names.emplace_back("column_default");
	return_types.push_back(LogicalType::VARCHAR);

	names.emplace_back("is_nullable"); // YES/NO
	return_types.push_back(LogicalType::VARCHAR);

	names.emplace_back("data_type");
	return_types.push_back(LogicalType::VARCHAR);

	names.emplace_back("character_maximum_length");
	return_types.push_back(LogicalType::INTEGER);

	names.emplace_back("character_octet_length");
	return_types.push_back(LogicalType::INTEGER);

	names.emplace_back("numeric_precision");
	return_types.push_back(LogicalType::INTEGER);

	names.emplace_back("numeric_scale");
	return_types.push_back(LogicalType::INTEGER);

	names.emplace_back("datetime_precision");
	return_types.push_back(LogicalType::INTEGER);

	return nullptr;
}

unique_ptr<FunctionOperatorData> InformationSchemaColumnsInit(ClientContext &context, const FunctionData *bind_data,
                                                              vector<column_t> &column_ids,
                                                              TableFilterCollection *filters) {
	auto result = make_unique<InformationSchemaColumnsData>();

	// scan all the schemas for tables and views and collect them
	auto schemas = Catalog::GetCatalog(context).schemas->GetEntries<SchemaCatalogEntry>(context);
	for (auto &schema : schemas) {
		schema->Scan(context, CatalogType::TABLE_ENTRY, [&](CatalogEntry *entry) { result->entries.push_back(entry); });
	}

	// check the temp schema as well
	context.temporary_objects->Scan(context, CatalogType::TABLE_ENTRY,
	                                [&](CatalogEntry *entry) { result->entries.push_back(entry); });
	return move(result);
}

namespace { // anonymous namespace for the ColumnHelper classes for working with tables/views

class ColumnHelper {
public:
	static unique_ptr<ColumnHelper> Create(CatalogEntry *entry);

	virtual ~ColumnHelper() {
	}

	virtual StandardEntry *Entry() = 0;
	virtual idx_t NumColumns() = 0;
	virtual const string &ColumnName(idx_t col) = 0;
	virtual const LogicalType &ColumnType(idx_t col) = 0;
	virtual const Value ColumnDefault(idx_t col) = 0;
	virtual bool IsNullable(idx_t col) = 0;

	void WriteColumns(idx_t index, idx_t start_col, idx_t end_col, DataChunk &output);
};

class TableColumnHelper : public ColumnHelper {
public:
	explicit TableColumnHelper(TableCatalogEntry *entry) : entry(entry) {
		for (auto &constraint : entry->constraints) {
			if (constraint->type == ConstraintType::NOT_NULL) {
				auto &not_null = *reinterpret_cast<NotNullConstraint *>(constraint.get());
				not_null_cols.insert(not_null.index);
			}
		}
	}

	StandardEntry *Entry() override {
		return entry;
	}
	idx_t NumColumns() override {
		return entry->columns.size();
	}
	const string &ColumnName(idx_t col) override {
		return entry->columns[col].name;
	}
	const LogicalType &ColumnType(idx_t col) override {
		return entry->columns[col].type;
	}
	const Value ColumnDefault(idx_t col) override {
		if (entry->columns[col].default_value) {
			return Value(entry->columns[col].default_value->ToString());
		}
		return Value();
	}
	bool IsNullable(idx_t col) override {
		return not_null_cols.find(col) == not_null_cols.end();
	}

private:
	TableCatalogEntry *entry;
	std::set<idx_t> not_null_cols;
};

class ViewColumnHelper : public ColumnHelper {
public:
	explicit ViewColumnHelper(ViewCatalogEntry *entry) : entry(entry) {
	}

	StandardEntry *Entry() override {
		return entry;
	}
	idx_t NumColumns() override {
		return entry->types.size();
	}
	const string &ColumnName(idx_t col) override {
		return entry->aliases[col];
	}
	const LogicalType &ColumnType(idx_t col) override {
		return entry->types[col];
	}
	const Value ColumnDefault(idx_t col) override {
		return Value();
	}
	bool IsNullable(idx_t col) override {
		return true;
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
		throw NotImplementedException("Unsupported catalog type for information_schema_columns");
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
		output.SetValue(6, index, Value(IsNullable(i) ? "YES" : "NO"));

		// "data_type", PhysicalType::VARCHAR
		const LogicalType &type = ColumnType(i);
		output.SetValue(7, index, Value(type.ToString()));

		if (type == LogicalType::VARCHAR) {
			// FIXME: need check constraints in place to set this correctly
			// "character_maximum_length", PhysicalType::INTEGER
			output.SetValue(8, index, Value());
			// "character_octet_length", PhysicalType::INTEGER
			// FIXME: where did this number come from?
			output.SetValue(9, index, Value::INTEGER(1073741824));
		} else {
			// "character_maximum_length", PhysicalType::INTEGER
			output.SetValue(8, index, Value());
			// "character_octet_length", PhysicalType::INTEGER
			output.SetValue(9, index, Value());
		}

		Value numeric_precision, numeric_scale;
		switch (type.id()) {
		case LogicalTypeId::DECIMAL:
			numeric_precision = Value::INTEGER(type.width());
			numeric_scale = Value::INTEGER(type.scale());
			break;
		case LogicalTypeId::HUGEINT:
			numeric_precision = Value::INTEGER(128);
			numeric_scale = Value::INTEGER(0);
			break;
		case LogicalTypeId::BIGINT:
			numeric_precision = Value::INTEGER(64);
			numeric_scale = Value::INTEGER(0);
			break;
		case LogicalTypeId::INTEGER:
			numeric_precision = Value::INTEGER(32);
			numeric_scale = Value::INTEGER(0);
			break;
		case LogicalTypeId::SMALLINT:
			numeric_precision = Value::INTEGER(16);
			numeric_scale = Value::INTEGER(0);
			break;
		case LogicalTypeId::TINYINT:
			numeric_precision = Value::INTEGER(8);
			numeric_scale = Value::INTEGER(0);
			break;
		case LogicalTypeId::FLOAT:
			numeric_precision = Value::INTEGER(24);
			numeric_scale = Value::INTEGER(0);
			break;
		case LogicalTypeId::DOUBLE:
			numeric_precision = Value::INTEGER(53);
			numeric_scale = Value::INTEGER(0);
			break;
		default:
			numeric_precision = Value();
			numeric_scale = Value();
			break;
		}
		output.SetValue(10, index, numeric_precision);
		output.SetValue(11, index, numeric_scale);

		Value datetime_precision;
		switch (type.id()) {
		case LogicalTypeId::DATE:
		case LogicalTypeId::INTERVAL:
		case LogicalTypeId::TIME:
		case LogicalTypeId::TIMESTAMP:
		case LogicalTypeId::TIMESTAMP_NS:
		case LogicalTypeId::TIMESTAMP_MS:
		case LogicalTypeId::TIMESTAMP_SEC:
			// No fractional seconds are currently supported in DuckDB
			datetime_precision = Value::INTEGER(0);
			break;
		default:
			datetime_precision = Value();
		}
		output.SetValue(12, index, datetime_precision);
	}
}

} // anonymous namespace

void InformationSchemaColumnsFunction(ClientContext &context, const FunctionData *bind_data,
                                      FunctionOperatorData *operator_state, DataChunk *input, DataChunk &output) {
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
	set.AddFunction(TableFunction("information_schema_columns", {}, InformationSchemaColumnsFunction,
	                              InformationSchemaColumnsBind, InformationSchemaColumnsInit));
}

} // namespace duckdb
