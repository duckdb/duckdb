#include "duckdb/function/table/system_functions.hpp"

#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/view_catalog_entry.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/client_data.hpp"
#include "duckdb/parser/constraints/not_null_constraint.hpp"

#include <set>

namespace duckdb {

struct DuckDBColumnsData : public GlobalTableFunctionState {
	DuckDBColumnsData() : offset(0), column_offset(0) {
	}

	vector<reference<CatalogEntry>> entries;
	idx_t offset;
	idx_t column_offset;
};

static unique_ptr<FunctionData> DuckDBColumnsBind(ClientContext &context, TableFunctionBindInput &input,
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

	names.emplace_back("column_name");
	return_types.emplace_back(LogicalType::VARCHAR);

	names.emplace_back("column_index");
	return_types.emplace_back(LogicalType::INTEGER);

	names.emplace_back("comment");
	return_types.emplace_back(LogicalType::VARCHAR);

	names.emplace_back("internal");
	return_types.emplace_back(LogicalType::BOOLEAN);

	names.emplace_back("column_default");
	return_types.emplace_back(LogicalType::VARCHAR);

	names.emplace_back("is_nullable");
	return_types.emplace_back(LogicalType::BOOLEAN);

	names.emplace_back("data_type");
	return_types.emplace_back(LogicalType::VARCHAR);

	names.emplace_back("data_type_id");
	return_types.emplace_back(LogicalType::BIGINT);

	names.emplace_back("character_maximum_length");
	return_types.emplace_back(LogicalType::INTEGER);

	names.emplace_back("numeric_precision");
	return_types.emplace_back(LogicalType::INTEGER);

	names.emplace_back("numeric_precision_radix");
	return_types.emplace_back(LogicalType::INTEGER);

	names.emplace_back("numeric_scale");
	return_types.emplace_back(LogicalType::INTEGER);

	return nullptr;
}

unique_ptr<GlobalTableFunctionState> DuckDBColumnsInit(ClientContext &context, TableFunctionInitInput &input) {
	auto result = make_uniq<DuckDBColumnsData>();

	// scan all the schemas for tables and views and collect them
	auto schemas = Catalog::GetAllSchemas(context);
	for (auto &schema : schemas) {
		schema.get().Scan(context, CatalogType::TABLE_ENTRY,
		                  [&](CatalogEntry &entry) { result->entries.push_back(entry); });
	}
	return std::move(result);
}

class ColumnHelper {
public:
	static unique_ptr<ColumnHelper> Create(CatalogEntry &entry);

	virtual ~ColumnHelper() {
	}

	virtual StandardEntry &Entry() = 0;
	virtual idx_t NumColumns() = 0;
	virtual const string &ColumnName(idx_t col) = 0;
	virtual const LogicalType &ColumnType(idx_t col) = 0;
	virtual const Value ColumnDefault(idx_t col) = 0;
	virtual bool IsNullable(idx_t col) = 0;
	virtual const Value ColumnComment(idx_t col) = 0;

	void WriteColumns(idx_t index, idx_t start_col, idx_t end_col, DataChunk &output);
};

class TableColumnHelper : public ColumnHelper {
public:
	explicit TableColumnHelper(TableCatalogEntry &entry) : entry(entry) {
		for (auto &constraint : entry.GetConstraints()) {
			if (constraint->type == ConstraintType::NOT_NULL) {
				auto &not_null = constraint->Cast<NotNullConstraint>();
				not_null_cols.insert(not_null.index.index);
			}
		}
	}

	StandardEntry &Entry() override {
		return entry;
	}
	idx_t NumColumns() override {
		return entry.GetColumns().LogicalColumnCount();
	}
	const string &ColumnName(idx_t col) override {
		return entry.GetColumn(LogicalIndex(col)).Name();
	}
	const LogicalType &ColumnType(idx_t col) override {
		return entry.GetColumn(LogicalIndex(col)).Type();
	}
	const Value ColumnDefault(idx_t col) override {
		auto &column = entry.GetColumn(LogicalIndex(col));
		if (column.Generated()) {
			return Value(column.GeneratedExpression().ToString());
		} else if (column.HasDefaultValue()) {
			return Value(column.DefaultValue().ToString());
		}
		return Value();
	}
	bool IsNullable(idx_t col) override {
		return not_null_cols.find(col) == not_null_cols.end();
	}
	const Value ColumnComment(idx_t col) override {
		return entry.GetColumn(LogicalIndex(col)).Comment();
	}

private:
	TableCatalogEntry &entry;
	std::set<idx_t> not_null_cols;
};

class ViewColumnHelper : public ColumnHelper {
public:
	explicit ViewColumnHelper(ViewCatalogEntry &entry) : entry(entry) {
	}

	StandardEntry &Entry() override {
		return entry;
	}
	idx_t NumColumns() override {
		return entry.types.size();
	}
	const string &ColumnName(idx_t col) override {
		return col < entry.aliases.size() ? entry.aliases[col] : entry.names[col];
	}
	const LogicalType &ColumnType(idx_t col) override {
		return entry.types[col];
	}
	const Value ColumnDefault(idx_t col) override {
		return Value();
	}
	bool IsNullable(idx_t col) override {
		return true;
	}
	const Value ColumnComment(idx_t col) override {
		if (entry.column_comments.empty()) {
			return Value();
		}
		D_ASSERT(entry.column_comments.size() == entry.types.size());
		return entry.column_comments[col];
	}

private:
	ViewCatalogEntry &entry;
};

unique_ptr<ColumnHelper> ColumnHelper::Create(CatalogEntry &entry) {
	switch (entry.type) {
	case CatalogType::TABLE_ENTRY:
		return make_uniq<TableColumnHelper>(entry.Cast<TableCatalogEntry>());
	case CatalogType::VIEW_ENTRY:
		return make_uniq<ViewColumnHelper>(entry.Cast<ViewCatalogEntry>());
	default:
		throw NotImplementedException("Unsupported catalog type for duckdb_columns");
	}
}

void ColumnHelper::WriteColumns(idx_t start_index, idx_t start_col, idx_t end_col, DataChunk &output) {
	for (idx_t i = start_col; i < end_col; i++) {
		auto index = start_index + (i - start_col);
		auto &entry = Entry();

		idx_t col = 0;
		// database_name, VARCHAR
		output.SetValue(col++, index, entry.catalog.GetName());
		// database_oid, BIGINT
		output.SetValue(col++, index, Value::BIGINT(NumericCast<int64_t>(entry.catalog.GetOid())));
		// schema_name, VARCHAR
		output.SetValue(col++, index, entry.schema.name);
		// schema_oid, BIGINT
		output.SetValue(col++, index, Value::BIGINT(NumericCast<int64_t>(entry.schema.oid)));
		// table_name, VARCHAR
		output.SetValue(col++, index, entry.name);
		// table_oid, BIGINT
		output.SetValue(col++, index, Value::BIGINT(NumericCast<int64_t>(entry.oid)));
		// column_name, VARCHAR
		output.SetValue(col++, index, Value(ColumnName(i)));
		// column_index, INTEGER
		output.SetValue(col++, index, Value::INTEGER(UnsafeNumericCast<int32_t>(i + 1)));
		// comment, VARCHAR
		output.SetValue(col++, index, ColumnComment(i));
		// internal, BOOLEAN
		output.SetValue(col++, index, Value::BOOLEAN(entry.internal));
		// column_default, VARCHAR
		output.SetValue(col++, index, Value(ColumnDefault(i)));
		// is_nullable, BOOLEAN
		output.SetValue(col++, index, Value::BOOLEAN(IsNullable(i)));
		// data_type, VARCHAR
		const LogicalType &type = ColumnType(i);
		output.SetValue(col++, index, Value(type.ToString()));
		// data_type_id, BIGINT
		output.SetValue(col++, index, Value::BIGINT(int(type.id())));
		if (type == LogicalType::VARCHAR) {
			// FIXME: need check constraints in place to set this correctly
			// character_maximum_length, INTEGER
			output.SetValue(col++, index, Value());
		} else {
			// "character_maximum_length", PhysicalType::INTEGER
			output.SetValue(col++, index, Value());
		}

		Value numeric_precision, numeric_scale, numeric_precision_radix;
		switch (type.id()) {
		case LogicalTypeId::DECIMAL:
			numeric_precision = Value::INTEGER(DecimalType::GetWidth(type));
			numeric_scale = Value::INTEGER(DecimalType::GetScale(type));
			numeric_precision_radix = Value::INTEGER(10);
			break;
		case LogicalTypeId::HUGEINT:
			numeric_precision = Value::INTEGER(128);
			numeric_scale = Value::INTEGER(0);
			numeric_precision_radix = Value::INTEGER(2);
			break;
		case LogicalTypeId::BIGINT:
			numeric_precision = Value::INTEGER(64);
			numeric_scale = Value::INTEGER(0);
			numeric_precision_radix = Value::INTEGER(2);
			break;
		case LogicalTypeId::INTEGER:
			numeric_precision = Value::INTEGER(32);
			numeric_scale = Value::INTEGER(0);
			numeric_precision_radix = Value::INTEGER(2);
			break;
		case LogicalTypeId::SMALLINT:
			numeric_precision = Value::INTEGER(16);
			numeric_scale = Value::INTEGER(0);
			numeric_precision_radix = Value::INTEGER(2);
			break;
		case LogicalTypeId::TINYINT:
			numeric_precision = Value::INTEGER(8);
			numeric_scale = Value::INTEGER(0);
			numeric_precision_radix = Value::INTEGER(2);
			break;
		case LogicalTypeId::FLOAT:
			numeric_precision = Value::INTEGER(24);
			numeric_scale = Value::INTEGER(0);
			numeric_precision_radix = Value::INTEGER(2);
			break;
		case LogicalTypeId::DOUBLE:
			numeric_precision = Value::INTEGER(53);
			numeric_scale = Value::INTEGER(0);
			numeric_precision_radix = Value::INTEGER(2);
			break;
		default:
			numeric_precision = Value();
			numeric_scale = Value();
			numeric_precision_radix = Value();
			break;
		}

		// numeric_precision, INTEGER
		output.SetValue(col++, index, numeric_precision);
		// numeric_precision_radix, INTEGER
		output.SetValue(col++, index, numeric_precision_radix);
		// numeric_scale, INTEGER
		output.SetValue(col++, index, numeric_scale);
	}
}

void DuckDBColumnsFunction(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &data = data_p.global_state->Cast<DuckDBColumnsData>();
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
		auto column_helper = ColumnHelper::Create(data.entries[next].get());
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

void DuckDBColumnsFun::RegisterFunction(BuiltinFunctions &set) {
	set.AddFunction(TableFunction("duckdb_columns", {}, DuckDBColumnsFunction, DuckDBColumnsBind, DuckDBColumnsInit));
}

} // namespace duckdb
