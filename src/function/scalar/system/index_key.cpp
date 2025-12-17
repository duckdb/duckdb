#include "duckdb/function/scalar/system_functions.hpp"

#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_entry/duck_table_entry.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/common/constants.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/optional_ptr.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/types/value.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/execution/index/art/art.hpp"
#include "duckdb/execution/index/art/art_key.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/storage/data_table.hpp"

namespace duckdb {

namespace {

string GetStringArgument(ClientContext &context, const Expression &expr, const string &parameter_name,
                         const string &default_value, bool allow_empty_default) {
	if (!expr.IsFoldable()) {
		throw BinderException("index_key parameter '%s' must be a foldable", parameter_name);
	}
	ExpressionExecutor executor(context, expr);
	Vector value_vec(expr.return_type);
	executor.ExecuteExpression(value_vec);
	auto value = value_vec.GetValue(0);
	if (value.IsNull()) {
		if (!allow_empty_default) {
			throw BinderException("index_key parameter '%s' cannot be NULL", parameter_name);
		}
		return default_value;
	}
	auto result_str = StringValue::Get(value);
	if (result_str.empty() && allow_empty_default) {
		return default_value;
	}
	return result_str;
}

optional_ptr<Index> FindIndexByName(TableIndexList &index_list, const string &index_name, const string &catalog_name,
                                    const string &schema_name, const string &table_name) {
	optional_ptr<Index> found_index = nullptr;
	index_list.Scan([&](Index &candidate) {
		if (candidate.GetIndexName() == index_name) {
			found_index = &candidate;
			return true;
		}
		return false;
	});

	if (!found_index) {
		vector<string> available_names;
		index_list.Scan([&](Index &idx) {
			available_names.push_back(idx.GetIndexName());
			return false;
		});
		string available_list = StringUtil::Join(available_names, ", ");

		if (available_names.empty()) {
			throw CatalogException(
			    "index_key: index '%s' was not found on table %s.%s.%s. No indexes found on this table.", index_name,
			    catalog_name, schema_name, table_name);
		}
		throw CatalogException("index_key: index '%s' was not found on table %s.%s.%s. Available indexes: %s",
		                       index_name, catalog_name, schema_name, table_name, available_list);
	}

	return found_index;
}

void ValidateIndexIsBound(const Index &index, const string &catalog_name, const string &schema_name,
                          const string &table_name, const string &index_name) {
	if (!index.IsBound()) {
		throw CatalogException("index_key: index '%s' on table %s.%s.%s is not yet bound", index_name, catalog_name,
		                       schema_name, table_name);
	}
}

void ValidateIndexIsART(const Index &index) {
	if (index.GetIndexType() != ART::TYPE_NAME) {
		throw NotImplementedException(
		    "index_key: index type '%s' is not yet supported (only ART indexes are supported)", index.GetIndexType());
	}
}

vector<string> ExtractColumnNames(const BoundIndex &bound_index, const ColumnList &columns) {
	const auto &column_ids = bound_index.GetColumnIds();
	vector<string> column_names;
	column_names.reserve(column_ids.size());
	for (auto column_id : column_ids) {
		auto &col_def = columns.GetColumn(PhysicalIndex(column_id));
		column_names.emplace_back(col_def.GetName());
	}
	return column_names;
}

void ValidateKeyStructType(const Expression &key_expr) {
	if (key_expr.HasParameter()) {
		throw ParameterNotResolvedException();
	}
	D_ASSERT(key_expr.return_type.id() == LogicalTypeId::STRUCT);
}

// Returns a mapping from the struct field name to its offset in the struct type.
case_insensitive_map_t<idx_t> BuildFieldLookup(const LogicalType &key_type) {
	const auto &child_types = StructType::GetChildTypes(key_type);
	case_insensitive_map_t<idx_t> field_lookup;
	for (idx_t i = 0; i < child_types.size(); i++) {
		field_lookup[child_types[i].first] = i;
	}
	return field_lookup;
}

// Return a mapping from the logical offset of the index column (not physical) to its corresponding offset in
// the struct, and also perform type checking to see that they are compatible.
vector<idx_t> BuildFieldMap(const vector<string> &column_names, const LogicalType &key_type,
                            const vector<LogicalType> &key_types, const string &index_name) {
	const auto &child_types = StructType::GetChildTypes(key_type);
	auto field_lookup = BuildFieldLookup(key_type);

	if (child_types.size() < column_names.size()) {
		throw BinderException("index_key key_struct is missing fields for index '%s'", index_name);
	}

	vector<idx_t> field_map;
	field_map.reserve(column_names.size());
	for (idx_t i = 0; i < column_names.size(); i++) {
		auto entry = field_lookup.find(column_names[i]);
		if (entry == field_lookup.end()) {
			throw BinderException("index_key key_struct does not contain field '%s' required by index '%s'",
			                      column_names[i], index_name);
		}
		field_map.push_back(entry->second);
		const auto &struct_child_type = child_types[entry->second].second;
		if (struct_child_type != key_types[i]) {
			throw BinderException("index_key key_struct field '%s' has type %s but index expects %s", column_names[i],
			                      struct_child_type.ToString().c_str(), key_types[i].ToString().c_str());
		}
	}
	return field_map;
}

struct IndexKeyBindData : public FunctionData {
	IndexKeyBindData(optional_ptr<ART> art_index_p, vector<LogicalType> key_types, vector<idx_t> field_map)
	    : art_index(art_index_p), key_types(std::move(key_types)), field_map(std::move(field_map)) {
	}

	unique_ptr<FunctionData> Copy() const override {
		return make_uniq<IndexKeyBindData>(*this);
	}

	bool Equals(const FunctionData &other_p) const override {
		auto &other = other_p.Cast<IndexKeyBindData>();
		return art_index == other.art_index && key_types == other.key_types && field_map == other.field_map;
	}

	optional_ptr<ART> art_index;
	vector<LogicalType> key_types;
	// Mapping from offset in key_types (a column in the index) to its corresponding offset in the struct.
	vector<idx_t> field_map;
};

unique_ptr<FunctionData> IndexKeyBind(ClientContext &context, ScalarFunction &bound_function,
                                      vector<unique_ptr<Expression>> &arguments) {
	if (arguments.size() != 5) {
		throw BinderException(
		    "index_key requires five arguments: catalog, schema, table, index_or_constraint, key_struct");
	}

	string catalog_name = GetStringArgument(context, *arguments[0], "catalog", INVALID_CATALOG, true);
	string schema_name = GetStringArgument(context, *arguments[1], "schema", DEFAULT_SCHEMA, true);
	string table_name = GetStringArgument(context, *arguments[2], "table", string(), false);
	string index_name = GetStringArgument(context, *arguments[3], "index_or_constraint", string(), false);

	auto &table_entry = Catalog::GetEntry(context, CatalogType::TABLE_ENTRY, catalog_name, schema_name, table_name)
	                        .Cast<TableCatalogEntry>();
	auto &duck_table = table_entry.Cast<DuckTableEntry>();
	auto &data_table = duck_table.GetStorage();
	auto &data_table_info = *data_table.GetDataTableInfo();

	data_table_info.BindIndexes(context, ART::TYPE_NAME);

	auto &index_list = data_table_info.GetIndexes();
	auto &columns = duck_table.GetColumns();

	auto found_index = FindIndexByName(index_list, index_name, catalog_name, schema_name, table_entry.name);
	ValidateIndexIsBound(*found_index, catalog_name, schema_name, table_entry.name, index_name);
	ValidateIndexIsART(*found_index);

	auto &bound_index = found_index->Cast<BoundIndex>();
	auto &art_index = bound_index.Cast<ART>();
	vector<LogicalType> key_types = art_index.logical_types;
	if (key_types.empty()) {
		throw CatalogException("index_key: index '%s' has no key columns", index_name);
	}
	vector<string> column_names = ExtractColumnNames(bound_index, columns);

	auto &key_expr = *arguments[4];
	ValidateKeyStructType(key_expr);
	LogicalType key_type = key_expr.return_type;
	bound_function.arguments[4] = key_type;
	vector<idx_t> field_map = BuildFieldMap(column_names, key_type, key_types, index_name);

	return make_uniq<IndexKeyBindData>(&art_index, std::move(key_types), std::move(field_map));
}

void IndexKeyFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &func_expr = state.expr.Cast<BoundFunctionExpression>();
	auto &bind_data = func_expr.bind_info->Cast<IndexKeyBindData>();

	idx_t count = args.size();
	auto &key_struct = args.data[4];

	DataChunk key_chunk;
	key_chunk.Initialize(Allocator::DefaultAllocator(), bind_data.key_types);
	key_chunk.SetCardinality(count);

	auto &struct_children = StructVector::GetEntries(key_struct);

	for (idx_t col_idx = 0; col_idx < bind_data.key_types.size(); col_idx++) {
		auto field_idx = bind_data.field_map[col_idx];
		D_ASSERT(field_idx < struct_children.size());
		key_chunk.data[col_idx].Reference(*struct_children[field_idx]);
	}

	unsafe_vector<ARTKey> key_buffer;
	key_buffer.resize(count);

	ArenaAllocator allocator(Allocator::DefaultAllocator());
	bind_data.art_index->GenerateKeys<>(allocator, key_chunk, key_buffer);

	for (idx_t row = 0; row < count; row++) {
		auto &generated_key = key_buffer[row];
		if (generated_key.Empty()) {
			result.SetValue(row, Value());
		} else {
			const_data_ptr_t data_ptr = generated_key.data;
			Value blob_value = Value::BLOB(data_ptr, generated_key.len);
			result.SetValue(row, blob_value);
		}
	}

	if (count == 1) {
		result.SetVectorType(VectorType::CONSTANT_VECTOR);
	}
	result.Verify(count);
}

} // namespace

ScalarFunction IndexKeyFun::GetFunction() {
	return ScalarFunction(
	    {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalTypeId::STRUCT},
	    LogicalType::BLOB, IndexKeyFunction, IndexKeyBind);
}

} // namespace duckdb
