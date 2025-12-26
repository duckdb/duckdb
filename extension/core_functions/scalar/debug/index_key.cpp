#include "core_functions/scalar/debug_functions.hpp"

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

// The number of fixed arguments before the variadic key column arguments:
// catalog, schema, table, index_name
static constexpr idx_t INDEX_KEY_FIXED_ARGS = 4;

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

void ValidateIndex(const Index &index, const string &catalog_name, const string &schema_name, const string &table_name,
                   const string &index_name) {
	if (!index.IsBound()) {
		throw CatalogException("index_key: index '%s' on table %s.%s.%s is not yet bound", index_name, catalog_name,
		                       schema_name, table_name);
	}
	if (index.GetIndexType() != ART::TYPE_NAME) {
		throw NotImplementedException(
		    "index_key: index type '%s' is not yet supported (only ART indexes are supported)", index.GetIndexType());
	}
}

struct IndexKeyBindData : public FunctionData {
	IndexKeyBindData(optional_ptr<ART> art_index_p, vector<LogicalType> index_types)
	    : art_index(art_index_p), index_types(std::move(index_types)) {
	}

	unique_ptr<FunctionData> Copy() const override {
		return make_uniq<IndexKeyBindData>(*this);
	}

	bool Equals(const FunctionData &other_p) const override {
		auto &other = other_p.Cast<IndexKeyBindData>();
		return art_index == other.art_index && index_types == other.index_types;
	}

	optional_ptr<ART> art_index;
	vector<LogicalType> index_types;
};

unique_ptr<FunctionData> IndexKeyBind(ClientContext &context, ScalarFunction &bound_function,
                                      vector<unique_ptr<Expression>> &arguments) {
	if (arguments.size() < INDEX_KEY_FIXED_ARGS) {
		throw BinderException(
		    "index_key requires at least four arguments: catalog, schema, table, index_or_constraint");
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

	auto found_index = FindIndexByName(index_list, index_name, catalog_name, schema_name, table_entry.name);
	ValidateIndex(*found_index, catalog_name, schema_name, table_entry.name, index_name);

	auto &bound_index = found_index->Cast<BoundIndex>();
	auto &art_index = bound_index.Cast<ART>();
	vector<LogicalType> index_types = art_index.logical_types;
	if (index_types.empty()) {
		throw CatalogException("index_key: index '%s' has no key columns", index_name);
	}

	// Validate that the number of variadic arguments matches the number of index columns
	idx_t num_key_args = arguments.size() - INDEX_KEY_FIXED_ARGS;
	if (num_key_args != index_types.size()) {
		throw BinderException("index_key: index '%s' expects %llu key column(s), but %llu argument(s) provided",
		                      index_name, index_types.size(), num_key_args);
	}

	// Validate that each argument type matches the corresponding index column type exactly (no casting)
	for (idx_t i = 0; i < index_types.size(); i++) {
		auto &arg_expr = *arguments[INDEX_KEY_FIXED_ARGS + i];
		if (arg_expr.HasParameter()) {
			throw ParameterNotResolvedException();
		}
		if (arg_expr.return_type != index_types[i]) {
			throw BinderException("index_key: argument %llu has type %s but index '%s' expects %s", i + 1,
			                      arg_expr.return_type.ToString().c_str(), index_name,
			                      index_types[i].ToString().c_str());
		}
	}

	return make_uniq<IndexKeyBindData>(&art_index, std::move(index_types));
}

void IndexKeyFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &func_expr = state.expr.Cast<BoundFunctionExpression>();
	auto &bind_data = func_expr.bind_info->Cast<IndexKeyBindData>();

	idx_t count = args.size();
	D_ASSERT(args.ColumnCount() >= INDEX_KEY_FIXED_ARGS + bind_data.index_types.size());

	DataChunk key_chunk;
	key_chunk.Initialize(Allocator::DefaultAllocator(), bind_data.index_types);
	key_chunk.SetCardinality(count);

	// Copy the variadic key column arguments (starting from INDEX_KEY_FIXED_ARGS) into the key chunk
	for (idx_t col_idx = 0; col_idx < bind_data.index_types.size(); col_idx++) {
		key_chunk.data[col_idx].Reference(args.data[INDEX_KEY_FIXED_ARGS + col_idx]);
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
	ScalarFunction fun({LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR},
	                   LogicalType::BLOB, IndexKeyFunction, IndexKeyBind);
	fun.varargs = LogicalType::ANY;
	return fun;
}

} // namespace duckdb
