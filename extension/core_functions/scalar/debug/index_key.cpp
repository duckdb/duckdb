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
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/storage/data_table.hpp"

namespace duckdb {

namespace {

// The number of fixed arguments before the variadic key column arguments:
static constexpr idx_t INDEX_KEY_FIXED_ARGS = 2;

struct TablePath {
	string catalog;
	string schema;
	string table_name;
};

optional_idx FindStructFieldIndex(const LogicalType &struct_type, const string &field_name) {
	auto &struct_children = StructType::GetChildTypes(struct_type);
	for (idx_t i = 0; i < struct_children.size(); i++) {
		if (StringUtil::CIEquals(struct_children[i].first, field_name)) {
			return optional_idx(i);
		}
	}
	return optional_idx();
}

string GetOptionalStructField(const vector<Value> &children, const LogicalType &struct_type, const string &field_name,
                               const string &default_value) {
	auto field_idx = FindStructFieldIndex(struct_type, field_name);
	if (!field_idx.IsValid()) {
		return default_value;
	}
	auto &field_value = children[field_idx.GetIndex()];
	if (!field_value.IsNull() && field_value.type().id() == LogicalTypeId::VARCHAR) {
		string result = StringValue::Get(field_value);
		return result.empty() ? default_value : result;
	}
	return default_value;
}

string GetRequiredStructField(const vector<Value> &children, const LogicalType &struct_type, const string &field_name) {
	auto field_idx = FindStructFieldIndex(struct_type, field_name);
	if (!field_idx.IsValid()) {
		throw BinderException("index_key table_path must contain a '%s' field", field_name);
	}
	auto &field_value = children[field_idx.GetIndex()];
	if (field_value.IsNull()) {
		throw BinderException("index_key table_path must contain a non-NULL '%s' field", field_name);
	}
	if (field_value.type().id() != LogicalTypeId::VARCHAR) {
		throw BinderException("index_key table_path field '%s' must be VARCHAR", field_name);
	}
	auto result = StringValue::Get(field_value);
	if (result.empty()) {
		throw BinderException("index_key table_path field '%s' cannot be empty", field_name);
	}
	return result;
}

TablePath EvaluateTablePath(ClientContext &context, const Expression &expr) {
	if (!expr.IsFoldable()) {
		throw BinderException("index_key table_path parameter must be a foldable");
	}

	auto input_struct = ExpressionExecutor::EvaluateScalar(context, expr);
	if (input_struct.IsNull()) {
		throw BinderException("index_key table_path parameter cannot be NULL");
	}

	if (input_struct.type().id() != LogicalTypeId::STRUCT) {
		throw BinderException("index_key table_path parameter must evaluate to a STRUCT");
	}

	auto &input_children = StructValue::GetChildren(input_struct);
	auto &struct_type = expr.return_type;

	TablePath path;
	path.catalog = GetOptionalStructField(input_children, struct_type, "catalog", INVALID_CATALOG);
	path.schema = GetOptionalStructField(input_children, struct_type, "schema", DEFAULT_SCHEMA);
	path.table_name = GetRequiredStructField(input_children, struct_type, "table_name");
	return path;
}

string GetStringArgument(ClientContext &context, const Expression &expr, const string &param_name) {
	if (!expr.IsFoldable()) {
		throw BinderException("index_key parameter '%s' must be a foldable", param_name);
	}
	auto value = ExpressionExecutor::EvaluateScalar(context, expr);
	if (value.IsNull()) {
		throw BinderException("index_key parameter '%s' cannot be NULL", param_name);
	}
	if (value.type().id() != LogicalTypeId::VARCHAR) {
		throw BinderException("index_key parameter '%s' must be VARCHAR", param_name);
	}
	return StringValue::Get(value);
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
		auto available_list = StringUtil::Join(available_names, ", ");

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
	IndexKeyBindData(optional_ptr<ART> art_index_p, vector<LogicalType> index_types, string index_name,
	                 idx_t fixed_args_count)
	    : art_index(art_index_p), index_types(std::move(index_types)), index_name(std::move(index_name)),
	      fixed_args(fixed_args_count) {
	}

	unique_ptr<FunctionData> Copy() const override {
		return make_uniq<IndexKeyBindData>(art_index, index_types, index_name, fixed_args);
	}

	bool Equals(const FunctionData &other_p) const override {
		auto &other = other_p.Cast<IndexKeyBindData>();
		return art_index == other.art_index && index_types == other.index_types && index_name == other.index_name &&
		       fixed_args == other.fixed_args;
	}

	optional_ptr<ART> art_index;
	vector<LogicalType> index_types;
	string index_name;
	idx_t fixed_args;
};

unique_ptr<FunctionData> IndexKeyBind(ClientContext &context, ScalarFunction &bound_function,
                                      vector<unique_ptr<Expression>> &arguments) {
	if (arguments.size() < INDEX_KEY_FIXED_ARGS) {
		throw BinderException("index_key requires at least two arguments: table_path (STRUCT), index_name");
	}

	auto &struct_expr = *arguments[0];
	auto &struct_type = struct_expr.return_type;
	bound_function.arguments[0] = struct_type;

	auto path = EvaluateTablePath(context, struct_expr);

	// Extract index_name (always the second argument)
	idx_t index_name_idx = INDEX_KEY_FIXED_ARGS - 1;
	auto index_name = GetStringArgument(context, *arguments[index_name_idx], "index_name");

	auto &table_entry = Catalog::GetEntry(context, CatalogType::TABLE_ENTRY, path.catalog, path.schema, path.table_name)
	                        .Cast<TableCatalogEntry>();
	auto &duck_table = table_entry.Cast<DuckTableEntry>();
	auto &data_table = duck_table.GetStorage();
	auto &data_table_info = *data_table.GetDataTableInfo();

	data_table_info.BindIndexes(context, ART::TYPE_NAME);

	auto &index_list = data_table_info.GetIndexes();

	auto found_index = FindIndexByName(index_list, index_name, path.catalog, path.schema, path.table_name);
	ValidateIndex(*found_index, path.catalog, path.schema, path.table_name, index_name);

	auto &bound_index = found_index->Cast<BoundIndex>();
	auto &art_index = bound_index.Cast<ART>();
	auto index_types = art_index.logical_types;
	if (index_types.empty()) {
		throw CatalogException("index_key: index '%s' has no key columns", index_name);
	}

	idx_t num_key_args = arguments.size() - INDEX_KEY_FIXED_ARGS;
	if (num_key_args != index_types.size()) {
		throw BinderException("index_key: index '%s' expects %llu key column(s), but %llu argument(s) provided",
		                      index_name, index_types.size(), num_key_args);
	}
	return make_uniq<IndexKeyBindData>(&art_index, std::move(index_types), index_name, INDEX_KEY_FIXED_ARGS);
}

void IndexKeyFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &func_expr = state.expr.Cast<BoundFunctionExpression>();
	auto &bind_data = func_expr.bind_info->Cast<IndexKeyBindData>();

	idx_t count = args.size();
	D_ASSERT(args.ColumnCount() >= bind_data.fixed_args + bind_data.index_types.size());

	for (idx_t i = 0; i < bind_data.index_types.size(); i++) {
		auto &key_vector = args.data[bind_data.fixed_args + i];
		if (key_vector.GetType() != bind_data.index_types[i]) {
			throw InvalidInputException("index_key: argument %llu has type %s but index '%s' expects %s", i + 1,
			                            key_vector.GetType().ToString().c_str(), bind_data.index_name.c_str(),
			                            bind_data.index_types[i].ToString().c_str());
		}
	}

	DataChunk key_chunk;
	key_chunk.Initialize(Allocator::DefaultAllocator(), bind_data.index_types);
	key_chunk.SetCardinality(count);

	for (idx_t col_idx = 0; col_idx < bind_data.index_types.size(); col_idx++) {
		key_chunk.data[col_idx].Reference(args.data[bind_data.fixed_args + col_idx]);
	}

	unsafe_vector<ARTKey> key_buffer;
	key_buffer.resize(count);

	ArenaAllocator allocator(Allocator::DefaultAllocator());
	bind_data.art_index->GenerateKeys<>(allocator, key_chunk, key_buffer);

	result.SetVectorType(VectorType::FLAT_VECTOR);
	auto result_data = FlatVector::GetData<string_t>(result);
	auto &result_validity = FlatVector::Validity(result);
	result_validity.SetAllValid(count);

	for (idx_t row = 0; row < count; row++) {
		auto &generated_key = key_buffer[row];
		if (generated_key.Empty()) {
			result_validity.SetInvalid(row);
		} else {
			result_data[row] = StringVector::AddStringOrBlob(result, const_char_ptr_cast(generated_key.data), generated_key.len);
		}
	}

	if (count == 1) {
		result.SetVectorType(VectorType::CONSTANT_VECTOR);
	}
	result.Verify(count);
}

} // namespace

ScalarFunction IndexKeyFun::GetFunction() {
	ScalarFunction fun("index_key", {LogicalTypeId::STRUCT, LogicalType::VARCHAR}, LogicalType::BLOB,
	                   IndexKeyFunction, IndexKeyBind);
	fun.varargs = LogicalTypeId::ANY;
	return fun;
}

} // namespace duckdb
