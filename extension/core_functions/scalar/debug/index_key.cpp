#include "core_functions/scalar/debug_functions.hpp"

#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_entry/duck_table_entry.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/common/constants.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/execution/index/art/art.hpp"
#include "duckdb/execution/index/art/art_key.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/main/table_description.hpp"
#include "duckdb/parser/parsed_data/parse_info.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/storage/data_table.hpp"

namespace duckdb {

namespace {

static constexpr idx_t INDEX_KEY_FIXED_ARGS = 2;

static TableDescription ExtractTableDescription(const child_list_t<LogicalType> &field_types,
                                                const vector<Value> &field_values) {
	unordered_map<string, string> fields;
	fields["catalog"] = INVALID_CATALOG;
	fields["schema"] = DEFAULT_SCHEMA;
	fields["table"] = "";

	for (idx_t i = 0; i < field_types.size(); i++) {
		auto field_name = StringUtil::Lower(field_types[i].first);

		if (fields.find(field_name) == fields.end()) {
			throw BinderException("index_key: unknown field '%s' in table_path", field_types[i].first);
		}

		auto &field_value = field_values[i];
		if (field_value.IsNull()) {
			continue;
		}
		if (field_value.type().id() != LogicalTypeId::VARCHAR) {
			throw BinderException("index_key: path field '%s' must be VARCHAR", field_types[i].first);
		}

		auto value = StringValue::Get(field_value);
		if (!value.empty()) {
			fields[field_name] = value;
		}
	}

	if (fields["table"].empty()) {
		throw BinderException("index_key: table_path must contain a non-empty 'table' field");
	}

	return TableDescription(fields["catalog"], fields["schema"], fields["table"]);
}

static TableDescription EvaluateTableDescription(ClientContext &context, const Expression &expr) {
	if (expr.HasParameter()) {
		throw ParameterNotResolvedException();
	}
	if (!expr.IsFoldable()) {
		throw BinderException("index_key: table_path parameter must be a constant");
	}

	auto input_struct = ExpressionExecutor::EvaluateScalar(context, expr);
	if (input_struct.IsNull()) {
		throw BinderException("index_key: table_path parameter cannot be NULL");
	}

	if (input_struct.type().id() != LogicalTypeId::STRUCT) {
		throw BinderException("index_key: table_path parameter must evaluate to a STRUCT");
	}

	return ExtractTableDescription(StructType::GetChildTypes(expr.return_type), StructValue::GetChildren(input_struct));
}

static string GetStringArgument(ClientContext &context, const Expression &expr, const string &param_name) {
	if (expr.HasParameter()) {
		throw ParameterNotResolvedException();
	}
	if (!expr.IsFoldable()) {
		throw BinderException("index_key: parameter '%s' must be a constant", param_name);
	}
	auto value = ExpressionExecutor::EvaluateScalar(context, expr);
	if (value.IsNull()) {
		throw BinderException("index_key: parameter '%s' cannot be NULL", param_name);
	}
	if (value.type().id() != LogicalTypeId::VARCHAR) {
		throw BinderException("index_key: parameter '%s' must be VARCHAR", param_name);
	}
	return StringValue::Get(value);
}

static optional_ptr<Index> FindIndexByName(TableIndexList &index_list, const string &index,
                                           const TableDescription &path) {
	optional_ptr<Index> found_index = nullptr;
	index_list.Scan([&](Index &candidate) {
		if (candidate.GetIndexName() == index) {
			found_index = &candidate;
			return true;
		}
		return false;
	});

	if (!found_index) {
		auto qualified_table = ParseInfo::QualifierToString(path.database, path.schema, path.table);
		vector<string> available;
		index_list.Scan([&](Index &idx) {
			available.push_back(idx.GetIndexName());
			return false;
		});

		if (available.empty()) {
			throw CatalogException("index_key: index '%s' was not found on table %s. No indexes found on this table.",
			                       index, qualified_table);
		}
		auto available_list = StringUtil::Join(available, ", ");
		throw CatalogException("index_key: index '%s' was not found on table %s. Available indexes: %s", index,
		                       qualified_table, available_list);
	}

	return found_index;
}

static void ValidateIndex(const Index &index, const TableDescription &path, const string &index_name) {
	if (!index.IsBound()) {
		auto qualified_table = ParseInfo::QualifierToString(path.database, path.schema, path.table);
		throw CatalogException("index_key: index '%s' on table %s is not yet bound", index_name, qualified_table);
	}
}

struct IndexKeyBindData : public FunctionData {
	IndexKeyBindData(optional_ptr<BoundIndex> bound_index_p, vector<LogicalType> key_types, string index_name)
	    : bound_index(bound_index_p), key_types(std::move(key_types)), index_name(std::move(index_name)) {
	}

	unique_ptr<FunctionData> Copy() const override {
		return make_uniq<IndexKeyBindData>(bound_index, key_types, index_name);
	}

	bool Equals(const FunctionData &other_p) const override {
		auto &other = other_p.Cast<IndexKeyBindData>();
		return bound_index == other.bound_index && key_types == other.key_types && index_name == other.index_name;
	}

	optional_ptr<BoundIndex> bound_index;
	vector<LogicalType> key_types;
	string index_name;
};

static unique_ptr<FunctionData> IndexKeyBind(ClientContext &context, ScalarFunction &bound_function,
                                             vector<unique_ptr<Expression>> &arguments) {
	if (arguments.size() < INDEX_KEY_FIXED_ARGS) {
		throw BinderException("index_key: requires at least two arguments - table_path (STRUCT), index_name");
	}

	auto &struct_expr = *arguments[0];
	auto &struct_type = struct_expr.return_type;
	bound_function.arguments[0] = struct_type;

	auto path = EvaluateTableDescription(context, struct_expr);
	auto index = GetStringArgument(context, *arguments[1], "index_name");

	auto &table_entry = Catalog::GetEntry(context, CatalogType::TABLE_ENTRY, path.database, path.schema, path.table)
	                        .Cast<TableCatalogEntry>();
	auto &duck_table = table_entry.Cast<DuckTableEntry>();
	auto &data_table = duck_table.GetStorage();
	auto &data_table_info = *data_table.GetDataTableInfo();

	data_table_info.BindIndexes(context);

	auto &index_list = data_table_info.GetIndexes();

	auto found_index = FindIndexByName(index_list, index, path);
	ValidateIndex(*found_index, path, index);

	auto &bound_index = found_index->Cast<BoundIndex>();
	auto key_types = bound_index.logical_types;
	if (key_types.empty()) {
		throw CatalogException("index_key: index '%s' has no key columns", index);
	}

	idx_t num_key_args = arguments.size() - INDEX_KEY_FIXED_ARGS;
	if (num_key_args != key_types.size()) {
		throw BinderException("index_key: index '%s' expects %llu key column(s), but %llu argument(s) provided", index,
		                      key_types.size(), num_key_args);
	}
	return make_uniq<IndexKeyBindData>(&bound_index, std::move(key_types), index);
}

static void IndexKeyFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &func_expr = state.expr.Cast<BoundFunctionExpression>();
	auto &bind_data = func_expr.bind_info->Cast<IndexKeyBindData>();

	idx_t count = args.size();
	D_ASSERT(args.ColumnCount() >= INDEX_KEY_FIXED_ARGS + bind_data.key_types.size());

	for (idx_t i = 0; i < bind_data.key_types.size(); i++) {
		auto &key_vector = args.data[INDEX_KEY_FIXED_ARGS + i];
		if (key_vector.GetType() != bind_data.key_types[i]) {
			throw InvalidInputException("index_key: argument %llu has type %s but index '%s' expects %s", i + 1,
			                            key_vector.GetType().ToString().c_str(), bind_data.index_name.c_str(),
			                            bind_data.key_types[i].ToString().c_str());
		}
	}

	DataChunk key_chunk;
	key_chunk.Initialize(Allocator::DefaultAllocator(), bind_data.key_types);
	key_chunk.SetCardinality(count);

	for (idx_t col_idx = 0; col_idx < bind_data.key_types.size(); col_idx++) {
		key_chunk.data[col_idx].Reference(args.data[INDEX_KEY_FIXED_ARGS + col_idx]);
	}

	result.SetVectorType(VectorType::FLAT_VECTOR);
	auto result_data = FlatVector::GetData<string_t>(result);
	auto &result_validity = FlatVector::Validity(result);
	result_validity.SetAllValid(count);

	auto index_type = bind_data.bound_index->GetIndexType();
	if (index_type == ART::TYPE_NAME) {
		auto &art = bind_data.bound_index->Cast<ART>();
		unsafe_vector<ARTKey> keys(count);
		ArenaAllocator allocator(Allocator::DefaultAllocator());
		art.GenerateKeys<>(allocator, key_chunk, keys);

		for (idx_t i = 0; i < count; i++) {
			auto &key = keys[i];
			if (key.Empty()) {
				result_validity.SetInvalid(i);
			} else {
				result_data[i] = StringVector::AddStringOrBlob(result, const_char_ptr_cast(key.data), key.len);
			}
		}
		if (count == 1) {
			result.SetVectorType(VectorType::CONSTANT_VECTOR);
		}
		result.Verify(count);
		return;
	}
	throw NotImplementedException("index_key: index type '%s' is not yet supported (only ART indexes are supported)",
	                              index_type);
}

} // namespace

ScalarFunction IndexKeyFun::GetFunction() {
	ScalarFunction fun("index_key", {LogicalTypeId::STRUCT, LogicalType::VARCHAR}, LogicalType::BLOB, IndexKeyFunction,
	                   IndexKeyBind);
	fun.varargs = LogicalTypeId::ANY;
	return fun;
}

} // namespace duckdb
