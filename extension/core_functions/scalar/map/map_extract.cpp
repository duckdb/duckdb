#include "duckdb/common/vector/map_vector.hpp"
#include "core_functions/scalar/map_functions.hpp"
#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_entry/scalar_function_catalog_entry.hpp"
#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/function/function_binder.hpp"
#include "duckdb/function/scalar/list/contains_or_position.hpp"
#include "duckdb/function/scalar/nested_functions.hpp"
#include "duckdb/planner/expression/bound_case_expression.hpp"
#include "duckdb/planner/expression/bound_cast_expression.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/planner/expression/bound_operator_expression.hpp"

namespace duckdb {

static void MapExtractValueFunc(DataChunk &args, ExpressionState &state, Vector &result) {
	const auto count = args.size();

	const auto &map_vec = args.data[0];
	const auto &arg_vec = args.data[1];

	const auto &key_vec = MapVector::GetKeys(map_vec);
	const auto &val_vec = MapVector::GetValues(map_vec);

	// Collect the matching positions
	Vector pos_vec(LogicalType::INTEGER, count);
	ListSearchOp<int32_t>(map_vec, key_vec, arg_vec, pos_vec, args.size());

	UnifiedVectorFormat pos_format;
	UnifiedVectorFormat lst_format;

	pos_vec.ToUnifiedFormat(pos_format);
	map_vec.ToUnifiedFormat(lst_format);

	const auto pos_data = UnifiedVectorFormat::GetData<int32_t>(pos_format);
	const auto inc_list_data = UnifiedVectorFormat::GetData<list_entry_t>(lst_format);

	for (idx_t row_idx = 0; row_idx < count; row_idx++) {
		auto lst_idx = lst_format.sel->get_index(row_idx);
		if (!lst_format.validity.RowIsValid(lst_idx)) {
			FlatVector::SetNull(result, row_idx, true);
			continue;
		}

		const auto pos_idx = pos_format.sel->get_index(row_idx);
		if (!pos_format.validity.RowIsValid(pos_idx)) {
			// We didnt find the key in the map, so return NULL
			FlatVector::SetNull(result, row_idx, true);
			continue;
		}

		// Compute the actual position of the value in the map value vector
		const auto pos = inc_list_data[lst_idx].offset + UnsafeNumericCast<idx_t>(pos_data[pos_idx] - 1);
		VectorOperations::Copy(val_vec, result, pos + 1, pos, row_idx);
	}

	if (args.size() == 1) {
		result.SetVectorType(VectorType::CONSTANT_VECTOR);
	}

	result.Verify();
}

static void MapExtractListFunc(DataChunk &args, ExpressionState &state, Vector &result) {
	const auto count = args.size();

	const auto &map_vec = args.data[0];
	const auto &arg_vec = args.data[1];

	const auto &key_vec = MapVector::GetKeys(map_vec);
	const auto &val_vec = MapVector::GetValues(map_vec);

	// Collect the matching positions
	Vector pos_vec(LogicalType::INTEGER, count);
	ListSearchOp<int32_t>(map_vec, key_vec, arg_vec, pos_vec, args.size());

	auto pos_entries = pos_vec.Values<int32_t>();
	auto map_entries = map_vec.Values<list_entry_t>();
	const auto val_size = ListVector::GetListSize(map_vec);
	auto out_list_data = FlatVector::Writer<list_entry_t>(result, count);

	for (idx_t row_idx = 0; row_idx < count; row_idx++) {
		auto map_entry = map_entries[row_idx];
		if (!map_entry.IsValid()) {
			out_list_data.WriteNull();
			continue;
		}

		auto list = out_list_data.WriteDynamicList();
		auto pos_entry = pos_entries[row_idx];
		if (!pos_entry.IsValid()) {
			// key not found: return empty list
			continue;
		}

		const auto &inc_list = map_entry.GetValue();
		const auto pos = inc_list.offset + UnsafeNumericCast<idx_t>(pos_entry.GetValue() - 1);
		SelectionVector sel(1);
		sel.set_index(0, pos);
		list.Append(val_vec, sel, val_size, 0, 1);
	}
}

static bool TypeHasCollation(const LogicalType &type) {
	switch (type.id()) {
	case LogicalTypeId::VARCHAR:
		return !type.HasAlias() && !StringType::GetCollation(type).empty();
	case LogicalTypeId::LIST:
		return TypeHasCollation(ListType::GetChildType(type));
	case LogicalTypeId::ARRAY:
		return TypeHasCollation(ArrayType::GetChildType(type));
	default:
		return false;
	}
}

static unique_ptr<Expression> BindSiblingFunction(FunctionBindExpressionInput &input, const char *name,
                                                  vector<unique_ptr<Expression>> children) {
	auto &catalog = Catalog::GetSystemCatalog(input.context);
	auto &function_entry = catalog.GetEntry<ScalarFunctionCatalogEntry>(
	    input.context, QualifiedName(catalog.GetName(), Identifier::DefaultSchema(), Identifier(name)));
	FunctionBinder function_binder(input.context);
	ErrorData error;
	auto function = function_binder.BindScalarFunction(function_entry, std::move(children), error);
	if (!function) {
		error.Throw();
	}
	return function;
}

static unique_ptr<Expression> IsNull(unique_ptr<Expression> expression) {
	auto result = make_uniq<BoundOperatorExpression>(ExpressionType::OPERATOR_IS_NULL, LogicalType::BOOLEAN);
	result->GetChildrenMutable().push_back(std::move(expression));
	return std::move(result);
}

static unique_ptr<Expression> CastToResultType(FunctionBindExpressionInput &input, unique_ptr<Expression> expression) {
	const auto &result_type = input.bound_function.GetReturnType();
	if (expression->GetReturnType() == result_type) {
		return expression;
	}
	return BoundCastExpression::AddCastToType(input.context, std::move(expression), result_type);
}

//! Binds map_values(map), list_position(map_keys(map), key) and friends for the collated rewrites below.
struct MapSearchRewrite {
	unique_ptr<Expression> values;
	unique_ptr<Expression> position;
	unique_ptr<Expression> key_is_null;
};

static MapSearchRewrite BindMapSearch(FunctionBindExpressionInput &input) {
	MapSearchRewrite rewrite;

	vector<unique_ptr<Expression>> keys_children;
	keys_children.push_back(input.children[0]->Copy());
	auto keys = BindSiblingFunction(input, "map_keys", std::move(keys_children));

	vector<unique_ptr<Expression>> values_children;
	values_children.push_back(std::move(input.children[0]));
	rewrite.values = BindSiblingFunction(input, "map_values", std::move(values_children));

	rewrite.key_is_null = IsNull(input.children[1]->Copy());

	vector<unique_ptr<Expression>> position_children;
	position_children.push_back(std::move(keys));
	position_children.push_back(std::move(input.children[1]));
	rewrite.position = BindSiblingFunction(input, "list_position", std::move(position_children));

	return rewrite;
}

static unique_ptr<Expression> BindMapExtractValueExpression(FunctionBindExpressionInput &input) {
	// Untyped NULL maps are left to the direct implementation (they evaluate to NULL).
	if (input.children[0]->GetReturnType().id() == LogicalTypeId::SQLNULL) {
		return nullptr;
	}
	// Check both key types: template "K" unifies the map-key and search-key types, but the
	// binder does not insert casts for types that only differ in their collation annotation,
	// so the collation shows up on whichever side originally carried it.
	if (!TypeHasCollation(input.children[1]->GetReturnType()) &&
	    !TypeHasCollation(MapType::KeyType(input.children[0]->GetReturnType()))) {
		return nullptr;
	}

	// Rebind a collated lookup through the collation-aware list search:
	//
	//   map_extract_value(m, k) == list_extract(map_values(m), list_position(map_keys(m), k))
	//
	// Map keys are unique, so the first match is the only match. NULL propagation is preserved:
	// a NULL map yields NULL through map_keys/map_values, and a missing key yields a NULL
	// position, which list_extract turns into NULL.
	auto rewrite = BindMapSearch(input);

	vector<unique_ptr<Expression>> extract_children;
	extract_children.push_back(std::move(rewrite.values));
	extract_children.push_back(std::move(rewrite.position));
	auto extract = CastToResultType(input, BindSiblingFunction(input, "list_extract", std::move(extract_children)));

	// A NULL search key never matches because map keys are never NULL; the guard keeps the
	// rewrite exact even for maps carrying NULL keys, e.g. read from a file that skips
	// validation.
	const auto &result_type = input.bound_function.GetReturnType();
	auto null_result = make_uniq<BoundConstantExpression>(Value(result_type));
	return make_uniq<BoundCaseExpression>(std::move(rewrite.key_is_null), std::move(null_result), std::move(extract));
}

static unique_ptr<Expression> BindMapExtractExpression(FunctionBindExpressionInput &input) {
	// Untyped NULL maps are left to the direct implementation (they evaluate to NULL).
	if (input.children[0]->GetReturnType().id() == LogicalTypeId::SQLNULL) {
		return nullptr;
	}
	// Check both key types: template "K" unifies the map-key and search-key types, but the
	// binder does not insert casts for types that only differ in their collation annotation,
	// so the collation shows up on whichever side originally carried it.
	if (!TypeHasCollation(input.children[1]->GetReturnType()) &&
	    !TypeHasCollation(MapType::KeyType(input.children[0]->GetReturnType()))) {
		return nullptr;
	}

	// Rebind a collated lookup through the collation-aware list search:
	//
	//   map_extract(m, k) == CASE WHEN m IS NULL THEN NULL
	//                             WHEN k IS NULL THEN []
	//                             WHEN pos IS NULL THEN []
	//                             ELSE [list_extract(map_values(m), pos)] END
	//
	//   with pos = list_position(map_keys(m), k)
	//
	// Map keys are unique, so the first match is the only match. The explicit NULL guards
	// preserve the direct implementation's NULL handling: a NULL map yields NULL, and a NULL
	// or missing search key yields an empty list (a NULL key can never match because map keys
	// are never NULL).
	const auto &result_type = input.bound_function.GetReturnType();
	const auto &value_type = ListType::GetChildType(result_type);

	auto map_is_null = IsNull(input.children[0]->Copy());
	auto rewrite = BindMapSearch(input);
	auto position_is_null = IsNull(rewrite.position->Copy());

	vector<unique_ptr<Expression>> extract_children;
	extract_children.push_back(std::move(rewrite.values));
	extract_children.push_back(std::move(rewrite.position));
	auto extract = BindSiblingFunction(input, "list_extract", std::move(extract_children));

	vector<unique_ptr<Expression>> pack_children;
	pack_children.push_back(std::move(extract));
	auto packed = CastToResultType(input, BindSiblingFunction(input, "list_pack", std::move(pack_children)));

	auto missing_result = make_uniq<BoundConstantExpression>(Value::LIST(value_type, {}));
	auto found_case =
	    make_uniq<BoundCaseExpression>(std::move(position_is_null), std::move(missing_result), std::move(packed));

	auto null_key_result = make_uniq<BoundConstantExpression>(Value::LIST(value_type, {}));
	auto key_case = make_uniq<BoundCaseExpression>(std::move(rewrite.key_is_null), std::move(null_key_result),
	                                               std::move(found_case));

	auto null_map_result = make_uniq<BoundConstantExpression>(Value(result_type));
	return make_uniq<BoundCaseExpression>(std::move(map_is_null), std::move(null_map_result), std::move(key_case));
}

ScalarFunction MapExtractValueFun::GetFunction() {
	auto key_type = LogicalType::TEMPLATE("K");
	auto val_type = LogicalType::TEMPLATE("V");

	ScalarFunction fun({LogicalType::MAP(key_type, val_type), key_type}, val_type, MapExtractValueFunc);
	fun.SetNullHandling(FunctionNullHandling::SPECIAL_HANDLING);
	fun.SetBindExpressionCallback(BindMapExtractValueExpression);
	return fun;
}

ScalarFunction MapExtractFun::GetFunction() {
	auto key_type = LogicalType::TEMPLATE("K");
	auto val_type = LogicalType::TEMPLATE("V");

	ScalarFunction fun({LogicalType::MAP(key_type, val_type), key_type}, LogicalType::LIST(val_type),
	                   MapExtractListFunc);
	fun.SetNullHandling(FunctionNullHandling::SPECIAL_HANDLING);
	fun.SetBindExpressionCallback(BindMapExtractExpression);
	return fun;
}

} // namespace duckdb
