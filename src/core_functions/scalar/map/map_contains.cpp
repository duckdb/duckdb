#include "duckdb/core_functions/scalar/map_functions.hpp"
#include "duckdb/core_functions/create_sort_key.hpp"
#include "duckdb/planner/expression/bound_cast_expression.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/common/optionally_owned_ptr.hpp"

namespace duckdb {

enum class MapContainsType : uint8_t {
	KEY,
	VALUE,
	BOTH,
};

struct MapContainsBindData final : public FunctionData {
	MapContainsType type;

	explicit MapContainsBindData(MapContainsType type_p) : type(type_p) {
	}

	bool Equals(const FunctionData &other) const override {
		return type == other.Cast<MapContainsBindData>().type;
	}

	unique_ptr<FunctionData> Copy() const override {
		return make_uniq<MapContainsBindData>(type);
	}
};

static unique_ptr<FunctionData> MapContainsBind(ClientContext &context, ScalarFunction &bound_function,
                                                vector<unique_ptr<Expression>> &arguments) {

	const auto &map_type = arguments[0]->return_type;
	const auto &key_type = MapType::KeyType(map_type);
	const auto &val_type = MapType::ValueType(map_type);

	bound_function.arguments[0] = map_type;

	MapContainsType type;

	if (bound_function.name == "map_contains_key") {
		type = MapContainsType::KEY;
		if (key_type.id() != LogicalTypeId::SQLNULL) {
			const auto &in_key_type = arguments[1]->return_type;
			if (key_type != in_key_type && key_type.id() != LogicalTypeId::SQLNULL) {
				arguments[1] = BoundCastExpression::AddCastToType(context, std::move(arguments[1]), key_type);
			}
			bound_function.arguments[1] = key_type;
		}
	} else if (bound_function.name == "map_contains_value") {
		type = MapContainsType::VALUE;
		if (val_type.id() != LogicalTypeId::SQLNULL) {
			const auto &in_val_type = arguments[1]->return_type;
			if (val_type != in_val_type && val_type.id() != LogicalTypeId::SQLNULL) {
				arguments[1] = BoundCastExpression::AddCastToType(context, std::move(arguments[1]), val_type);
			}
			bound_function.arguments[1] = val_type;
		}
	} else {
		D_ASSERT(bound_function.name == "map_contains");
		type = MapContainsType::BOTH;
		if (key_type.id() != LogicalTypeId::SQLNULL) {
			const auto &in_key_type = arguments[1]->return_type;
			if (key_type != in_key_type) {
				arguments[1] = BoundCastExpression::AddCastToType(context, std::move(arguments[1]), key_type);
			}
			bound_function.arguments[1] = key_type;
		}
		if (val_type.id() != LogicalTypeId::SQLNULL) {
			const auto &in_val_type = arguments[2]->return_type;
			if (val_type != in_val_type) {
				arguments[2] = BoundCastExpression::AddCastToType(context, std::move(arguments[2]), val_type);
			}
			bound_function.arguments[2] = val_type;
		}
	}

	return make_uniq<MapContainsBindData>(type);
}

// Decide whether to search for key, value or both
optionally_owned_ptr<Vector> GetTargetVector(DataChunk &args, MapContainsType type, idx_t count) {
	switch (type) {
	case MapContainsType::KEY:
	case MapContainsType::VALUE:
		return optionally_owned_ptr<Vector>(&args.data[1]);
	default:
		D_ASSERT(type == MapContainsType::BOTH);
		break;
	}
	// Create a struct vector holding both key and value
	auto &entry_type = ListType::GetChildType(args.data[0].GetType());
	auto target_vec = make_uniq<Vector>(entry_type, count);
	auto &target_parts = StructVector::GetEntries(*target_vec);
	target_parts[0]->Reference(args.data[1]);
	target_parts[1]->Reference(args.data[2]);
	return target_vec;
}

static Vector &GetSourceVector(Vector &map_vector, MapContainsType type) {
	switch (type) {
	case MapContainsType::KEY:
		return MapVector::GetKeys(map_vector);
	case MapContainsType::VALUE:
		return MapVector::GetValues(map_vector);
	default:
		D_ASSERT(type == MapContainsType::BOTH);
		break;
	}
	return ListVector::GetEntry(map_vector);
}

static void MapContainsFunction(DataChunk &args, ExpressionState &state, Vector &result) {

	auto key_is_null = MapType::KeyType(args.data[0].GetType()).id() == LogicalTypeId::SQLNULL;
	auto val_is_null = MapType::ValueType(args.data[0].GetType()).id() == LogicalTypeId::SQLNULL;

	if (key_is_null || val_is_null) {
		result.SetVectorType(VectorType::CONSTANT_VECTOR);
		ConstantVector::GetData<uint8_t>(result)[0] = false;
		return;
	}

	// Get the bind data
	auto &func_expr = state.expr.Cast<BoundFunctionExpression>();
	auto &info = func_expr.bind_info->Cast<MapContainsBindData>();

	const auto target_count = args.size();
	auto &map_vec = args.data[0];

	auto &search_vec = GetSourceVector(map_vec, info.type);
	auto search_count = ListVector::GetListSize(map_vec);

	auto target_vec_ptr = GetTargetVector(args, info.type, target_count);
	auto &target_vec = *target_vec_ptr;

	UnifiedVectorFormat search_format;
	UnifiedVectorFormat target_format;

	search_vec.ToUnifiedFormat(search_count, search_format);
	target_vec.ToUnifiedFormat(target_count, target_format);

	Vector search_data_vec(LogicalType::BLOB, search_count);
	Vector target_data_vec(LogicalType::BLOB, target_count);

	const OrderModifiers order_modifiers(OrderType::ASCENDING, OrderByNullType::NULLS_LAST);

	CreateSortKeyHelpers::CreateSortKey(search_vec, search_count, order_modifiers, search_data_vec);
	CreateSortKeyHelpers::CreateSortKey(target_vec, target_count, order_modifiers, target_data_vec);

	const auto search_data = FlatVector::GetData<string_t>(search_data_vec);

	BinaryExecutor::Execute<list_entry_t, string_t, bool>(
	    map_vec, target_data_vec, result, args.size(), [&](const list_entry_t &list, const string_t &target) {
		    // Short circuit if list is empty
		    if (list.length == 0) {
			    return false;
		    }

		    for (auto idx = list.offset; idx < list.offset + list.length; idx++) {
			    const auto search_idx = search_format.sel->get_index(idx);
			    if (search_format.validity.RowIsValid(search_idx) && search_data[search_idx] == target) {
				    return true;
			    }
		    }
		    return false;
	    });

	if (target_count == 1) {
		result.SetVectorType(VectorType::CONSTANT_VECTOR);
	}
}

ScalarFunction MapContainsFun::GetFunction() {
	ScalarFunction fun({LogicalType::MAP(LogicalType::ANY, LogicalType::ANY), LogicalType::ANY, LogicalType::ANY},
	                   LogicalType::BOOLEAN, MapContainsFunction, MapContainsBind);
	return fun;
}

ScalarFunction MapContainsKeyFun::GetFunction() {
	ScalarFunction fun({LogicalType::MAP(LogicalType::ANY, LogicalType::ANY), LogicalType::ANY}, LogicalType::BOOLEAN,
	                   MapContainsFunction, MapContainsBind);
	return fun;
}

ScalarFunction MapContainsValueFun::GetFunction() {
	ScalarFunction fun({LogicalType::MAP(LogicalType::ANY, LogicalType::ANY), LogicalType::ANY}, LogicalType::BOOLEAN,
	                   MapContainsFunction, MapContainsBind);
	return fun;
}

} // namespace duckdb
