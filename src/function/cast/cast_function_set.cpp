#include "duckdb/function/cast/cast_function_set.hpp"
#include "duckdb/main/config.hpp"
#include "duckdb/common/types/type_map.hpp"
#include "duckdb/common/pair.hpp"
#include "duckdb/function/cast_rules.hpp"

namespace duckdb {

BindCastInput::BindCastInput(CastFunctionSet &function_set, BindCastInfo *info, ClientContext *context)
    : function_set(function_set), info(info), context(context) {
}

BoundCastInfo BindCastInput::GetCastFunction(const LogicalType &source, const LogicalType &target) {
	GetCastFunctionInput input(context);
	return function_set.GetCastFunction(source, target, input);
}

BindCastFunction::BindCastFunction(bind_cast_function_t function_p, unique_ptr<BindCastInfo> info_p)
    : function(function_p), info(std::move(info_p)) {
}

CastFunctionSet::CastFunctionSet() : map_info(nullptr) {
	bind_functions.emplace_back(DefaultCasts::GetDefaultCastFunction);
}

CastFunctionSet &CastFunctionSet::Get(ClientContext &context) {
	return DBConfig::GetConfig(context).GetCastFunctions();
}

CastFunctionSet &CastFunctionSet::Get(DatabaseInstance &db) {
	return DBConfig::GetConfig(db).GetCastFunctions();
}

BoundCastInfo CastFunctionSet::GetCastFunction(const LogicalType &source, const LogicalType &target,
                                               GetCastFunctionInput &get_input) {
	if (source == target) {
		return DefaultCasts::NopCast;
	}
	// the first function is the default
	// we iterate the set of bind functions backwards
	for (idx_t i = bind_functions.size(); i > 0; i--) {
		auto &bind_function = bind_functions[i - 1];
		BindCastInput input(*this, bind_function.info.get(), get_input.context);
		auto result = bind_function.function(input, source, target);
		if (result.function) {
			// found a cast function! return it
			return result;
		}
	}
	// no cast found: return the default null cast
	return DefaultCasts::TryVectorNullCast;
}

struct MapCastNode {
	MapCastNode(BoundCastInfo info, int64_t implicit_cast_cost)
	    : cast_info(std::move(info)), bind_function(nullptr), implicit_cast_cost(implicit_cast_cost) {
	}
	MapCastNode(bind_cast_function_t func, int64_t implicit_cast_cost)
	    : cast_info(nullptr), bind_function(func), implicit_cast_cost(implicit_cast_cost) {
	}

	BoundCastInfo cast_info;
	bind_cast_function_t bind_function;
	int64_t implicit_cast_cost;
};

struct MapCastInfo : public BindCastInfo {
	type_map_t<type_map_t<MapCastNode>> casts;
};

int64_t CastFunctionSet::ImplicitCastCost(const LogicalType &source, const LogicalType &target) {
	// check if a cast has been registered
	if (map_info) {
		auto source_entry = map_info->casts.find(source);
		if (source_entry != map_info->casts.end()) {
			auto target_entry = source_entry->second.find(target);
			if (target_entry != source_entry->second.end()) {
				return target_entry->second.implicit_cast_cost;
			}
		}
	}
	// if not, fallback to the default implicit cast rules
	return CastRules::ImplicitCast(source, target);
}

BoundCastInfo MapCastFunction(BindCastInput &input, const LogicalType &source, const LogicalType &target) {
	D_ASSERT(input.info);
	auto &map_info = (MapCastInfo &)*input.info;
	auto &casts = map_info.casts;

	auto entry = casts.find(source);
	if (entry == casts.end()) {
		// source type not found
		return nullptr;
	}
	auto target_entry = entry->second.find(target);
	if (target_entry == entry->second.end()) {
		// target type not found
		return nullptr;
	}
	if (target_entry->second.bind_function) {
		return target_entry->second.bind_function(input, source, target);
	}
	return target_entry->second.cast_info.Copy();
}

void CastFunctionSet::RegisterCastFunction(const LogicalType &source, const LogicalType &target, BoundCastInfo function,
                                           int64_t implicit_cast_cost) {
	RegisterCastFunction(source, target, MapCastNode(std::move(function), implicit_cast_cost));
}

void CastFunctionSet::RegisterCastFunction(const LogicalType &source, const LogicalType &target,
                                           bind_cast_function_t bind_function, int64_t implicit_cast_cost) {
	RegisterCastFunction(source, target, MapCastNode(bind_function, implicit_cast_cost));
}

void CastFunctionSet::RegisterCastFunction(const LogicalType &source, const LogicalType &target, MapCastNode node) {
	if (!map_info) {
		// create the cast map and the cast map function
		auto info = make_unique<MapCastInfo>();
		map_info = info.get();
		bind_functions.emplace_back(MapCastFunction, std::move(info));
	}
	map_info->casts[source].insert(make_pair(target, std::move(node)));
}

} // namespace duckdb
