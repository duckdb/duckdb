#include "duckdb/function/cast/cast_function_set.hpp"
#include "duckdb/main/config.hpp"
#include "duckdb/common/types/type_map.hpp"

namespace duckdb {

BindCastFunction::BindCastFunction(bind_cast_function_t function_p, unique_ptr<BindCastInfo> info_p) :
	function(function_p), info(move(info_p)) {}


CastFunctionSet::CastFunctionSet() : map_info(nullptr) {
	bind_functions.push_back(DefaultCasts::GetDefaultCastFunction);
}

CastFunctionSet &CastFunctionSet::Get(ClientContext &context) {
	return DBConfig::GetConfig(context).GetCastFunctions();
}

CastFunctionSet &CastFunctionSet::Get(DatabaseInstance &db) {
	return DBConfig::GetConfig(db).GetCastFunctions();
}

BoundCastInfo CastFunctionSet::GetCastFunction(const LogicalType &source, const LogicalType &target) {
	if (source == target) {
		return DefaultCasts::NopCast;
	}
	// the first function is the default
	// we iterate the set of bind functions backwards
	for(idx_t i = bind_functions.size(); i > 0; i--) {
		auto &bind_function = bind_functions[i - 1];
		BindCastInput input(*this, bind_function.info.get());
		auto result = bind_function.function(input, source, target);
		if (result.function) {
			// found a cast function! return it
			return result;
		}
	}
	// no cast found: return the default null cast
	return DefaultCasts::TryVectorNullCast;
}

struct MapCastInfo : public BindCastInfo {
	type_map_t<type_map_t<BoundCastInfo>> casts;
};

BoundCastInfo MapCastFunction(BindCastInput &input, const LogicalType &source, const LogicalType &target) {
	D_ASSERT(input.info);
	auto &map_info = (MapCastInfo &) *input.info;
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
	return target_entry->second.Copy();
}

void CastFunctionSet::RegisterCastFunction(const LogicalType &source, const LogicalType &target, BoundCastInfo function) {
	if (!map_info) {
		// create the cast map and the cast map function
		auto info = make_unique<MapCastInfo>();
		map_info = info.get();
		bind_functions.emplace_back(MapCastFunction, move(info));
	}
	map_info->casts[source][target] = move(function);
}


}
