#include "duckdb/function/cast/cast_function_set.hpp"

#include "duckdb/common/pair.hpp"
#include "duckdb/common/types/type_map.hpp"
#include "duckdb/function/cast_rules.hpp"
#include "duckdb/planner/collation_binding.hpp"
#include "duckdb/main/config.hpp"

namespace duckdb {

BindCastInput::BindCastInput(CastFunctionSet &function_set, optional_ptr<BindCastInfo> info,
                             optional_ptr<ClientContext> context)
    : function_set(function_set), info(info), context(context) {
}

BoundCastInfo BindCastInput::GetCastFunction(const LogicalType &source, const LogicalType &target) {
	GetCastFunctionInput input(context);
	input.query_location = query_location;
	return function_set.GetCastFunction(source, target, input);
}

BindCastFunction::BindCastFunction(bind_cast_function_t function_p, unique_ptr<BindCastInfo> info_p)
    : function(function_p), info(std::move(info_p)) {
}

CastFunctionSet::CastFunctionSet() : map_info(nullptr) {
	bind_functions.emplace_back(DefaultCasts::GetDefaultCastFunction);
}

CastFunctionSet::CastFunctionSet(DBConfig &config_p) : CastFunctionSet() {
	this->config = &config_p;
}

CastFunctionSet &CastFunctionSet::Get(ClientContext &context) {
	return DBConfig::GetConfig(context).GetCastFunctions();
}

CollationBinding &CollationBinding::Get(ClientContext &context) {
	return DBConfig::GetConfig(context).GetCollationBinding();
}

CastFunctionSet &CastFunctionSet::Get(DatabaseInstance &db) {
	return DBConfig::GetConfig(db).GetCastFunctions();
}

CollationBinding &CollationBinding::Get(DatabaseInstance &db) {
	return DBConfig::GetConfig(db).GetCollationBinding();
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
		input.query_location = get_input.query_location;
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

template <class MAP_VALUE_TYPE>
static auto RelaxedTypeMatch(type_map_t<MAP_VALUE_TYPE> &map, const LogicalType &type) -> decltype(map.find(type)) {
	D_ASSERT(map.find(type) == map.end()); // we shouldn't be here
	switch (type.id()) {
	case LogicalTypeId::LIST:
		return map.find(LogicalType::LIST(LogicalType::ANY));
	case LogicalTypeId::STRUCT:
		return map.find(LogicalType::STRUCT({{"any", LogicalType::ANY}}));
	case LogicalTypeId::MAP:
		for (auto it = map.begin(); it != map.end(); it++) {
			const auto &entry_type = it->first;
			if (entry_type.id() != LogicalTypeId::MAP) {
				continue;
			}
			auto &entry_key_type = MapType::KeyType(entry_type);
			auto &entry_val_type = MapType::ValueType(entry_type);
			if ((entry_key_type == LogicalType::ANY || entry_key_type == MapType::KeyType(type)) &&
			    (entry_val_type == LogicalType::ANY || entry_val_type == MapType::ValueType(type))) {
				return it;
			}
		}
		return map.end();
	case LogicalTypeId::UNION:
		return map.find(LogicalType::UNION({{"any", LogicalType::ANY}}));
	case LogicalTypeId::ARRAY:
		return map.find(LogicalType::ARRAY(LogicalType::ANY, optional_idx()));
	default:
		return map.find(LogicalType::ANY);
	}
}

struct MapCastInfo : public BindCastInfo {
public:
	const optional_ptr<MapCastNode> GetEntry(const LogicalType &source, const LogicalType &target) {
		auto source_type_id_entry = casts.find(source.id());
		if (source_type_id_entry == casts.end()) {
			source_type_id_entry = casts.find(LogicalTypeId::ANY);
			if (source_type_id_entry == casts.end()) {
				return nullptr;
			}
		}

		auto &source_type_entries = source_type_id_entry->second;
		auto source_type_entry = source_type_entries.find(source);
		if (source_type_entry == source_type_entries.end()) {
			source_type_entry = RelaxedTypeMatch(source_type_entries, source);
			if (source_type_entry == source_type_entries.end()) {
				return nullptr;
			}
		}

		auto &target_type_id_entries = source_type_entry->second;
		auto target_type_id_entry = target_type_id_entries.find(target.id());
		if (target_type_id_entry == target_type_id_entries.end()) {
			target_type_id_entry = target_type_id_entries.find(LogicalTypeId::ANY);
			if (target_type_id_entry == target_type_id_entries.end()) {
				return nullptr;
			}
		}

		auto &target_type_entries = target_type_id_entry->second;
		auto target_type_entry = target_type_entries.find(target);
		if (target_type_entry == target_type_entries.end()) {
			target_type_entry = RelaxedTypeMatch(target_type_entries, target);
			if (target_type_entry == target_type_entries.end()) {
				return nullptr;
			}
		}

		return &target_type_entry->second;
	}

	void AddEntry(const LogicalType &source, const LogicalType &target, MapCastNode node) {
		casts[source.id()][source][target.id()].insert(make_pair(target, std::move(node)));
	}

private:
	type_id_map_t<type_map_t<type_id_map_t<type_map_t<MapCastNode>>>> casts;
};

int64_t CastFunctionSet::ImplicitCastCost(const LogicalType &source, const LogicalType &target) {
	// check if a cast has been registered
	if (map_info) {
		auto entry = map_info->GetEntry(source, target);
		if (entry) {
			return entry->implicit_cast_cost;
		}
	}
	// if not, fallback to the default implicit cast rules
	auto score = CastRules::ImplicitCast(source, target);
	if (score < 0 && config && config->options.old_implicit_casting) {
		if (source.id() != LogicalTypeId::BLOB && target.id() == LogicalTypeId::VARCHAR) {
			score = 149;
		}
	}
	return score;
}

BoundCastInfo MapCastFunction(BindCastInput &input, const LogicalType &source, const LogicalType &target) {
	D_ASSERT(input.info);
	auto &map_info = input.info->Cast<MapCastInfo>();
	auto entry = map_info.GetEntry(source, target);
	if (entry) {
		if (entry->bind_function) {
			return entry->bind_function(input, source, target);
		}
		return entry->cast_info.Copy();
	}
	return nullptr;
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
		auto info = make_uniq<MapCastInfo>();
		map_info = info.get();
		bind_functions.emplace_back(MapCastFunction, std::move(info));
	}
	map_info->AddEntry(source, target, std::move(node));
}

} // namespace duckdb
