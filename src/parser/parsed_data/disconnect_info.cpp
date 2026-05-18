#include "duckdb/parser/parsed_data/disconnect_info.hpp"

namespace duckdb {

unique_ptr<DisconnectInfo> DisconnectInfo::Copy() const {
	auto result = make_uniq<DisconnectInfo>();
	result->name = name;
	result->target_is_local = target_is_local;
	result->name_is_string_literal = name_is_string_literal;
	return result;
}

string DisconnectInfo::ToString() const {
	if (target_is_local) {
		return "DISCONNECT LOCAL;";
	}
	if (name.empty()) {
		return "DISCONNECT;";
	}
	string result = "DISCONNECT ";
	if (name_is_string_literal) {
		result += SQLString(name);
	} else {
		result += SQLIdentifier(name);
	}
	result += ";";
	return result;
}

} // namespace duckdb
