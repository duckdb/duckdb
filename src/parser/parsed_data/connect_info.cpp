#include "duckdb/parser/parsed_data/connect_info.hpp"

namespace duckdb {

unique_ptr<ConnectInfo> ConnectInfo::Copy() const {
	auto result = make_uniq<ConnectInfo>();
	result->name = name;
	result->target_is_local = target_is_local;
	result->name_is_string_literal = name_is_string_literal;
	return result;
}

string ConnectInfo::ToString() const {
	if (target_is_local) {
		return "CONNECT LOCAL;";
	}
	if (name.empty()) {
		return "CONNECT;";
	}
	string result = "CONNECT ";
	if (name_is_string_literal) {
		result += SQLString(name.GetIdentifierName());
	} else {
		result += SQLIdentifier(name);
	}
	result += ";";
	return result;
}

} // namespace duckdb
