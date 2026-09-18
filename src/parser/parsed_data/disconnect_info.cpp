#include "duckdb/parser/parsed_data/disconnect_info.hpp"

namespace duckdb {

unique_ptr<DisconnectInfo> DisconnectInfo::Copy() const {
	return make_uniq<DisconnectInfo>();
}

string DisconnectInfo::ToString() const {
	return "DISCONNECT;";
}

} // namespace duckdb
