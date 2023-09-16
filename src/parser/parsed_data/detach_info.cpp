#include "duckdb/parser/parsed_data/detach_info.hpp"

namespace duckdb {

DetachInfo::DetachInfo() : ParseInfo(TYPE) {
}

unique_ptr<DetachInfo> DetachInfo::Copy() const {
	auto result = make_uniq<DetachInfo>();
	result->name = name;
	result->if_not_found = if_not_found;
	return result;
}

} // namespace duckdb
