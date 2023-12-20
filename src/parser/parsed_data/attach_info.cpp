#include "duckdb/parser/parsed_data/attach_info.hpp"

namespace duckdb {

unique_ptr<AttachInfo> AttachInfo::Copy() const {
	auto result = make_uniq<AttachInfo>();
	result->name = name;
	result->path = path;
	result->options = options;
	result->on_conflict = on_conflict;
	return result;
}

} // namespace duckdb
