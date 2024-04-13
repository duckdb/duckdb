#include "duckdb/parser/parsed_data/copy_info.hpp"

namespace duckdb {

unique_ptr<CopyInfo> CopyInfo::Copy() const {
	auto result = make_uniq<CopyInfo>();
	result->catalog = catalog;
	result->schema = schema;
	result->table = table;
	result->select_list = select_list;
	result->file_path = file_path;
	result->is_from = is_from;
	result->format = format;
	result->options = options;
	return result;
}

string CopyInfo::ToString() const {
	string result = "";
	if (is_from) {
		throw NotImplementedException("TODO COPYINFO::TOSTRING");
	} else {
		throw NotImplementedException("TODO COPYINFO::TOSTRING");
	}
	result += ";";
	return result;
}

} // namespace duckdb
