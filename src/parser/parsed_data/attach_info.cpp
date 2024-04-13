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

string AttachInfo::ToString() const {
	string result = "";
	result += "ATTACH DATABASE";
	if (on_conflict == OnCreateNotFound::IGNORE_ON_CONFLICT) {
		result += " IF NOT EXISTS";
	}
	result += StringUtil::Format(" '%s'", path);
	if (!name.empty()) {
		result += " AS " + name;
	}
	if (!options.empty()) {
		vector<string> stringified;
		for (auto &opt : options) {
			stringified.push_back(StringUtil::Format("%s = %s", opt.first, opt.second.ToString()));
		}
		result += " (" + StringUtil::Join(stringified, ", ") + ")";
	}
	result += ";";
	return result;
}

} // namespace duckdb
