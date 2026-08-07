#include "duckdb/parser/parsed_data/connect_info.hpp"

#include "duckdb/common/string_util.hpp"

namespace duckdb {

unique_ptr<ConnectInfo> ConnectInfo::Copy() const {
	auto result = make_uniq<ConnectInfo>();
	result->name = name;
	result->target_is_local = target_is_local;
	result->name_is_string_literal = name_is_string_literal;
	for (auto &entry : parsed_options) {
		result->parsed_options[entry.first] = entry.second->Copy();
	}
	result->options = options;
	if (external_resource) {
		result->external_resource = external_resource->Copy();
	}
	return result;
}

string ConnectInfo::ToString() const {
	if (external_resource) {
		// `CONNECT TO [CREATE] EXTERNAL RESOURCE <resource> [(create opts)] [(connect opts)]`
		string result = "CONNECT TO " + external_resource->ToString();
		if (!parsed_options.empty() || !options.empty()) {
			vector<string> stringified;
			for (auto &opt : parsed_options) {
				stringified.push_back(StringUtil::Format("%s %s", opt.first, opt.second->ToString()));
			}
			for (auto &opt : options) {
				stringified.push_back(StringUtil::Format("%s %s", opt.first, opt.second.ToSQLString()));
			}
			result += " (" + StringUtil::Join(stringified, ", ") + ")";
		}
		result += ";";
		return result;
	}
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
	if (!parsed_options.empty() || !options.empty()) {
		vector<string> stringified;
		for (auto &opt : parsed_options) {
			stringified.push_back(StringUtil::Format("%s %s", opt.first, opt.second->ToString()));
		}
		for (auto &opt : options) {
			stringified.push_back(StringUtil::Format("%s %s", opt.first, opt.second.ToSQLString()));
		}
		result += " (" + StringUtil::Join(stringified, ", ") + ")";
	}
	result += ";";
	return result;
}

} // namespace duckdb
