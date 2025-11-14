#include "duckdb/parser/parsed_data/vacuum_info.hpp"
#include "duckdb/parser/keyword_helper.hpp"

namespace duckdb {

VacuumInfo::VacuumInfo(VacuumOptions options) : ParseInfo(TYPE), options(options), has_table(false) {
}

unique_ptr<VacuumInfo> VacuumInfo::Copy() {
	auto result = make_uniq<VacuumInfo>(options);
	result->has_table = has_table;
	if (has_table) {
		result->ref = ref->Copy();
	}
	result->columns = columns;
	return result;
}

string VacuumInfo::ToString() const {
	string result = "";
	result += "VACUUM";
	if (options.analyze) {
		result += " ANALYZE";
	}
	if (ref) {
		result += " " + ref->ToString();
		if (!columns.empty()) {
			vector<string> names;
			for (auto &column : columns) {
				names.push_back(KeywordHelper::WriteOptionallyQuoted(column));
			}
			result += "(" + StringUtil::Join(names, ", ") + ")";
		}
	}
	result += ";";
	return result;
}

} // namespace duckdb
