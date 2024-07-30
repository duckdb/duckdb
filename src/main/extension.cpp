#include "duckdb/main/extension.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/main/extension_helper.hpp"

namespace duckdb {

Extension::~Extension() {
}

static string PrettyPrintString(const string &s) {
	string res = "";
	for (auto c : s) {
		if (StringUtil::CharacterIsAlpha(c) || StringUtil::CharacterIsDigit(c) || c == '_' || c == '-' || c == ' ' ||
		    c == '.') {
			res += c;
		} else {
			auto value = UnsafeNumericCast<uint8_t>(c);
			res += "\\x";
			uint8_t first = value / 16;
			if (first < 10) {
				res.push_back((char)('0' + first));
			} else {
				res.push_back((char)('a' + first - 10));
			}
			uint8_t second = value % 16;
			if (second < 10) {
				res.push_back((char)('0' + second));
			} else {
				res.push_back((char)('a' + second - 10));
			}
		}
	}
	return res;
}

string ParsedExtensionMetaData::GetInvalidMetadataError() {
	const string engine_version = string(ExtensionHelper::GetVersionDirectoryName());
	const string engine_platform = string(DuckDB::Platform());

	if (!AppearsValid()) {
		return "The file is not a DuckDB extension. The metadata at the end of the file is invalid";
	}

	string result;
	if (engine_version != duckdb_version) {
		result += StringUtil::Format("The file was built for DuckDB version '%s', but we can only load extensions "
		                             "built for DuckDB version '%s'.",
		                             PrettyPrintString(duckdb_version), engine_version);
	}
	if (engine_platform != platform) {
		if (!result.empty()) {
			result += " Also, t";
		} else {
			result += "T";
		}
		result += StringUtil::Format(
		    "he file was built for the platform '%s', but we can only load extensions built for platform '%s'.",
		    PrettyPrintString(platform), engine_platform);
	}

	return result;
}

} // namespace duckdb
