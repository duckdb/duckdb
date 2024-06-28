#include "duckdb/common/exception/catalog_exception.hpp"
#include "duckdb/common/to_string.hpp"
#include "duckdb/common/string_util.hpp"

namespace duckdb {

CatalogException::CatalogException(const string &msg) : Exception(ExceptionType::CATALOG, msg) {
}

CatalogException::CatalogException(const string &msg, const unordered_map<string, string> &extra_info)
    : Exception(ExceptionType::CATALOG, msg, extra_info) {
}

CatalogException CatalogException::MissingEntry(CatalogType type, const string &name, const string &suggestion,
                                                QueryErrorContext context) {
	string did_you_mean;
	if (!suggestion.empty()) {
		did_you_mean = "\nDid you mean \"" + suggestion + "\"?";
	}

	auto extra_info = Exception::InitializeExtraInfo("MISSING_ENTRY", context.query_location);

	extra_info["name"] = name;
	extra_info["type"] = CatalogTypeToString(type);
	if (!suggestion.empty()) {
		extra_info["candidates"] = suggestion;
	}
	return CatalogException(
	    StringUtil::Format("%s with name %s does not exist!%s", CatalogTypeToString(type), name, did_you_mean),
	    extra_info);
}

CatalogException CatalogException::MissingEntry(const string &type, const string &name,
                                                const vector<string> &suggestions, QueryErrorContext context) {
	auto extra_info = Exception::InitializeExtraInfo("MISSING_ENTRY", context.query_location);
	extra_info["error_subtype"] = "MISSING_ENTRY";
	extra_info["name"] = name;
	extra_info["type"] = type;
	if (!suggestions.empty()) {
		extra_info["candidates"] = StringUtil::Join(suggestions, ", ");
	}
	return CatalogException(StringUtil::Format("unrecognized %s \"%s\"\n%s", type, name,
	                                           StringUtil::CandidatesErrorMessage(suggestions, name, "Did you mean")),
	                        extra_info);
}

CatalogException CatalogException::EntryAlreadyExists(CatalogType type, const string &name, QueryErrorContext context) {
	auto extra_info = Exception::InitializeExtraInfo("ENTRY_ALREADY_EXISTS", optional_idx());
	extra_info["name"] = name;
	extra_info["type"] = CatalogTypeToString(type);
	return CatalogException(StringUtil::Format("%s with name \"%s\" already exists!", CatalogTypeToString(type), name),
	                        extra_info);
}

} // namespace duckdb
