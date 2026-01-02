#include "duckdb/common/exception/catalog_exception.hpp"
#include "duckdb/common/to_string.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/catalog/entry_lookup_info.hpp"
#include "duckdb/planner/tableref/bound_at_clause.hpp"

namespace duckdb {

CatalogException::CatalogException(const string &msg) : Exception(ExceptionType::CATALOG, msg) {
}

CatalogException::CatalogException(const unordered_map<string, string> &extra_info, const string &msg)
    : Exception(extra_info, ExceptionType::CATALOG, msg) {
}

CatalogException CatalogException::MissingEntry(const EntryLookupInfo &lookup_info, const string &suggestion) {
	auto type = lookup_info.GetCatalogType();
	auto context = lookup_info.GetErrorContext();
	auto &name = lookup_info.GetEntryName();
	auto at_clause = lookup_info.GetAtClause();

	string did_you_mean;
	if (!suggestion.empty()) {
		did_you_mean = "\nDid you mean \"" + suggestion + "\"?";
	}
	string version_info;
	if (at_clause) {
		version_info += " at " + StringUtil::Lower(at_clause->Unit()) + " " + at_clause->GetValue().ToString();
	}

	auto extra_info = Exception::InitializeExtraInfo("MISSING_ENTRY", context.query_location);

	extra_info["name"] = name;
	extra_info["type"] = CatalogTypeToString(type);
	if (!suggestion.empty()) {
		extra_info["candidates"] = suggestion;
	}
	return CatalogException(extra_info,
	                        StringUtil::Format("%s with name %s does not exist%s!%s", CatalogTypeToString(type), name,
	                                           version_info, did_you_mean));
}

CatalogException CatalogException::MissingEntry(CatalogType type, const string &name, const string &suggestion,
                                                QueryErrorContext context) {
	EntryLookupInfo lookup_info(type, name, context);
	return MissingEntry(lookup_info, suggestion);
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
	return CatalogException(extra_info,
	                        StringUtil::Format("unrecognized %s \"%s\"\n%s", type, name,
	                                           StringUtil::CandidatesErrorMessage(suggestions, name, "Did you mean")));
}

CatalogException CatalogException::EntryAlreadyExists(CatalogType type, const string &name, QueryErrorContext context) {
	auto extra_info = Exception::InitializeExtraInfo("ENTRY_ALREADY_EXISTS", optional_idx());
	extra_info["name"] = name;
	extra_info["type"] = CatalogTypeToString(type);
	return CatalogException(extra_info,
	                        StringUtil::Format("%s with name \"%s\" already exists!", CatalogTypeToString(type), name));
}

} // namespace duckdb
