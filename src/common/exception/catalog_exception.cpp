#include "duckdb/common/exception/catalog_exception.hpp"
#include "duckdb/common/to_string.hpp"
#include "duckdb/common/string_util.hpp"

namespace duckdb {

CatalogException::CatalogException(const string &msg) :
   StandardException(ExceptionType::CATALOG, msg) {
}

CatalogException CatalogException::MissingEntry(CatalogType type, const string &name, const string &suggestion, QueryErrorContext context) {
	string did_you_mean;
	if (!suggestion.empty()) {
		did_you_mean = "\nDid you mean \"" + suggestion + "\"?";
	}
	CatalogException result(context.FormatError("%s with name %s does not exist!%s", CatalogTypeToString(type),
												name, did_you_mean));
	result.InitializeExtraInfo("MISSING_ENTRY", context.query_location);

	result.extra_info["name"] = name;
	result.extra_info["type"] = CatalogTypeToString(type);
	if (!suggestion.empty()) {
		result.extra_info["candidates"] = suggestion;
	}
	return result;
}

CatalogException CatalogException::MissingEntry(const string &type, const string &name, const vector<string> &suggestions, QueryErrorContext context) {
	CatalogException result(context.FormatError("unrecognized %s \"%s\"\n%s", type,
												name, StringUtil::CandidatesErrorMessage(suggestions, name, "Did you mean")));
	result.InitializeExtraInfo("MISSING_ENTRY", context.query_location);
	result.extra_info["error_subtype"] = "MISSING_ENTRY";
	result.extra_info["name"] = name;
	result.extra_info["type"] = type;
	if (!suggestions.empty()) {
		result.extra_info["candidates"] = StringUtil::Join(suggestions, ", ");
	}
	return result;
}

CatalogException CatalogException::EntryAlreadyExists(CatalogType type, const string &name, QueryErrorContext context) {
	CatalogException result(context.FormatError("%s with name \"%s\" already exists!", CatalogTypeToString(type), name));
	result.InitializeExtraInfo("ENTRY_ALREADY_EXISTS", optional_idx());
	result.extra_info["name"] = name;
	result.extra_info["type"] = CatalogTypeToString(type);
	return result;
}


}
