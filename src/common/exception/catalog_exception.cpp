#include "duckdb/common/exception/catalog_exception.hpp"
#include "duckdb/common/to_string.hpp"
#include "duckdb/common/string_util.hpp"

namespace duckdb {

CatalogException::CatalogException(const string &msg) :
   StandardException(ExceptionType::CATALOG, msg), catalog_exception_type(CatalogExceptionType::INVALID) {
}

CatalogException CatalogException::MissingEntry(CatalogType type, const string &name, const string &suggestion, QueryErrorContext context) {
	string did_you_mean;
	if (!suggestion.empty()) {
		did_you_mean = "\nDid you mean \"" + suggestion + "\"?";
	}
	CatalogException result(context.FormatError("%s with name %s does not exist!%s", CatalogTypeToString(type),
												name, did_you_mean));

	result.catalog_exception_type = CatalogExceptionType::MISSING_ENTRY;
	result.extra_info["name"] = name;
	result.extra_info["type"] = CatalogTypeToString(type);
	if (!suggestion.empty()) {
		result.extra_info["candidates"] = suggestion;
	}
	if (context.query_location != DConstants::INVALID_INDEX) {
		result.extra_info["position"] = to_string(context.query_location);
	}
	return result;
}

CatalogException CatalogException::MissingEntry(const string &type, const string &name, const vector<string> &suggestions, QueryErrorContext context) {
	CatalogException result(context.FormatError("unrecognized %s \"%s\"\n%s", type,
												name, StringUtil::CandidatesErrorMessage(suggestions, name, "Did you mean")));
	result.catalog_exception_type = CatalogExceptionType::MISSING_ENTRY;
	result.extra_info["name"] = name;
	result.extra_info["type"] = type;
	if (!suggestions.empty()) {
		result.extra_info["candidates"] = StringUtil::Join(suggestions, ", ");
	}
	if (context.query_location != DConstants::INVALID_INDEX) {
		result.extra_info["position"] = to_string(context.query_location);
	}
	return result;
}

CatalogException CatalogException::EntryAlreadyExists(CatalogType type, const string &name, QueryErrorContext context) {
	CatalogException result(context.FormatError("%s with name \"%s\" already exists!", CatalogTypeToString(type), name));
	result.catalog_exception_type = CatalogExceptionType::ENTRY_ALREADY_EXISTS;
	result.extra_info["name"] = name;
	result.extra_info["type"] = CatalogTypeToString(type);
	return result;
}


}
