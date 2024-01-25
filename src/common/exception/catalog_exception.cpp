#include "duckdb/common/exception/catalog_exception.hpp"
#include "duckdb/common/to_string.hpp"

namespace duckdb {

CatalogException::CatalogException(const string &msg) :
   StandardException(ExceptionType::CATALOG, msg), catalog_exception_type(CatalogExceptionType::INVALID), catalog_type(CatalogType::INVALID) {
}

CatalogException CatalogException::MissingEntry(CatalogType type, const string &name, const string &suggestion, QueryErrorContext context) {
	string did_you_mean;
	if (!suggestion.empty()) {
		did_you_mean = "\nDid you mean \"" + suggestion + "\"?";
	}
	CatalogException result(context.FormatError("%s with name %s does not exist!%s", CatalogTypeToString(type),
												name, did_you_mean));

	result.catalog_exception_type = CatalogExceptionType::MISSING_ENTRY;
	result.catalog_type = type;
	if (!suggestion.empty()) {
		result.extra_info["candidates"] = suggestion;
	}
	if (context.query_location != DConstants::INVALID_INDEX) {
		result.extra_info["position"] = to_string(context.query_location);
	}
	return result;
}

}
