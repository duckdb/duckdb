#include "duckdb/common/enums/catalog_type.hpp"

#include "duckdb/common/exception.hpp"

using namespace std;

namespace duckdb {

string CatalogTypeToString(CatalogType type) {
	switch (type) {
	case CatalogType::COLLATION_ENTRY:
		return "Collation";
	case CatalogType::TABLE_ENTRY:
		return "Table";
	case CatalogType::SCHEMA_ENTRY:
		return "Schema";
	case CatalogType::TABLE_FUNCTION_ENTRY:
		return "Table Function";
	case CatalogType::SCALAR_FUNCTION_ENTRY:
		return "Scalar Function";
	case CatalogType::AGGREGATE_FUNCTION_ENTRY:
		return "Aggregate Function";
	case CatalogType::COPY_FUNCTION:
		return "Copy Function";
	case CatalogType::VIEW_ENTRY:
		return "View";
	case CatalogType::INDEX_ENTRY:
		return "Index";
	case CatalogType::PREPARED_STATEMENT:
		return "Prepared Statement";
	case CatalogType::SEQUENCE_ENTRY:
		return "Sequence";
	default:
		return "Unknown";
	}
}

} // namespace duckdb
