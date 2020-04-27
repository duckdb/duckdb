#include "duckdb/common/enums/catalog_type.hpp"

#include "duckdb/common/exception.hpp"

using namespace std;

namespace duckdb {

string CatalogTypeToString(CatalogType type) {
	switch (type) {
	case CatalogType::COLLATION:
		return "Collation";
	case CatalogType::TABLE:
		return "Table";
	case CatalogType::SCHEMA:
		return "Schema";
	case CatalogType::TABLE_FUNCTION:
		return "Table Function";
	case CatalogType::SCALAR_FUNCTION:
		return "Scalar Function";
	case CatalogType::AGGREGATE_FUNCTION:
		return "Aggregate Function";
	case CatalogType::VIEW:
		return "View";
	case CatalogType::INDEX:
		return "Index";
	case CatalogType::PREPARED_STATEMENT:
		return "Prepared Statement";
	case CatalogType::SEQUENCE:
		return "Sequence";
	default:
		return "Unknown";
	}
}

} // namespace duckdb
