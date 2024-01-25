#include "duckdb/common/exception/catalog_exception.hpp"

namespace duckdb {

CatalogException::CatalogException(const string &msg) : StandardException(ExceptionType::CATALOG, msg) {
}

}
