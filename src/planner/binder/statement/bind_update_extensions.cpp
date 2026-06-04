#include "duckdb/planner/binder.hpp"
#include "duckdb/parser/statement/update_extensions_statement.hpp"

namespace duckdb {

BoundStatement Binder::Bind(UpdateExtensionsStatement &) {
	throw NotImplementedException(
	    "UPDATE EXTENSIONS is not supported by SereneDB: extensions are compiled into the server binary and cannot "
	    "be updated at runtime.\n"
	    "If you are missing an extension, please open an issue at https://github.com/serenedb/serenedb/issues.");
}

} // namespace duckdb
