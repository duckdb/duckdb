#include "duckdb/parser/statement/load_statement.hpp"
#include "duckdb/planner/binder.hpp"

namespace duckdb {

BoundStatement Binder::Bind(LoadStatement &stmt) {
	const bool is_install =
	    stmt.info->load_type == LoadType::INSTALL || stmt.info->load_type == LoadType::FORCE_INSTALL;
	const string action = is_install ? "INSTALL" : "LOAD";
	const string verb = is_install ? "installed" : "loaded";
	throw NotImplementedException(
	    action + " is not supported by SereneDB: extensions are compiled into the server binary and cannot be " + verb +
	    " at runtime.\nIf you need the \"" + stmt.info->filename +
	    "\" extension, please open an issue at https://github.com/serenedb/serenedb/issues.");
}

} // namespace duckdb
