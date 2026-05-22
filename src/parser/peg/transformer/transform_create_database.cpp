#include "duckdb/parser/statement/attach_statement.hpp"
#include "duckdb/parser/statement/detach_statement.hpp"
#include "duckdb/parser/peg/transformer/peg_transformer.hpp"

namespace duckdb {

// CREATE DATABASE foo -> ATTACH '' AS foo (TYPE serenedb)
unique_ptr<SQLStatement> PEGTransformerFactory::TransformCreateDatabaseStatement(PEGTransformer &transformer,
                                                                                 const bool &if_not_exists,
                                                                                 const string &catalog_name) {
	auto result = make_uniq<AttachStatement>();
	auto info = make_uniq<AttachInfo>();
	info->name = catalog_name;
	info->path = "";
	info->options["type"] = Value("serenedb");
	info->on_conflict = if_not_exists ? OnCreateConflict::IGNORE_ON_CONFLICT : OnCreateConflict::ERROR_ON_CONFLICT;
	result->info = std::move(info);
	return std::move(result);
}

// DROP DATABASE foo [(FORCE)] -> DETACH foo with is_drop=true so duckdb routes
// through DatabaseManager::DropDatabase (PG-compatible "database X does not
// exist" error and storage-extension drop_database callback). FORCE is parsed
// for grammar compatibility but currently ignored (serenedb's drop is
// already synchronous).
unique_ptr<SQLStatement> PEGTransformerFactory::TransformDropDatabaseStatement(PEGTransformer &transformer,
                                                                               const bool &if_exists,
                                                                               const string &catalog_name,
                                                                               const bool &drop_database_force) {
	(void)drop_database_force;
	auto result = make_uniq<DetachStatement>();
	auto info = make_uniq<DetachInfo>();
	info->name = catalog_name;
	info->if_not_found = if_exists ? OnEntryNotFound::RETURN_NULL : OnEntryNotFound::THROW_EXCEPTION;
	info->is_drop = true;
	result->info = std::move(info);
	return std::move(result);
}

bool PEGTransformerFactory::TransformDropDatabaseForce(PEGTransformer &transformer) {
	return true;
}

} // namespace duckdb
