#include "duckdb/parser/statement/drop_statement.hpp"
#include "duckdb/parser/peg/transformer/peg_transformer.hpp"

namespace duckdb {

unique_ptr<SQLStatement> PEGTransformerFactory::TransformDeallocateStatement(PEGTransformer &transformer,
                                                                             const bool &deallocate_prepare,
                                                                             const string &identifier) {
	auto result = make_uniq<DropStatement>();
	result->info->type = CatalogType::PREPARED_STATEMENT;
	result->info->name = identifier;
	return std::move(result);
}

bool PEGTransformerFactory::TransformDeallocatePrepare(PEGTransformer &transformer) {
	return true;
}

// DiscardStatement <- 'DISCARD' DiscardTarget
// DiscardTarget    <- 'ALL' / 'PLANS' / 'SEQUENCES' / 'TEMPORARY' / 'TEMP'
// PG-compat: SereneDB has no temp tables / session sequences, so every DISCARD
// variant collapses to DEALLOCATE ALL (clear prepared statement cache).
// Mirrors the v2026.05.18 libpg_query path (deallocate.y).
unique_ptr<SQLStatement> PEGTransformerFactory::TransformDiscardStatement(PEGTransformer &transformer) {
	auto result = make_uniq<DropStatement>();
	result->info->type = CatalogType::PREPARED_STATEMENT;
	result->info->name = "";
	return std::move(result);
}

} // namespace duckdb
