#include "duckdb/parser/statement/detach_statement.hpp"
#include "duckdb/parser/peg/transformer/peg_transformer.hpp"

namespace duckdb {

unique_ptr<SQLStatement> PEGTransformerFactory::TransformDetachStatement(PEGTransformer &transformer,
                                                                         const bool &if_exists,
                                                                         const Identifier &catalog_name) {
	auto result = make_uniq<DetachStatement>();
	auto info = make_uniq<DetachInfo>();
	info->if_not_found = if_exists ? OnEntryNotFound::RETURN_NULL : OnEntryNotFound::THROW_EXCEPTION;
	info->name = catalog_name;
	result->info = std::move(info);
	return std::move(result);
}

} // namespace duckdb
