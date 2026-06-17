#include "duckdb/parser/statement/drop_statement.hpp"
#include "duckdb/parser/peg/transformer/peg_transformer.hpp"

namespace duckdb {

unique_ptr<SQLStatement> PEGTransformerFactory::TransformDeallocateStatement(PEGTransformer &transformer,
                                                                             const bool &deallocate_prepare,
                                                                             const Identifier &identifier) {
	auto result = make_uniq<DropStatement>();
	result->info->type = CatalogType::PREPARED_STATEMENT;
	result->info->name = identifier;
	return std::move(result);
}

bool PEGTransformerFactory::TransformDeallocatePrepare(PEGTransformer &transformer) {
	return true;
}

} // namespace duckdb
