#include "duckdb/parser/statement/drop_statement.hpp"
#include "duckdb/parser/peg/transformer/peg_transformer.hpp"

namespace duckdb {

unique_ptr<SQLStatement> PEGTransformerFactory::TransformDeallocateStatement(const bool &deallocate_prepare,
                                                                             const string &identifier) {
	auto result = make_uniq<DropStatement>();
	result->info->type = CatalogType::PREPARED_STATEMENT;
	result->info->name = identifier;
	return std::move(result);
}

bool PEGTransformerFactory::TransformDeallocatePrepare() {
	return true;
}

} // namespace duckdb
