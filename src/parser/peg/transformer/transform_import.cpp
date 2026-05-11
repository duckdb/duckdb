#include "duckdb/parser/statement/pragma_statement.hpp"
#include "duckdb/parser/peg/transformer/peg_transformer.hpp"

namespace duckdb {

unique_ptr<SQLStatement> PEGTransformerFactory::TransformImportStatement(string import) {
	auto result = make_uniq<PragmaStatement>();
	result->info->name = "import_database";
	result->info->parameters.emplace_back(make_uniq<ConstantExpression>(Value(std::move(import))));
	return std::move(result);
}

} // namespace duckdb
