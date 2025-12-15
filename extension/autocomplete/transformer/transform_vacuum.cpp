#include "transformer/peg_transformer.hpp"

namespace duckdb {

unique_ptr<SQLStatement> PEGTransformerFactory::TransformVacuumStatement(PEGTransformer &transformer,
																   optional_ptr<ParseResult> parse_result) {
	throw NotImplementedException("TransformVacuumStatement has not yet been implemented");
}




}
