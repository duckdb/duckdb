#include "duckdb/parser/expression/parameter_expression.hpp"
#include "duckdb/parser/transformer.hpp"
#include "duckdb/common/algorithm.hpp"

namespace duckdb {

unique_ptr<ParsedExpression> Transformer::TransformParamRef(duckdb_libpgquery::PGParamRef *node) {
	D_ASSERT(node);
	auto expr = make_unique<ParameterExpression>();
	if (node->number < 0) {
		throw ParserException("Parameter numbers cannot be negative");
	}

	if (node->name) {
		// This is a named parameter, try to find an entry for it
		D_ASSERT(node->number == 0);
		int32_t index;
		if (GetNamedParam(node->name, index)) {
			// We've seen this named parameter before and assigned it an index!
			node->number = index;
		}
	}
	if (node->number == 0) {
		expr->parameter_nr = ParamCount() + 1;
		if (node->name && !HasNamedParameters() && ParamCount() != 0) {
			// This parameter is named, but there were other parameter before it, and they were not named
			throw NotImplementedException("Mixing positional and named parameters is not supported yet");
		}
		if (node->name) {
			D_ASSERT(!named_param_map.count(node->name));
			// Add it to the named parameter map so we can find it next time it's referenced
			SetNamedParam(node->name, expr->parameter_nr);
		}
	} else {
		if (!node->name && HasNamedParameters()) {
			// This parameter does not have a name, but the named param map is not empty
			throw NotImplementedException("Mixing positional and named parameters is not supported yet");
		}
		expr->parameter_nr = node->number;
	}
	SetParamCount(MaxValue<idx_t>(ParamCount(), expr->parameter_nr));
	return move(expr);
}

} // namespace duckdb
