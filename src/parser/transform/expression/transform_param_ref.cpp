#include "duckdb/parser/expression/parameter_expression.hpp"
#include "duckdb/parser/transformer.hpp"
#include "duckdb/common/algorithm.hpp"

namespace duckdb {

string GetParameterIdentifier(duckdb_libpgquery::PGParamRef *node) {
	if (node->name) {
		return node->name;
	}
	if (node->number < 0) {
		throw ParserException("Parameter numbers cannot be negative");
	}
	return StringUtil::Format("%d", node->number);
}

idx_t Transformer::FindParameterIndexIfKnown(const string &name) {
	idx_t index = DConstants::INVALID_INDEX;
	// This is a named parameter, try to find an entry for it
	if (GetNamedParam(name, index)) {
		return index;
	}
	return index;
}

unique_ptr<ParsedExpression> Transformer::TransformParamRef(duckdb_libpgquery::PGParamRef *node) {
	D_ASSERT(node);
	auto expr = make_uniq<ParameterExpression>();

	auto identifier = GetParameterIdentifier(node);
	idx_t known_param_index = FindParameterIndexIfKnown(identifier);

	// Parameters come in three different types:
	// auto-increment: '?', has no name, and number is 0
	// positional: '$<number>', has no name, but does have a number
	// named: '$<name>', has a name, but the number is 0

	if (known_param_index == DConstants::INVALID_INDEX) {
		// We have not seen this parameter before
		if (node->number != 0) {
			// Preserve the parameter number
			expr->parameter_nr = node->number;
		} else {
			expr->parameter_nr = ParamCount() + 1;
			if (!node->name) {
				identifier = StringUtil::Format("%d", expr->parameter_nr);
			}
		}

		if (!named_param_map.count(identifier)) {
			// Add it to the named parameter map so we can find it next time it's referenced
			SetNamedParam(identifier, expr->parameter_nr);
		}
	} else {
		expr->parameter_nr = known_param_index;
	}

	idx_t new_param_count = MaxValue<idx_t>(ParamCount(), expr->parameter_nr);
	SetParamCount(new_param_count);
	return std::move(expr);
}

} // namespace duckdb
