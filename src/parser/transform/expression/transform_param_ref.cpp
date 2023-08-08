#include "duckdb/parser/expression/parameter_expression.hpp"
#include "duckdb/parser/transformer.hpp"
#include "duckdb/common/algorithm.hpp"

namespace duckdb {

namespace {

struct PreparedParam {
	PreparedParamType type;
	string identifier;
};

} // namespace

static PreparedParam GetParameterIdentifier(duckdb_libpgquery::PGParamRef &node) {
	PreparedParam param;
	if (node.name) {
		param.type = PreparedParamType::NAMED;
		param.identifier = node.name;
		return param;
	}
	if (node.number < 0) {
		throw ParserException("Parameter numbers cannot be negative");
	}
	param.identifier = StringUtil::Format("%d", node.number);
	param.type = node.number == 0 ? PreparedParamType::AUTO_INCREMENT : PreparedParamType::POSITIONAL;
	return param;
}

unique_ptr<ParsedExpression> Transformer::TransformParamRef(duckdb_libpgquery::PGParamRef &node) {
	auto expr = make_uniq<ParameterExpression>();

	auto param = GetParameterIdentifier(node);
	idx_t known_param_index = DConstants::INVALID_INDEX;
	// This is a named parameter, try to find an entry for it
	GetParam(param.identifier, known_param_index, param.type);

	if (known_param_index == DConstants::INVALID_INDEX) {
		// We have not seen this parameter before
		if (node.number != 0) {
			// Preserve the parameter number
			known_param_index = node.number;
		} else {
			known_param_index = ParamCount() + 1;
			if (!node.name) {
				param.identifier = StringUtil::Format("%d", known_param_index);
			}
		}

		if (!named_param_map.count(param.identifier)) {
			// Add it to the named parameter map so we can find it next time it's referenced
			SetParam(param.identifier, known_param_index, param.type);
		}
	}

	expr->identifier = param.identifier;
	idx_t new_param_count = MaxValue<idx_t>(ParamCount(), known_param_index);
	SetParamCount(new_param_count);
	return std::move(expr);
}

} // namespace duckdb
