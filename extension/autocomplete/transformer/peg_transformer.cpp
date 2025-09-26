#include "transformer/peg_transformer.hpp"

#include "duckdb/parser/statement/set_statement.hpp"
#include "duckdb/parser/expression/constant_expression.hpp"
#include "duckdb/common/exception/binder_exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/printer.hpp"

namespace duckdb {

template <typename T>
T PEGTransformer::Transform(optional_ptr<ParseResult> parse_result) {
	auto it = transform_functions.find(parse_result->name);
	if (it == transform_functions.end()) {
		throw NotImplementedException("No transformer function found for rule '%s'", parse_result->name);
	}
	auto &func = it->second;

	unique_ptr<TransformResultValue> base_result = func(*this, parse_result);
	if (!base_result) {
		throw InternalException("Transformer for rule '%s' returned a nullptr.", parse_result->name);
	}

	auto *typed_result_ptr = dynamic_cast<TypedTransformResult<T> *>(base_result.get());
	if (!typed_result_ptr) {
		throw InternalException("Transformer for rule '" + parse_result->name + "' returned an unexpected type.");
	}

	return std::move(typed_result_ptr->value);
}

template <typename T>
T PEGTransformer::TransformEnum(optional_ptr<ParseResult> parse_result) {
	auto enum_rule_name = parse_result->name;

	auto rule_value = enum_mappings.find(enum_rule_name);
	if (rule_value == enum_mappings.end()) {
		throw ParserException("Enum transform failed: could not find mapping for '%s'", enum_rule_name);
	}

	auto *typed_enum_ptr = dynamic_cast<TypedTransformEnumResult<T> *>(rule_value->second.get());
	if (!typed_enum_ptr) {
		throw InternalException("Enum mapping for rule '%s' has an unexpected type.", enum_rule_name);
	}

	return typed_enum_ptr->value;
}

template <typename T>
void PEGTransformer::TransformOptional(ListParseResult &list_pr, idx_t child_idx, T &target) {
	auto &opt = list_pr.Child<OptionalParseResult>(child_idx);
	if (opt.HasResult()) {
		target = Transform<T>(opt.optional_result);
	}
}

void PEGTransformer::ParamTypeCheck(PreparedParamType last_type, PreparedParamType new_type) {
	// Mixing positional/auto-increment and named parameters is not supported
	if (last_type == PreparedParamType::INVALID) {
		return;
	}
	if (last_type == PreparedParamType::NAMED) {
		if (new_type != PreparedParamType::NAMED) {
			throw NotImplementedException("Mixing named and positional parameters is not supported yet");
		}
	}
	if (last_type != PreparedParamType::NAMED) {
		if (new_type == PreparedParamType::NAMED) {
			throw NotImplementedException("Mixing named and positional parameters is not supported yet");
		}
	}
}

bool PEGTransformer::GetParam(const string &identifier, idx_t &index, PreparedParamType type) {
	ParamTypeCheck(last_param_type, type);
	auto entry = named_parameter_map.find(identifier);
	if (entry == named_parameter_map.end()) {
		return false;
	}
	index = entry->second;
	return true;
}

void PEGTransformer::SetParam(const string &identifier, idx_t index, PreparedParamType type) {
	ParamTypeCheck(last_param_type, type);
	last_param_type = type;
	D_ASSERT(!named_parameter_map.count(identifier));
	named_parameter_map[identifier] = index;
}

void PEGTransformer::ClearParameters() {
	prepared_statement_parameter_index = 0;
	named_parameter_map.clear();
}

} // namespace duckdb
