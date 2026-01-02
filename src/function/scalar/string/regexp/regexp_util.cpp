#include "duckdb/function/scalar/regexp.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "re2/re2.h"
#include "re2/stringpiece.h"

namespace duckdb {

namespace regexp_util {

bool TryParseConstantPattern(ClientContext &context, Expression &expr, string &constant_string) {
	if (!expr.IsFoldable()) {
		return false;
	}
	Value pattern_str = ExpressionExecutor::EvaluateScalar(context, expr);
	if (!pattern_str.IsNull() && pattern_str.type().id() == LogicalTypeId::VARCHAR) {
		constant_string = StringValue::Get(pattern_str);
		return true;
	}
	return false;
}

void ParseRegexOptions(const string &options, duckdb_re2::RE2::Options &result, bool *global_replace) {
	for (idx_t i = 0; i < options.size(); i++) {
		switch (options[i]) {
		case 'c':
			// case-sensitive matching
			result.set_case_sensitive(true);
			break;
		case 'i':
			// case-insensitive matching
			result.set_case_sensitive(false);
			break;
		case 'l':
			// literal matching
			result.set_literal(true);
			break;
		case 'm':
		case 'n':
		case 'p':
			// newline-sensitive matching
			result.set_dot_nl(false);
			break;
		case 's':
			// non-newline-sensitive matching
			result.set_dot_nl(true);
			break;
		case 'g':
			// global replace, only available for regexp_replace
			if (global_replace) {
				*global_replace = true;
			} else {
				throw InvalidInputException("Option 'g' (global replace) is only valid for regexp_replace");
			}
			break;
		case ' ':
		case '\t':
		case '\n':
			// ignore whitespace
			break;
		default:
			throw InvalidInputException("Unrecognized Regex option %c", options[i]);
		}
	}
}

void ParseRegexOptions(ClientContext &context, Expression &expr, RE2::Options &target, bool *global_replace) {
	if (expr.HasParameter()) {
		throw ParameterNotResolvedException();
	}
	if (!expr.IsFoldable()) {
		throw InvalidInputException("Regex options field must be a constant");
	}
	Value options_str = ExpressionExecutor::EvaluateScalar(context, expr);
	if (options_str.IsNull()) {
		throw InvalidInputException("Regex options field must not be NULL");
	}
	if (options_str.type().id() != LogicalTypeId::VARCHAR) {
		throw InvalidInputException("Regex options field must be a string");
	}
	ParseRegexOptions(StringValue::Get(options_str), target, global_replace);
}

void ParseGroupNameList(ClientContext &context, const string &function_name, Expression &group_expr,
                        const string &pattern_string, RE2::Options &options, bool require_constant_pattern,
                        vector<string> &out_names, child_list_t<LogicalType> &out_struct_children) {
	if (group_expr.HasParameter()) {
		throw ParameterNotResolvedException();
	}
	if (!group_expr.IsFoldable()) {
		throw InvalidInputException("Group specification field must be a constant list");
	}
	Value list_val = ExpressionExecutor::EvaluateScalar(context, group_expr);
	if (list_val.IsNull() || list_val.type().id() != LogicalTypeId::LIST) {
		throw BinderException("Group specification must be a non-NULL LIST");
	}
	auto &children = ListValue::GetChildren(list_val);
	if (children.empty()) {
		throw BinderException("Group name list must be non-empty");
	}
	case_insensitive_set_t name_set;
	for (auto &child : children) {
		if (child.IsNull()) {
			throw BinderException("NULL group name in %s", function_name);
		}
		auto name = child.ToString();
		if (name_set.find(name) != name_set.end()) {
			throw BinderException("Duplicate group name '%s' in %s", name, function_name);
		}
		name_set.insert(name);
		out_names.push_back(name);
		out_struct_children.emplace_back(make_pair(name, LogicalType::VARCHAR));
	}
	if (require_constant_pattern) {
		duckdb_re2::StringPiece const_piece(pattern_string.c_str(), pattern_string.size());
		RE2 constant_re(const_piece, options);
		auto group_cnt = constant_re.NumberOfCapturingGroups();
		if (group_cnt == -1) {
			throw BinderException("Pattern failed to parse: %s", constant_re.error());
		}
		if ((idx_t)group_cnt < out_names.size()) {
			throw BinderException("Not enough capturing groups (%d) for provided names (%llu)", group_cnt,
			                      NumericCast<uint64_t>(out_names.size()));
		}
	}
}

// Advance exactly one UTF-8 codepoint starting at 'base'. Falls back to single byte on invalid lead.
// Does not do a full validation of UTF-8 sequence, assumes input is mostly valid UTF-8.
idx_t AdvanceOneUTF8Basic(const duckdb_re2::StringPiece &input, idx_t base) {
	if (base >= input.length()) {
		return 1; // Out of bounds, just advance one byte
	}
	unsigned char first = static_cast<unsigned char>(input[base]);
	idx_t char_len = 1;
	if ((first & 0x80) == 0) {
		char_len = 1; // ASCII
	} else if ((first & 0xE0) == 0xC0) {
		char_len = 2;
	} else if ((first & 0xF0) == 0xE0) {
		char_len = 3;
	} else if ((first & 0xF8) == 0xF0) {
		char_len = 4;
	} else {
		// This should be impossible since RE2 operates on codepoints
		throw InternalException("Invalid UTF-8 lead byte in regexp_extract_all");
	}
	if (base + char_len > input.length()) {
		throw InternalException("Invalid UTF-8 sequence in regexp_extract_all");
	}
	return char_len;
}

} // namespace regexp_util

} // namespace duckdb
