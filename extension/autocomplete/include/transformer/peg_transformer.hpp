#pragma once

#include "tokenizer.hpp"
#include "parse_result.hpp"
#include "transform_enum_result.hpp"
#include "transform_result.hpp"
#include "ast/setting_info.hpp"
#include "duckdb/function/macro_function.hpp"
#include "duckdb/parser/expression/case_expression.hpp"
#include "duckdb/parser/expression/function_expression.hpp"
#include "duckdb/parser/expression/parameter_expression.hpp"
#include "duckdb/parser/expression/window_expression.hpp"
#include "duckdb/parser/parsed_data/create_type_info.hpp"
#include "duckdb/parser/parsed_data/transaction_info.hpp"
#include "duckdb/parser/statement/copy_database_statement.hpp"
#include "duckdb/parser/statement/set_statement.hpp"
#include "duckdb/parser/statement/create_statement.hpp"
#include "duckdb/parser/tableref/basetableref.hpp"
#include "parser/peg_parser.hpp"
#include "duckdb/storage/arena_allocator.hpp"
#include "duckdb/parser/query_node/select_node.hpp"
#include "duckdb/parser/statement/drop_statement.hpp"
#include "duckdb/parser/statement/insert_statement.hpp"

namespace duckdb {

// Forward declare
struct QualifiedName;
struct MatcherToken;

struct PEGTransformerState {
	explicit PEGTransformerState(const vector<MatcherToken> &tokens_p) : tokens(tokens_p), token_index(0) {
	}

	const vector<MatcherToken> &tokens;
	idx_t token_index;
};

class PEGTransformer {
public:
	using AnyTransformFunction =
	    std::function<unique_ptr<TransformResultValue>(PEGTransformer &, optional_ptr<ParseResult>)>;

	PEGTransformer(ArenaAllocator &allocator, PEGTransformerState &state,
	               const case_insensitive_map_t<AnyTransformFunction> &transform_functions,
	               const case_insensitive_map_t<PEGRule> &grammar_rules,
	               const case_insensitive_map_t<unique_ptr<TransformEnumValue>> &enum_mappings)
	    : allocator(allocator), state(state), grammar_rules(grammar_rules), transform_functions(transform_functions),
	      enum_mappings(enum_mappings) {
	}

public:
	template <typename T>
	T Transform(optional_ptr<ParseResult> parse_result) {
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
	T TransformEnum(optional_ptr<ParseResult> parse_result) {
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
	void TransformOptional(ListParseResult &list_pr, idx_t child_idx, T &target) {
		auto &opt = list_pr.Child<OptionalParseResult>(child_idx);
		if (opt.HasResult()) {
			target = Transform<T>(opt.optional_result);
		}
	}

	// Make overloads return raw pointers, as ownership is handled by the ArenaAllocator.
	template <class T, typename... Args>
	T *Make(Args &&...args) {
		return allocator.Make<T>(std::forward<Args>(args)...);
	}

	void ClearParameters();
	static void ParamTypeCheck(PreparedParamType last_type, PreparedParamType new_type);
	void SetParam(const string &name, idx_t index, PreparedParamType type);
	bool GetParam(const string &name, idx_t &index, PreparedParamType type);

public:
	ArenaAllocator &allocator;
	PEGTransformerState &state;
	const case_insensitive_map_t<PEGRule> &grammar_rules;
	const case_insensitive_map_t<AnyTransformFunction> &transform_functions;
	const case_insensitive_map_t<unique_ptr<TransformEnumValue>> &enum_mappings;
	case_insensitive_map_t<idx_t> named_parameter_map;
	idx_t prepared_statement_parameter_index = 0;
	PreparedParamType last_param_type = PreparedParamType::INVALID;
};

class PEGTransformerFactory {
public:
	static PEGTransformerFactory &GetInstance();
	explicit PEGTransformerFactory();
	static unique_ptr<SQLStatement> Transform(vector<MatcherToken> &tokens, const char *root_rule = "Statement");

private:
	template <typename T>
	void RegisterEnum(const string &rule_name, T value) {
		auto existing_rule = enum_mappings.find(rule_name);
		if (existing_rule != enum_mappings.end()) {
			throw InternalException("EnumRule %s already exists", rule_name);
		}
		enum_mappings[rule_name] = make_uniq<TypedTransformEnumResult<T>>(value);
	}

	template <class FUNC>
	void Register(const string &rule_name, FUNC function) {
		auto existing_rule = sql_transform_functions.find(rule_name);
		if (existing_rule != sql_transform_functions.end()) {
			throw InternalException("Rule %s already exists", rule_name);
		}
		sql_transform_functions[rule_name] =
		    [function](PEGTransformer &transformer,
		               optional_ptr<ListParseResult> parse_result) -> unique_ptr<TransformResultValue> {
			auto result_value = function(transformer, parse_result);
			return make_uniq<TypedTransformResult<decltype(result_value)>>(std::move(result_value));
		};
	}

	PEGTransformerFactory(const PEGTransformerFactory &) = delete;

	static unique_ptr<SQLStatement> TransformStatement(PEGTransformer &, optional_ptr<ParseResult> list);

	// use.gram
	static unique_ptr<SQLStatement> TransformUseStatement(PEGTransformer &transformer, optional_ptr<ParseResult> parse_result);
	static unique_ptr<SQLStatement> TransformUseTarget(PEGTransformer &transformer, optional_ptr<ParseResult> parse_result);

	// set.gram
	static unique_ptr<SQLStatement> TransformResetStatement(PEGTransformer &transformer, optional_ptr<ParseResult> parse_result);
	static unique_ptr<SQLStatement> TransformSetAssignment(PEGTransformer &transformer, optional_ptr<ParseResult> parse_result);
	static unique_ptr<SQLStatement> TransformSetSetting(PEGTransformer &transformer, optional_ptr<ParseResult> parse_result);
	static unique_ptr<SQLStatement> TransformSetStatement(PEGTransformer &transformer, optional_ptr<ParseResult> parse_result);
	static unique_ptr<SQLStatement> TransformSetTimeZone(PEGTransformer &transformer, optional_ptr<ParseResult> parse_result);
	static unique_ptr<SQLStatement> TransformSetVariable(PEGTransformer &transformer, optional_ptr<ParseResult> parse_result);
	static unique_ptr<SQLStatement> TransformSettingScope(PEGTransformer &transformer, optional_ptr<ParseResult> parse_result);
	static unique_ptr<SQLStatement> TransformStandardAssignment(PEGTransformer &transformer, optional_ptr<ParseResult> parse_result);
	static unique_ptr<SQLStatement> TransformVariableAssign(PEGTransformer &transformer, optional_ptr<ParseResult> parse_result);
	static unique_ptr<SQLStatement> TransformVariableList(PEGTransformer &transformer, optional_ptr<ParseResult> parse_result);

private:
	PEGParser parser;
	case_insensitive_map_t<PEGTransformer::AnyTransformFunction> sql_transform_functions;
	case_insensitive_map_t<unique_ptr<TransformEnumValue>> enum_mappings;
};

} // namespace duckdb
