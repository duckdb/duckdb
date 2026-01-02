#pragma once

#include "tokenizer.hpp"
#include "parse_result.hpp"
#include "transform_enum_result.hpp"
#include "transform_result.hpp"
#include "ast/add_column_entry.hpp"
#include "ast/column_elements.hpp"
#include "ast/create_table_as.hpp"
#include "ast/extension_repository_info.hpp"
#include "ast/generic_copy_option.hpp"
#include "ast/insert_values.hpp"
#include "ast/join_prefix.hpp"
#include "ast/join_qualifier.hpp"
#include "ast/macro_parameter.hpp"
#include "ast/on_conflict_expression_target.hpp"
#include "ast/sequence_option.hpp"
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
#include "duckdb/parser/statement/transaction_statement.hpp"
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
	T Transform(ListParseResult &parse_result, idx_t child_index) {
		auto child_parse_result = parse_result.GetChild(child_index);
		return Transform<T>(child_parse_result);
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

	//! Helper functions
	static unique_ptr<SQLStatement> Transform(vector<MatcherToken> &tokens, const char *root_rule = "Statement");
	static optional_ptr<ParseResult> ExtractResultFromParens(optional_ptr<ParseResult> parse_result);
	static vector<optional_ptr<ParseResult>> ExtractParseResultsFromList(optional_ptr<ParseResult> parse_result);
	static bool ExpressionIsEmptyStar(ParsedExpression &expr);
	static QualifiedName StringToQualifiedName(vector<string> input);
	static LogicalType GetIntervalTargetType(DatePartSpecifier date_part);
	static bool ConstructConstantFromExpression(const ParsedExpression &expr, Value &value);

	// Registration methods
	void RegisterAlter();
	void RegisterAttach();
	void RegisterCall();
	void RegisterCheckpoint();
	void RegisterComment();
	void RegisterCommon();
	void RegisterCopy();
	void RegisterCreateIndex();
	void RegisterCreateMacro();
	void RegisterCreateSchema();
	void RegisterCreateSequence();
	void RegisterCreateSecret();
	void RegisterCreateTable();
	void RegisterCreateType();
	void RegisterCreateView();
	void RegisterDeallocate();
	void RegisterDelete();
	void RegisterDetach();
	void RegisterDrop();
	void RegisterExport();
	void RegisterExpression();
	void RegisterImport();
	void RegisterInsert();
	void RegisterLoad();
	void RegisterPragma();
	void RegisterSelect();
	void RegisterUse();
	void RegisterSet();
	void RegisterTransaction();
	void RegisterKeywordsAndIdentifiers();
	void RegisterEnums();

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
		               optional_ptr<ParseResult> parse_result) -> unique_ptr<TransformResultValue> {
			auto result_value = function(transformer, parse_result);
			return make_uniq<TypedTransformResult<decltype(result_value)>>(std::move(result_value));
		};
	}

	PEGTransformerFactory(const PEGTransformerFactory &) = delete;

	static unique_ptr<SQLStatement> TransformStatement(PEGTransformer &, optional_ptr<ParseResult> list);

	// alter.gram
	static unique_ptr<SQLStatement> TransformAlterStatement(PEGTransformer &transformer,
	                                                        optional_ptr<ParseResult> parse_result);
	static unique_ptr<AlterInfo> TransformAlterOptions(PEGTransformer &transformer,
	                                                   optional_ptr<ParseResult> parse_result);
	static unique_ptr<AlterInfo> TransformAlterTableStmt(PEGTransformer &transformer,
	                                                     optional_ptr<ParseResult> parse_result);
	static unique_ptr<AlterInfo> TransformAlterDatabaseStmt(PEGTransformer &transformer,
	                                                        optional_ptr<ParseResult> parse_result);
	static unique_ptr<AlterInfo> TransformAlterViewStmt(PEGTransformer &transformer,
	                                                    optional_ptr<ParseResult> parse_result);
	static unique_ptr<AlterInfo> TransformAlterSequenceStmt(PEGTransformer &transformer,
	                                                        optional_ptr<ParseResult> parse_result);
	static QualifiedName TransformQualifiedSequenceName(PEGTransformer &transformer,
	                                                    optional_ptr<ParseResult> parse_result);
	static unique_ptr<AlterInfo> TransformAlterSequenceOptions(PEGTransformer &transformer,
	                                                           optional_ptr<ParseResult> parse_result);
	static unique_ptr<AlterInfo> TransformSetSequenceOption(PEGTransformer &transformer,
	                                                        optional_ptr<ParseResult> parse_result);
	static unique_ptr<AlterTableInfo> TransformAlterTableOptions(PEGTransformer &transformer,
	                                                             optional_ptr<ParseResult> parse_result);
	static unique_ptr<AlterTableInfo> TransformAddColumn(PEGTransformer &transformer,
	                                                     optional_ptr<ParseResult> parse_result);
	static AddColumnEntry TransformAddColumnEntry(PEGTransformer &transformer, optional_ptr<ParseResult> parse_result);
	static unique_ptr<AlterTableInfo> TransformDropColumn(PEGTransformer &transformer,
	                                                      optional_ptr<ParseResult> parse_result);
	static unique_ptr<AlterTableInfo> TransformSetPartitionedBy(PEGTransformer &transformer,
	                                                            optional_ptr<ParseResult> parse_result);
	static unique_ptr<AlterTableInfo> TransformResetPartitionedBy(PEGTransformer &transformer,
	                                                              optional_ptr<ParseResult> parse_result);
	static unique_ptr<AlterTableInfo> TransformAlterColumn(PEGTransformer &transformer,
	                                                       optional_ptr<ParseResult> parse_result);
	static unique_ptr<AlterTableInfo> TransformAlterColumnEntry(PEGTransformer &transformer,
	                                                            optional_ptr<ParseResult> parse_result);
	static unique_ptr<AlterTableInfo> TransformDropDefault(PEGTransformer &transformer,
	                                                       optional_ptr<ParseResult> parse_result);
	static unique_ptr<AlterTableInfo> TransformChangeNullability(PEGTransformer &transformer,
	                                                             optional_ptr<ParseResult> parse_result);
	static unique_ptr<AlterTableInfo> TransformAlterType(PEGTransformer &transformer,
	                                                     optional_ptr<ParseResult> parse_result);
	static unique_ptr<ParsedExpression> TransformUsingExpression(PEGTransformer &transformer,
	                                                             optional_ptr<ParseResult> parse_result);
	static string TransformDropOrSet(PEGTransformer &transformer, optional_ptr<ParseResult> parse_result);
	static unique_ptr<AlterTableInfo> TransformAddOrDropDefault(PEGTransformer &transformer,
	                                                            optional_ptr<ParseResult> parse_result);
	static unique_ptr<AlterTableInfo> TransformAddDefault(PEGTransformer &transformer,
	                                                      optional_ptr<ParseResult> parse_result);
	static unique_ptr<AlterTableInfo> TransformRenameColumn(PEGTransformer &transformer,
	                                                        optional_ptr<ParseResult> parse_result);
	static unique_ptr<AlterTableInfo> TransformRenameAlter(PEGTransformer &transformer,
	                                                       optional_ptr<ParseResult> parse_result);
	static unique_ptr<AlterTableInfo> TransformAddConstraint(PEGTransformer &transformer,
	                                                         optional_ptr<ParseResult> parse_result);
	static string TransformSequenceName(PEGTransformer &transformer, optional_ptr<ParseResult> parse_result);
	static unique_ptr<AlterTableInfo> TransformSetSortedBy(PEGTransformer &transformer,
	                                                       optional_ptr<ParseResult> parse_result);
	static unique_ptr<AlterTableInfo> TransformResetSortedBy(PEGTransformer &transformer,
	                                                         optional_ptr<ParseResult> parse_result);

	// attach.gram
	static unique_ptr<SQLStatement> TransformAttachStatement(PEGTransformer &transformer,
	                                                         optional_ptr<ParseResult> parse_result);
	static string TransformAttachAlias(PEGTransformer &transformer, optional_ptr<ParseResult> parse_result);
	static vector<GenericCopyOption> TransformAttachOptions(PEGTransformer &transformer,
	                                                        optional_ptr<ParseResult> parse_result);
	static vector<GenericCopyOption> TransformGenericCopyOptionList(PEGTransformer &transformer,
	                                                                optional_ptr<ParseResult> parse_result);
	static GenericCopyOption TransformGenericCopyOption(PEGTransformer &transformer,
	                                                    optional_ptr<ParseResult> parse_result);

	// call.gram
	static unique_ptr<SQLStatement> TransformCallStatement(PEGTransformer &transformer,
	                                                       optional_ptr<ParseResult> parse_result);

	// checkpoint.gram
	static unique_ptr<SQLStatement> TransformCheckpointStatement(PEGTransformer &transformer,
	                                                             optional_ptr<ParseResult> parse_result);

	// comment.gram
	static unique_ptr<SQLStatement> TransformCommentStatement(PEGTransformer &transformer,
	                                                          optional_ptr<ParseResult> parse_result);
	static CatalogType TransformCommentOnType(PEGTransformer &transformer, optional_ptr<ParseResult> parse_result);
	static Value TransformCommentValue(PEGTransformer &transformer, optional_ptr<ParseResult> parse_result);

	// common.gram
	static unique_ptr<ParsedExpression> TransformNumberLiteral(PEGTransformer &transformer,
	                                                           optional_ptr<ParseResult> parse_result);
	static string TransformStringLiteral(PEGTransformer &transformer, optional_ptr<ParseResult> parse_result);
	static LogicalType TransformType(PEGTransformer &transformer, optional_ptr<ParseResult> parse_result);
	static int64_t TransformArrayBounds(PEGTransformer &transformer, optional_ptr<ParseResult> parse_result);
	static int64_t TransformArrayKeyword(PEGTransformer &transformer, optional_ptr<ParseResult> parse_result);
	static int64_t TransformSquareBracketsArray(PEGTransformer &transformer, optional_ptr<ParseResult> parse_result);

	static LogicalType TransformTimeType(PEGTransformer &transformer, optional_ptr<ParseResult> parse_result);
	static bool TransformTimeZone(PEGTransformer &transformer, optional_ptr<ParseResult> parse_result);
	static bool TransformWithOrWithout(PEGTransformer &transformer, optional_ptr<ParseResult> parse_result);
	static LogicalTypeId TransformTimeOrTimestamp(PEGTransformer &transformer, optional_ptr<ParseResult> parse_result);
	static LogicalType TransformNumericType(PEGTransformer &transformer, optional_ptr<ParseResult> parse_result);
	static LogicalType TransformSimpleNumericType(PEGTransformer &transformer, optional_ptr<ParseResult> parse_result);
	static LogicalType TransformDecimalNumericType(PEGTransformer &transformer, optional_ptr<ParseResult> parse_result);
	static LogicalType TransformFloatType(PEGTransformer &transformer, optional_ptr<ParseResult> parse_result);
	static LogicalType TransformDecimalType(PEGTransformer &transformer, optional_ptr<ParseResult> parse_result);
	static vector<unique_ptr<ParsedExpression>> TransformTypeModifiers(PEGTransformer &transformer,
	                                                                   optional_ptr<ParseResult> parse_result);
	static LogicalType TransformSimpleType(PEGTransformer &transformer, optional_ptr<ParseResult> parse_result);
	static QualifiedName TransformQualifiedTypeName(PEGTransformer &transformer,
	                                                optional_ptr<ParseResult> parse_result);
	static LogicalType TransformCharacterType(PEGTransformer &transformer, optional_ptr<ParseResult> parse_result);
	static LogicalType TransformMapType(PEGTransformer &transformer, optional_ptr<ParseResult> parse_result);
	static LogicalType TransformRowType(PEGTransformer &transformer, optional_ptr<ParseResult> parse_result);
	static LogicalType TransformUnionType(PEGTransformer &transformer, optional_ptr<ParseResult> parse_result);
	static child_list_t<LogicalType> TransformColIdTypeList(PEGTransformer &transformer,
	                                                        optional_ptr<ParseResult> parse_result);
	static std::pair<std::string, LogicalType> TransformColIdType(PEGTransformer &transformer,
	                                                              optional_ptr<ParseResult> parse_result);
	static LogicalType TransformBitType(PEGTransformer &transformer, optional_ptr<ParseResult> parse_result);
	static LogicalType TransformIntervalType(PEGTransformer &transformer, optional_ptr<ParseResult> parse_result);
	static LogicalType TransformIntervalInterval(PEGTransformer &transformer, optional_ptr<ParseResult> parse_result);
	static DatePartSpecifier TransformInterval(PEGTransformer &transformer, optional_ptr<ParseResult> parse_result);
	static LogicalType TransformSetofType(PEGTransformer &transformer, optional_ptr<ParseResult> parse_result);

	// copy.gram
	static unique_ptr<SQLStatement> TransformCopyStatement(PEGTransformer &transformer,
	                                                       optional_ptr<ParseResult> parse_result);
	static unique_ptr<SQLStatement> TransformCopySelect(PEGTransformer &transformer,
	                                                    optional_ptr<ParseResult> parse_result);
	static unique_ptr<SQLStatement> TransformCopyFromDatabase(PEGTransformer &transformer,
	                                                          optional_ptr<ParseResult> parse_result);
	static CopyDatabaseType TransformCopyDatabaseFlag(PEGTransformer &transformer,
	                                                  optional_ptr<ParseResult> parse_result);
	static string ExtractFormat(const string &file_path);
	static unique_ptr<SQLStatement> TransformCopyTable(PEGTransformer &transformer,
	                                                   optional_ptr<ParseResult> parse_result);
	static bool TransformFromOrTo(PEGTransformer &transformer, optional_ptr<ParseResult> parse_result);
	static string TransformCopyFileName(PEGTransformer &transformer, optional_ptr<ParseResult> parse_result);
	static string TransformIdentifierColId(PEGTransformer &transformer, optional_ptr<ParseResult> parse_result);
	static case_insensitive_map_t<vector<Value>> TransformCopyOptions(PEGTransformer &transformer,
	                                                                  optional_ptr<ParseResult> parse_result);
	static case_insensitive_map_t<vector<Value>>
	TransformGenericCopyOptionListParens(PEGTransformer &transformer, optional_ptr<ParseResult> parse_result);
	static case_insensitive_map_t<vector<Value>> TransformSpecializedOptionList(PEGTransformer &transformer,
	                                                                            optional_ptr<ParseResult> parse_result);

	static GenericCopyOption TransformSpecializedOption(PEGTransformer &transformer,
	                                                    optional_ptr<ParseResult> parse_result);
	static GenericCopyOption TransformSingleOption(PEGTransformer &transformer, optional_ptr<ParseResult> parse_result);
	static GenericCopyOption TransformEncodingOption(PEGTransformer &transformer,
	                                                 optional_ptr<ParseResult> parse_result);
	static GenericCopyOption TransformForceQuoteOption(PEGTransformer &transformer,
	                                                   optional_ptr<ParseResult> parse_result);

	// create_index.gram
	static unique_ptr<CreateStatement> TransformCreateIndexStmt(PEGTransformer &transformer,
	                                                            optional_ptr<ParseResult> parse_result);
	static string TransformIndexType(PEGTransformer &transformer, optional_ptr<ParseResult> parse_result);
	static unique_ptr<ParsedExpression> TransformIndexElement(PEGTransformer &transformer,
	                                                          optional_ptr<ParseResult> parse_result);
	static case_insensitive_map_t<Value> TransformWithList(PEGTransformer &transformer,
	                                                       optional_ptr<ParseResult> parse_result);
	static case_insensitive_map_t<Value> TransformRelOptionOrOids(PEGTransformer &transformer,
	                                                              optional_ptr<ParseResult> parse_result);
	static case_insensitive_map_t<Value> TransformRelOptionList(PEGTransformer &transformer,
	                                                            optional_ptr<ParseResult> parse_result);
	static case_insensitive_map_t<Value> TransformOids(PEGTransformer &transformer,
	                                                   optional_ptr<ParseResult> parse_result);
	static pair<string, Value> TransformRelOption(PEGTransformer &transformer, optional_ptr<ParseResult> parse_result);
	static string TransformIndexName(PEGTransformer &transformer, optional_ptr<ParseResult> parse_result);

	// create_macro.gram
	static unique_ptr<CreateStatement> TransformCreateMacroStmt(PEGTransformer &transformer,
	                                                            optional_ptr<ParseResult> parse_result);
	static unique_ptr<MacroFunction> TransformMacroDefinition(PEGTransformer &transformer,
	                                                          optional_ptr<ParseResult> parse_result);
	static unique_ptr<MacroFunction> TransformTableMacroDefinition(PEGTransformer &transformer,
	                                                               optional_ptr<ParseResult> parse_result);
	static unique_ptr<MacroFunction> TransformScalarMacroDefinition(PEGTransformer &transformer,
	                                                                optional_ptr<ParseResult> parse_result);
	static vector<MacroParameter> TransformMacroParameters(PEGTransformer &transformer,
	                                                       optional_ptr<ParseResult> parse_result);
	static MacroParameter TransformMacroParameter(PEGTransformer &transformer, optional_ptr<ParseResult> parse_result);
	static MacroParameter TransformSimpleParameter(PEGTransformer &transformer, optional_ptr<ParseResult> parse_result);

	// create_schema.gram
	static unique_ptr<CreateStatement> TransformCreateSchemaStmt(PEGTransformer &transformer,
	                                                             optional_ptr<ParseResult> parse_result);

	// create_secret.gram
	static unique_ptr<CreateStatement> TransformCreateSecretStmt(PEGTransformer &transformer,
	                                                             optional_ptr<ParseResult> parse_result);
	static string TransformSecretStorageSpecifier(PEGTransformer &transformer, optional_ptr<ParseResult> parse_result);
	static string TransformSecretName(PEGTransformer &transformer, optional_ptr<ParseResult> parse_result);

	// create_sequence.gram
	static unique_ptr<CreateStatement> TransformCreateSequenceStmt(PEGTransformer &transformer,
	                                                               optional_ptr<ParseResult> parse_result);
	static pair<string, unique_ptr<SequenceOption>> TransformSequenceOption(PEGTransformer &transformer,
	                                                                        optional_ptr<ParseResult> parse_result);
	static pair<string, unique_ptr<SequenceOption>> TransformSeqSetCycle(PEGTransformer &transformer,
	                                                                     optional_ptr<ParseResult> parse_result);
	static pair<string, unique_ptr<SequenceOption>> TransformSeqSetIncrement(PEGTransformer &transformer,
	                                                                         optional_ptr<ParseResult> parse_result);
	static pair<string, unique_ptr<SequenceOption>> TransformSeqSetMinMax(PEGTransformer &transformer,
	                                                                      optional_ptr<ParseResult> parse_result);
	static string TransformSeqMinOrMax(PEGTransformer &transformer, optional_ptr<ParseResult> parse_result);
	static pair<string, unique_ptr<SequenceOption>> TransformNoMinMax(PEGTransformer &transformer,
	                                                                  optional_ptr<ParseResult> parse_result);
	static pair<string, unique_ptr<SequenceOption>> TransformSeqStartWith(PEGTransformer &transformer,
	                                                                      optional_ptr<ParseResult> parse_result);
	static pair<string, unique_ptr<SequenceOption>> TransformSeqOwnedBy(PEGTransformer &transformer,
	                                                                    optional_ptr<ParseResult> parse_result);

	// create_table.gram
	static unique_ptr<SQLStatement> TransformCreateStatement(PEGTransformer &transformer,
	                                                         optional_ptr<ParseResult> parse_result);
	static SecretPersistType TransformTemporary(PEGTransformer &transformer, optional_ptr<ParseResult> parse_result);
	static unique_ptr<CreateStatement> TransformCreateStatementVariation(PEGTransformer &transformer,
	                                                                     optional_ptr<ParseResult> parse_result);
	static unique_ptr<CreateStatement> TransformCreateTableStmt(PEGTransformer &transformer,
	                                                            optional_ptr<ParseResult> parse_result);
	static CreateTableAs TransformCreateTableAs(PEGTransformer &transformer, optional_ptr<ParseResult> parse_result);
	static ColumnList TransformIdentifierList(PEGTransformer &transformer, optional_ptr<ParseResult> parse_result);
	static ColumnElements TransformCreateColumnList(PEGTransformer &transformer,
	                                                optional_ptr<ParseResult> parse_result);
	static ColumnElements TransformCreateTableColumnList(PEGTransformer &transformer,
	                                                     optional_ptr<ParseResult> parse_result);

	static QualifiedName TransformIdentifierOrStringLiteral(PEGTransformer &transformer,
	                                                        optional_ptr<ParseResult> parse_result);
	static string TransformColIdOrString(PEGTransformer &transformer, optional_ptr<ParseResult> parse_result);
	static string TransformColLabelOrString(PEGTransformer &transformer, optional_ptr<ParseResult> parse_result);
	static string TransformColId(PEGTransformer &transformer, optional_ptr<ParseResult> parse_result);
	static vector<string> TransformColumnIdList(PEGTransformer &transformer, optional_ptr<ParseResult> parse_result);
	static string TransformTypeFuncName(PEGTransformer &transformer, optional_ptr<ParseResult> parse_result);
	static string TransformIdentifier(PEGTransformer &transformer, optional_ptr<ParseResult> parse_result);
	static vector<string> TransformDottedIdentifier(PEGTransformer &transformer,
	                                                optional_ptr<ParseResult> parse_result);
	static ColumnDefinition TransformColumnDefinition(PEGTransformer &transformer,
	                                                  optional_ptr<ParseResult> parse_result);
	static LogicalType TransformTypeOrGenerated(PEGTransformer &transformer, optional_ptr<ParseResult> parse_result);
	static unique_ptr<Constraint> TransformTopLevelConstraint(PEGTransformer &transformer,
	                                                          optional_ptr<ParseResult> parse_result);
	static unique_ptr<Constraint> TransformTopLevelConstraintList(PEGTransformer &transformer,
	                                                              optional_ptr<ParseResult> parse_result);
	static unique_ptr<Constraint> TransformTopPrimaryKeyConstraint(PEGTransformer &transformer,
	                                                               optional_ptr<ParseResult> parse_result);
	static unique_ptr<Constraint> TransformTopUniqueConstraint(PEGTransformer &transformer,
	                                                           optional_ptr<ParseResult> parse_result);
	static unique_ptr<Constraint> TransformCheckConstraint(PEGTransformer &transformer,
	                                                       optional_ptr<ParseResult> parse_result);
	static unique_ptr<Constraint> TransformTopForeignKeyConstraint(PEGTransformer &transformer,
	                                                               optional_ptr<ParseResult> parse_result);
	static unique_ptr<ParsedExpression> TransformDefaultValue(PEGTransformer &transformer,
	                                                          optional_ptr<ParseResult> parse_result);

	// create_type.gram
	static unique_ptr<CreateStatement> TransformCreateTypeStmt(PEGTransformer &transformer,
	                                                           optional_ptr<ParseResult> parse_result);
	static unique_ptr<CreateTypeInfo> TransformCreateType(PEGTransformer &transformer,
	                                                      optional_ptr<ParseResult> parse_result);
	static unique_ptr<SelectStatement> TransformEnumSelectType(PEGTransformer &transformer,
	                                                           optional_ptr<ParseResult> parse_result);
	static LogicalType TransformEnumStringLiteralList(PEGTransformer &transformer,
	                                                  optional_ptr<ParseResult> parse_result);

	// create_view.gram
	static unique_ptr<CreateStatement> TransformCreateViewStmt(PEGTransformer &transformer,
	                                                           optional_ptr<ParseResult> parse_result);

	// deallocate.gram
	static unique_ptr<SQLStatement> TransformDeallocateStatement(PEGTransformer &transformer,
	                                                             optional_ptr<ParseResult> parse_result);

	// delete.gram
	static unique_ptr<SQLStatement> TransformDeleteStatement(PEGTransformer &transformer,
	                                                         optional_ptr<ParseResult> parse_result);
	static unique_ptr<BaseTableRef> TransformTargetOptAlias(PEGTransformer &transformer,
	                                                        optional_ptr<ParseResult> parse_result);

	static vector<unique_ptr<TableRef>> TransformDeleteUsingClause(PEGTransformer &transformer,
	                                                               optional_ptr<ParseResult> parse_result);
	static unique_ptr<SQLStatement> TransformTruncateStatement(PEGTransformer &transformer,
	                                                           optional_ptr<ParseResult> parse_result);

	// detach.gram
	static unique_ptr<SQLStatement> TransformDetachStatement(PEGTransformer &transformer,
	                                                         optional_ptr<ParseResult> parse_result);

	// drop.gram
	static unique_ptr<SQLStatement> TransformDropStatement(PEGTransformer &transformer,
	                                                       optional_ptr<ParseResult> parse_result);
	static unique_ptr<DropStatement> TransformDropEntries(PEGTransformer &transformer,
	                                                      optional_ptr<ParseResult> parse_result);
	static unique_ptr<DropStatement> TransformDropTable(PEGTransformer &transformer,
	                                                    optional_ptr<ParseResult> parse_result);
	static CatalogType TransformTableOrView(PEGTransformer &transformer, optional_ptr<ParseResult> parse_result);
	static unique_ptr<DropStatement> TransformDropTableFunction(PEGTransformer &transformer,
	                                                            optional_ptr<ParseResult> parse_result);
	static unique_ptr<DropStatement> TransformDropFunction(PEGTransformer &transformer,
	                                                       optional_ptr<ParseResult> parse_result);
	static unique_ptr<DropStatement> TransformDropSchema(PEGTransformer &transformer,
	                                                     optional_ptr<ParseResult> parse_result);
	static QualifiedName TransformQualifiedSchemaName(PEGTransformer &transformer,
	                                                  optional_ptr<ParseResult> parse_result);
	static unique_ptr<DropStatement> TransformDropIndex(PEGTransformer &transformer,
	                                                    optional_ptr<ParseResult> parse_result);
	static QualifiedName TransformQualifiedIndexName(PEGTransformer &transformer,
	                                                 optional_ptr<ParseResult> parse_result);
	static unique_ptr<DropStatement> TransformDropSequence(PEGTransformer &transformer,
	                                                       optional_ptr<ParseResult> parse_result);
	static unique_ptr<DropStatement> TransformDropCollation(PEGTransformer &transformer,
	                                                        optional_ptr<ParseResult> parse_result);
	static unique_ptr<DropStatement> TransformDropType(PEGTransformer &transformer,
	                                                   optional_ptr<ParseResult> parse_result);
	static bool TransformDropBehavior(PEGTransformer &transformer, optional_ptr<ParseResult> parse_result);
	static unique_ptr<DropStatement> TransformDropSecret(PEGTransformer &transformer,
	                                                     optional_ptr<ParseResult> parse_result);
	static string TransformDropSecretStorage(PEGTransformer &transformer, optional_ptr<ParseResult> parse_result);

	// export.gram
	static unique_ptr<SQLStatement> TransformExportStatement(PEGTransformer &transformer,
	                                                         optional_ptr<ParseResult> parse_result);
	static string TransformExportSource(PEGTransformer &transformer, optional_ptr<ParseResult> parse_result);

	// expression.gram
	static unique_ptr<ParsedExpression> TransformBaseExpression(PEGTransformer &transformer,
	                                                            optional_ptr<ParseResult> parse_result);
	static unique_ptr<ParsedExpression> TransformExpression(PEGTransformer &transformer,
	                                                        optional_ptr<ParseResult> parse_result);
	static unique_ptr<ParsedExpression> TransformLambdaArrowExpression(PEGTransformer &transformer,
	                                                                   optional_ptr<ParseResult> parse_result);
	static unique_ptr<ParsedExpression> TransformLogicalOrExpression(PEGTransformer &transformer,
	                                                                 optional_ptr<ParseResult> parse_result);
	static unique_ptr<ParsedExpression> TransformLogicalAndExpression(PEGTransformer &transformer,
	                                                                  optional_ptr<ParseResult> parse_result);
	static unique_ptr<ParsedExpression> TransformLogicalNotExpression(PEGTransformer &transformer,
	                                                                  optional_ptr<ParseResult> parse_result);
	static unique_ptr<ParsedExpression> TransformIsExpression(PEGTransformer &transformer,
	                                                          optional_ptr<ParseResult> parse_result);
	static unique_ptr<ParsedExpression> TransformIsTest(PEGTransformer &transformer,
	                                                    optional_ptr<ParseResult> parse_result);
	static unique_ptr<ParsedExpression> TransformIsLiteral(PEGTransformer &transformer,
	                                                       optional_ptr<ParseResult> parse_result);
	static unique_ptr<ParsedExpression> TransformIsNotNull(PEGTransformer &transformer,
	                                                       optional_ptr<ParseResult> parse_result);
	static unique_ptr<ParsedExpression> TransformIsNull(PEGTransformer &transformer,
	                                                    optional_ptr<ParseResult> parse_result);
	static unique_ptr<ParsedExpression> TransformIsDistinctFromExpression(PEGTransformer &transformer,
	                                                                      optional_ptr<ParseResult> parse_result);
	static unique_ptr<ParsedExpression> TransformComparisonExpression(PEGTransformer &transformer,
	                                                                  optional_ptr<ParseResult> parse_result);
	static ExpressionType TransformComparisonOperator(PEGTransformer &transformer,
	                                                  optional_ptr<ParseResult> parse_result);
	static unique_ptr<ParsedExpression> TransformBetweenInLikeExpression(PEGTransformer &transformer,
	                                                                     optional_ptr<ParseResult> parse_result);
	static unique_ptr<ParsedExpression> TransformBetweenInLikeOp(PEGTransformer &transformer,
	                                                             optional_ptr<ParseResult> parse_result);
	static unique_ptr<ParsedExpression> TransformBetweenClause(PEGTransformer &transformer,
	                                                           optional_ptr<ParseResult> parse_result);
	static unique_ptr<ParsedExpression> TransformLikeClause(PEGTransformer &transformer,
	                                                        optional_ptr<ParseResult> parse_result);
	static string TransformLikeVariations(PEGTransformer &transformer, optional_ptr<ParseResult> parse_result);
	static unique_ptr<ParsedExpression> TransformOtherOperatorExpression(PEGTransformer &transformer,
	                                                                     optional_ptr<ParseResult> parse_result);
	static string TransformOtherOperator(PEGTransformer &transformer, optional_ptr<ParseResult> parse_result);
	static string TransformStringOperator(PEGTransformer &transformer, optional_ptr<ParseResult> parse_result);
	static ExpressionType TransformLambdaOperator(PEGTransformer &transformer, optional_ptr<ParseResult> parse_result);
	static unique_ptr<ParsedExpression> TransformBitwiseExpression(PEGTransformer &transformer,
	                                                               optional_ptr<ParseResult> parse_result);
	static unique_ptr<ParsedExpression> TransformAdditiveExpression(PEGTransformer &transformer,
	                                                                optional_ptr<ParseResult> parse_result);
	static string TransformTerm(PEGTransformer &transformer, optional_ptr<ParseResult> parse_result);
	static unique_ptr<ParsedExpression> TransformMultiplicativeExpression(PEGTransformer &transformer,
	                                                                      optional_ptr<ParseResult> parse_result);
	static string TransformFactor(PEGTransformer &transformer, optional_ptr<ParseResult> parse_result);
	static unique_ptr<ParsedExpression> TransformExponentiationExpression(PEGTransformer &transformer,
	                                                                      optional_ptr<ParseResult> parse_result);
	static unique_ptr<ParsedExpression> TransformCollateExpression(PEGTransformer &transformer,
	                                                               optional_ptr<ParseResult> parse_result);
	static unique_ptr<ParsedExpression> TransformAtTimeZoneExpression(PEGTransformer &transformer,
	                                                                  optional_ptr<ParseResult> parse_result);
	static unique_ptr<ParsedExpression> TransformPrefixExpression(PEGTransformer &transformer,
	                                                              optional_ptr<ParseResult> parse_result);
	static unique_ptr<ParsedExpression> TransformConstantLiteral(PEGTransformer &transformer,
	                                                             optional_ptr<ParseResult> parse_result);
	static unique_ptr<ColumnRefExpression> TransformNestedColumnName(PEGTransformer &transformer,
	                                                                 optional_ptr<ParseResult> parse_result);
	static unique_ptr<ParsedExpression> TransformColumnReference(PEGTransformer &transformer,
	                                                             optional_ptr<ParseResult> parse_result);
	static unique_ptr<ColumnRefExpression>
	TransformSchemaReservedTableColumnName(PEGTransformer &transformer, optional_ptr<ParseResult> parse_result);
	static string TransformReservedTableQualification(PEGTransformer &transformer,
	                                                  optional_ptr<ParseResult> parse_result);
	static unique_ptr<ParsedExpression> TransformLiteralExpression(PEGTransformer &transformer,
	                                                               optional_ptr<ParseResult> parse_result);
	static unique_ptr<ParsedExpression> TransformParensExpression(PEGTransformer &transformer,
	                                                              optional_ptr<ParseResult> parse_result);
	static unique_ptr<ParsedExpression> TransformSingleExpression(PEGTransformer &transformer,
	                                                              optional_ptr<ParseResult> parse_result);
	static string TransformPrefixOperator(PEGTransformer &transformer, optional_ptr<ParseResult> parse_result);
	static unique_ptr<ParsedExpression> TransformListExpression(PEGTransformer &transformer,
	                                                            optional_ptr<ParseResult> parse_result);
	static unique_ptr<ParsedExpression> TransformArrayBoundedListExpression(PEGTransformer &transformer,
	                                                                        optional_ptr<ParseResult> parse_result);
	static unique_ptr<ParsedExpression> TransformArrayParensSelect(PEGTransformer &transformer,
	                                                               optional_ptr<ParseResult> parse_result);
	static unique_ptr<ParsedExpression> TransformStructExpression(PEGTransformer &transformer,
	                                                              optional_ptr<ParseResult> parse_result);
	static unique_ptr<ParsedExpression> TransformStructField(PEGTransformer &transformer,
	                                                         optional_ptr<ParseResult> parse_result);
	static vector<unique_ptr<ParsedExpression>> TransformBoundedListExpression(PEGTransformer &transformer,
	                                                                           optional_ptr<ParseResult> parse_result);
	static unique_ptr<ParsedExpression> TransformFunctionExpression(PEGTransformer &transformer,
	                                                                optional_ptr<ParseResult> parse_result);
	static QualifiedName TransformFunctionIdentifier(PEGTransformer &transformer,
	                                                 optional_ptr<ParseResult> parse_result);
	static QualifiedName TransformSchemaReservedFunctionName(PEGTransformer &transformer,
	                                                         optional_ptr<ParseResult> parse_result);
	static QualifiedName TransformCatalogReservedSchemaFunctionName(PEGTransformer &transformer,
	                                                                optional_ptr<ParseResult> parse_result);

	static ExpressionType TransformOperator(PEGTransformer &transformer, optional_ptr<ParseResult> parse_result);
	static ExpressionType TransformConjunctionOperator(PEGTransformer &transformer,
	                                                   optional_ptr<ParseResult> parse_result);
	static ExpressionType TransformIsOperator(PEGTransformer &transformer, optional_ptr<ParseResult> parse_result);
	static ExpressionType TransformInOperator(PEGTransformer &transformer, optional_ptr<ParseResult> parse_result);
	static ExpressionType TransformBetweenOperator(PEGTransformer &transformer, optional_ptr<ParseResult> parse_result);
	static unique_ptr<ParsedExpression> TransformParenthesisExpression(PEGTransformer &transformer,
	                                                                   optional_ptr<ParseResult> parse_result);

	static unique_ptr<ParsedExpression> TransformIndirection(PEGTransformer &transformer,
	                                                         optional_ptr<ParseResult> parse_result);
	static unique_ptr<ParsedExpression> TransformCastOperator(PEGTransformer &transformer,
	                                                          optional_ptr<ParseResult> parse_result);
	static unique_ptr<ParsedExpression> TransformDotOperator(PEGTransformer &transformer,
	                                                         optional_ptr<ParseResult> parse_result);
	static unique_ptr<ParsedExpression> TransformSliceExpression(PEGTransformer &transformer,
	                                                             optional_ptr<ParseResult> parse_result);
	static vector<unique_ptr<ParsedExpression>> TransformSliceBound(PEGTransformer &transformer,
	                                                                optional_ptr<ParseResult> parse_result);
	static unique_ptr<ParsedExpression> TransformEndSliceBound(PEGTransformer &transformer,
	                                                           optional_ptr<ParseResult> parse_result);
	static unique_ptr<ParsedExpression> TransformStepSliceBound(PEGTransformer &transformer,
	                                                            optional_ptr<ParseResult> parse_result);

	static unique_ptr<ColumnRefExpression> TransformTableReservedColumnName(PEGTransformer &transformer,
	                                                                        optional_ptr<ParseResult> parse_result);
	static string TransformTableQualification(PEGTransformer &transformer, optional_ptr<ParseResult> parse_result);
	static unique_ptr<ParsedExpression> TransformStarExpression(PEGTransformer &transformer,
	                                                            optional_ptr<ParseResult> parse_result);

	static unique_ptr<WindowExpression> TransformOverClause(PEGTransformer &transformer,
	                                                        optional_ptr<ParseResult> parse_result);
	static unique_ptr<WindowExpression> TransformWindowFrame(PEGTransformer &transformer,
	                                                         optional_ptr<ParseResult> parse_result);
	static unique_ptr<WindowExpression> TransformWindowFrameDefinition(PEGTransformer &transformer,
	                                                                   optional_ptr<ParseResult> parse_result);
	static unique_ptr<WindowExpression> TransformWindowFrameContentsParens(PEGTransformer &transformer,
	                                                                       optional_ptr<ParseResult> parse_result);
	static unique_ptr<WindowExpression> TransformWindowFrameNameContentsParens(PEGTransformer &transformer,
	                                                                           optional_ptr<ParseResult> parse_result);
	static unique_ptr<WindowExpression> TransformWindowFrameContents(PEGTransformer &transformer,
	                                                                 optional_ptr<ParseResult> parse_result);
	static vector<unique_ptr<ParsedExpression>> TransformWindowPartition(PEGTransformer &transformer,
	                                                                     optional_ptr<ParseResult> parse_result);

	static unique_ptr<ParsedExpression> TransformSpecialFunctionExpression(PEGTransformer &transformer,
	                                                                       optional_ptr<ParseResult> parse_result);
	static unique_ptr<ParsedExpression> TransformCoalesceExpression(PEGTransformer &transformer,
	                                                                optional_ptr<ParseResult> parse_result);
	static unique_ptr<ParsedExpression> TransformUnpackExpression(PEGTransformer &transformer,
	                                                              optional_ptr<ParseResult> parse_result);
	static unique_ptr<ParsedExpression> TransformColumnsExpression(PEGTransformer &transformer,
	                                                               optional_ptr<ParseResult> parse_result);
	static unique_ptr<ParsedExpression> TransformExtractExpression(PEGTransformer &transformer,
	                                                               optional_ptr<ParseResult> parse_result);
	static unique_ptr<ParsedExpression> TransformExtractArgument(PEGTransformer &transformer,
	                                                             optional_ptr<ParseResult> parse_result);
	static unique_ptr<ParsedExpression> TransformLambdaExpression(PEGTransformer &transformer,
	                                                              optional_ptr<ParseResult> parse_result);
	static unique_ptr<ParsedExpression> TransformNullIfExpression(PEGTransformer &transformer,
	                                                              optional_ptr<ParseResult> parse_result);
	static unique_ptr<ParsedExpression> TransformRowExpression(PEGTransformer &transformer,
	                                                           optional_ptr<ParseResult> parse_result);
	static unique_ptr<ParsedExpression> TransformTrimExpression(PEGTransformer &transformer,
	                                                            optional_ptr<ParseResult> parse_result);
	static string TransformTrimDirection(PEGTransformer &transformer, optional_ptr<ParseResult> parse_result);
	static unique_ptr<ParsedExpression> TransformTrimSource(PEGTransformer &transformer,
	                                                        optional_ptr<ParseResult> parse_result);
	static unique_ptr<ParsedExpression> TransformPositionExpression(PEGTransformer &transformer,
	                                                                optional_ptr<ParseResult> parse_result);
	static unique_ptr<ParsedExpression> TransformCastExpression(PEGTransformer &transformer,
	                                                            optional_ptr<ParseResult> parse_result);
	static bool TransformCastOrTryCast(PEGTransformer &transformer, optional_ptr<ParseResult> parse_result);
	static unique_ptr<ParsedExpression> TransformCaseExpression(PEGTransformer &transformer,
	                                                            optional_ptr<ParseResult> parse_result);
	static unique_ptr<ParsedExpression> TransformCaseElse(PEGTransformer &transformer,
	                                                      optional_ptr<ParseResult> parse_result);
	static CaseCheck TransformCaseWhenThen(PEGTransformer &transformer, optional_ptr<ParseResult> parse_result);
	static unique_ptr<ParsedExpression> TransformTypeLiteral(PEGTransformer &transformer,
	                                                         optional_ptr<ParseResult> parse_result);

	// import.gram
	static unique_ptr<SQLStatement> TransformImportStatement(PEGTransformer &transformer,
	                                                         optional_ptr<ParseResult> parse_result);

	// insert.gram
	static unique_ptr<SQLStatement> TransformInsertStatement(PEGTransformer &transformer,
	                                                         optional_ptr<ParseResult> parse_result);
	static unique_ptr<BaseTableRef> TransformInsertTarget(PEGTransformer &transformer,
	                                                      optional_ptr<ParseResult> parse_result);
	static unique_ptr<OnConflictInfo> TransformOnConflictClause(PEGTransformer &transformer,
	                                                            optional_ptr<ParseResult> parse_result);
	static OnConflictExpressionTarget TransformOnConflictExpressionTarget(PEGTransformer &transformer,
	                                                                      optional_ptr<ParseResult> parse_result);
	static OnConflictAction TransformOnConflictAction(PEGTransformer &transformer,
	                                                  optional_ptr<ParseResult> parse_result);
	static OnConflictAction TransformOnConflictUpdate(PEGTransformer &transformer,
	                                                  optional_ptr<ParseResult> parse_result);
	static OnConflictAction TransformOnConflictNothing(PEGTransformer &transformer,
	                                                   optional_ptr<ParseResult> parse_result);
	static InsertValues TransformInsertValues(PEGTransformer &transformer, optional_ptr<ParseResult> parse_result);
	static InsertColumnOrder TransformByNameOrPosition(PEGTransformer &transformer,
	                                                   optional_ptr<ParseResult> parse_result);
	static vector<string> TransformInsertColumnList(PEGTransformer &transformer,
	                                                optional_ptr<ParseResult> parse_result);
	static vector<string> TransformColumnList(PEGTransformer &transformer, optional_ptr<ParseResult> parse_result);
	static vector<unique_ptr<ParsedExpression>> TransformReturningClause(PEGTransformer &transformer,
	                                                                     optional_ptr<ParseResult> parse_result);

	// load.gram
	static unique_ptr<SQLStatement> TransformLoadStatement(PEGTransformer &transformer,
	                                                       optional_ptr<ParseResult> parse_result);
	static unique_ptr<SQLStatement> TransformInstallStatement(PEGTransformer &transformer,
	                                                          optional_ptr<ParseResult> parse_result);
	static ExtensionRepositoryInfo TransformFromSource(PEGTransformer &transformer,
	                                                   optional_ptr<ParseResult> parse_result);
	static string TransformVersionNumber(PEGTransformer &transformer, optional_ptr<ParseResult> parse_result);

	// pragma.gram
	static unique_ptr<SQLStatement> TransformPragmaStatement(PEGTransformer &transformer,
	                                                         optional_ptr<ParseResult> parse_result);
	static unique_ptr<SQLStatement> TransformPragmaAssign(PEGTransformer &transformer,
	                                                      optional_ptr<ParseResult> parse_result);
	static unique_ptr<SQLStatement> TransformPragmaFunction(PEGTransformer &transformer,
	                                                        optional_ptr<ParseResult> parse_result);
	static vector<unique_ptr<ParsedExpression>> TransformPragmaParameters(PEGTransformer &transformer,
	                                                                      optional_ptr<ParseResult> parse_result);

	// select.gram
	static unique_ptr<ParsedExpression> TransformFunctionArgument(PEGTransformer &transformer,
	                                                              optional_ptr<ParseResult> parse_result);
	static MacroParameter TransformNamedParameter(PEGTransformer &transformer, optional_ptr<ParseResult> parse_result);
	static vector<unique_ptr<ParsedExpression>> TransformTableFunctionArguments(PEGTransformer &transformer,
	                                                                            optional_ptr<ParseResult> parse_result);

	static unique_ptr<BaseTableRef> TransformBaseTableName(PEGTransformer &transformer,
	                                                       optional_ptr<ParseResult> parse_result);
	static unique_ptr<BaseTableRef> TransformSchemaReservedTable(PEGTransformer &transformer,
	                                                             optional_ptr<ParseResult> parse_result);
	static unique_ptr<BaseTableRef> TransformCatalogReservedSchemaTable(PEGTransformer &transformer,
	                                                                    optional_ptr<ParseResult> parse_result);
	static string TransformSchemaQualification(PEGTransformer &transformer, optional_ptr<ParseResult> parse_result);
	static string TransformCatalogQualification(PEGTransformer &transformer, optional_ptr<ParseResult> parse_result);
	static QualifiedName TransformQualifiedName(PEGTransformer &transformer, optional_ptr<ParseResult> parse_result);
	static QualifiedName
	TransformCatalogReservedSchemaIdentifierOrStringLiteral(PEGTransformer &transformer,
	                                                        optional_ptr<ParseResult> parse_result);
	static QualifiedName TransformCatalogReservedSchemaIdentifier(PEGTransformer &transformer,
	                                                              optional_ptr<ParseResult> parse_result);

	static QualifiedName TransformSchemaReservedIdentifierOrStringLiteral(PEGTransformer &transformer,
	                                                                      optional_ptr<ParseResult> parse_result);
	static string TransformReservedIdentifierOrStringLiteral(PEGTransformer &transformer,
	                                                         optional_ptr<ParseResult> parse_result);
	static QualifiedName TransformTableNameIdentifierOrStringLiteral(PEGTransformer &transformer,
	                                                                 optional_ptr<ParseResult> parse_result);
	static unique_ptr<ParsedExpression> TransformWhereClause(PEGTransformer &transformer,
	                                                         optional_ptr<ParseResult> parse_result);

	static vector<unique_ptr<ParsedExpression>> TransformTargetList(PEGTransformer &transformer,
	                                                                optional_ptr<ParseResult> parse_result);
	static unique_ptr<ParsedExpression> TransformAliasedExpression(PEGTransformer &transformer,
	                                                               optional_ptr<ParseResult> parse_result);
	static unique_ptr<ParsedExpression> TransformExpressionAsCollabel(PEGTransformer &transformer,
	                                                                  optional_ptr<ParseResult> parse_result);
	static unique_ptr<ParsedExpression> TransformColIdExpression(PEGTransformer &transformer,
	                                                             optional_ptr<ParseResult> parse_result);
	static unique_ptr<ParsedExpression> TransformExpressionOptIdentifier(PEGTransformer &transformer,
	                                                                     optional_ptr<ParseResult> parse_result);

	static vector<OrderByNode> TransformOrderByClause(PEGTransformer &transformer,
	                                                  optional_ptr<ParseResult> parse_result);
	static vector<OrderByNode> TransformOrderByExpressions(PEGTransformer &transformer,
	                                                       optional_ptr<ParseResult> parse_result);
	static vector<OrderByNode> TransformOrderByExpressionList(PEGTransformer &transformer,
	                                                          optional_ptr<ParseResult> parse_result);
	static vector<OrderByNode> TransformOrderByAll(PEGTransformer &transformer, optional_ptr<ParseResult> parse_result);
	static OrderByNode TransformOrderByExpression(PEGTransformer &transformer, optional_ptr<ParseResult> parse_result);
	static OrderType TransformDescOrAsc(PEGTransformer &transformer, optional_ptr<ParseResult> parse_result);
	static OrderByNullType TransformNullsFirstOrLast(PEGTransformer &transformer,
	                                                 optional_ptr<ParseResult> parse_result);

	static unique_ptr<TableRef> TransformTableRef(PEGTransformer &transformer, optional_ptr<ParseResult> parse_result);
	static unique_ptr<TableRef> TransformJoinOrPivot(PEGTransformer &transformer,
	                                                 optional_ptr<ParseResult> parse_result);
	static unique_ptr<TableRef> TransformJoinClause(PEGTransformer &transformer,
	                                                optional_ptr<ParseResult> parse_result);
	static unique_ptr<TableRef> TransformRegularJoinClause(PEGTransformer &transformer,
	                                                       optional_ptr<ParseResult> parse_result);
	static JoinType TransformJoinType(PEGTransformer &transformer, optional_ptr<ParseResult> parse_result);
	static JoinQualifier TransformJoinQualifier(PEGTransformer &transformer, optional_ptr<ParseResult> parse_result);
	static JoinQualifier TransformOnClause(PEGTransformer &transformer, optional_ptr<ParseResult> parse_result);
	static JoinQualifier TransformUsingClause(PEGTransformer &transformer, optional_ptr<ParseResult> parse_result);
	static unique_ptr<TableRef> TransformJoinWithoutOnClause(PEGTransformer &transformer,
	                                                         optional_ptr<ParseResult> parse_result);
	static JoinPrefix TransformJoinPrefix(PEGTransformer &transformer, optional_ptr<ParseResult> parse_result);
	static JoinPrefix TransformCrossJoinPrefix(PEGTransformer &transformer, optional_ptr<ParseResult> parse_result);
	static JoinPrefix TransformNaturalJoinPrefix(PEGTransformer &transformer, optional_ptr<ParseResult> parse_result);
	static JoinPrefix TransformPositionalJoinPrefix(PEGTransformer &transformer,
	                                                optional_ptr<ParseResult> parse_result);
	static unique_ptr<TableRef> TransformInnerTableRef(PEGTransformer &transformer,
	                                                   optional_ptr<ParseResult> parse_result);
	static unique_ptr<TableRef> TransformTableFunction(PEGTransformer &transformer,
	                                                   optional_ptr<ParseResult> parse_result);
	static unique_ptr<TableRef> TransformTableFunctionLateralOpt(PEGTransformer &transformer,
	                                                             optional_ptr<ParseResult> parse_result);
	static unique_ptr<TableRef> TransformTableFunctionAliasColon(PEGTransformer &transformer,
	                                                             optional_ptr<ParseResult> parse_result);
	static string TransformTableAliasColon(PEGTransformer &transformer, optional_ptr<ParseResult> parse_result);
	static QualifiedName TransformQualifiedTableFunction(PEGTransformer &transformer,
	                                                     optional_ptr<ParseResult> parse_result);
	static unique_ptr<TableRef> TransformTableSubquery(PEGTransformer &transformer,
	                                                   optional_ptr<ParseResult> parse_result);
	static unique_ptr<TableRef> TransformSubqueryReference(PEGTransformer &transformer,
	                                                       optional_ptr<ParseResult> parse_result);
	static unique_ptr<TableRef> TransformBaseTableRef(PEGTransformer &transformer,
	                                                  optional_ptr<ParseResult> parse_result);
	static unique_ptr<AtClause> TransformAtClause(PEGTransformer &transformer, optional_ptr<ParseResult> parse_result);
	static unique_ptr<AtClause> TransformAtSpecifier(PEGTransformer &transformer,
	                                                 optional_ptr<ParseResult> parse_result);
	static string TransformAtUnit(PEGTransformer &transformer, optional_ptr<ParseResult> parse_result);
	static unique_ptr<TableRef> TransformValuesRef(PEGTransformer &transformer, optional_ptr<ParseResult> parse_result);
	static unique_ptr<SelectStatement> TransformValuesClause(PEGTransformer &transformer,
	                                                         optional_ptr<ParseResult> parse_result);
	static vector<unique_ptr<ParsedExpression>> TransformValuesExpressions(PEGTransformer &transformer,
	                                                                       optional_ptr<ParseResult> parse_result);
	static unique_ptr<SelectStatement> TransformTableStatement(PEGTransformer &transformer,
	                                                           optional_ptr<ParseResult> parse_result);

	// use.gram
	static unique_ptr<SQLStatement> TransformUseStatement(PEGTransformer &transformer,
	                                                      optional_ptr<ParseResult> parse_result);
	static QualifiedName TransformUseTarget(PEGTransformer &transformer, optional_ptr<ParseResult> parse_result);

	// set.gram
	static unique_ptr<SQLStatement> TransformResetStatement(PEGTransformer &transformer,
	                                                        optional_ptr<ParseResult> parse_result);
	static vector<unique_ptr<ParsedExpression>> TransformSetAssignment(PEGTransformer &transformer,
	                                                                   optional_ptr<ParseResult> parse_result);
	static SettingInfo TransformSetSetting(PEGTransformer &transformer, optional_ptr<ParseResult> parse_result);
	static unique_ptr<SQLStatement> TransformSetStatement(PEGTransformer &transformer,
	                                                      optional_ptr<ParseResult> parse_result);
	static unique_ptr<SQLStatement> TransformSetTimeZone(PEGTransformer &transformer,
	                                                     optional_ptr<ParseResult> parse_result);
	static SettingInfo TransformSetVariable(PEGTransformer &transformer, optional_ptr<ParseResult> parse_result);
	static unique_ptr<SetVariableStatement> TransformStandardAssignment(PEGTransformer &transformer,
	                                                                    optional_ptr<ParseResult> parse_result);
	static vector<unique_ptr<ParsedExpression>> TransformVariableList(PEGTransformer &transformer,
	                                                                  optional_ptr<ParseResult> parse_result);

	static string TransformIdentifierOrKeyword(PEGTransformer &transformer, optional_ptr<ParseResult> parse_result);

	// transaction.gram
	static unique_ptr<SQLStatement> TransformTransactionStatement(PEGTransformer &transformer,
	                                                              optional_ptr<ParseResult> parse_result);
	static unique_ptr<TransactionStatement> TransformBeginTransaction(PEGTransformer &transformer,
	                                                                  optional_ptr<ParseResult> parse_result);
	static TransactionModifierType TransformReadOrWrite(PEGTransformer &transformer,
	                                                    optional_ptr<ParseResult> parse_result);
	static unique_ptr<TransactionStatement> TransformCommitTransaction(PEGTransformer &, optional_ptr<ParseResult>);
	static unique_ptr<TransactionStatement> TransformRollbackTransaction(PEGTransformer &, optional_ptr<ParseResult>);

private:
	PEGParser parser;
	case_insensitive_map_t<PEGTransformer::AnyTransformFunction> sql_transform_functions;
	case_insensitive_map_t<unique_ptr<TransformEnumValue>> enum_mappings;
};

} // namespace duckdb
