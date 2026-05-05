#include "duckdb/parser/peg/autocomplete_extension.hpp"
#include "duckdb/parser/peg/sql_formatter.hpp"
#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/view_catalog_entry.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/file_opener.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/settings.hpp"
#include "duckdb/main/extension/extension_loader.hpp"
#include "duckdb/parser/peg/transformer/peg_transformer.hpp"
#include "duckdb/parser/keyword_helper.hpp"
#include "duckdb/parser/peg/matcher.hpp"
#include "duckdb/parser/peg/autocomplete_catalog_provider.hpp"
#include "duckdb/main/attached_database.hpp"
#include "duckdb/parser/peg/tokenizer/base_tokenizer.hpp"
#include "duckdb/catalog/catalog_entry/pragma_function_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/scalar_function_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/table_function_catalog_entry.hpp"
#include "duckdb/parser/peg/tokenizer/highlight_tokenizer.hpp"
#include "duckdb/parser/peg/tokenizer/parser_tokenizer.hpp"
#include "duckdb/parser/parser_extension.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/common/vector_operations/unary_executor.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/parser/parser.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"

namespace duckdb {
struct AutoCompleteSuggestion;

struct SQLTokenizeFunctionData : public TableFunctionData {
	explicit SQLTokenizeFunctionData(vector<MatcherToken> tokens_p) : tokens(std::move(tokens_p)) {
	}

	vector<MatcherToken> tokens;
};

struct SQLTokenizeData : public GlobalTableFunctionState {
	SQLTokenizeData() : offset(0) {
	}

	idx_t offset;
};

struct SQLAutoCompleteFunctionData : public TableFunctionData {
	explicit SQLAutoCompleteFunctionData(vector<AutoCompleteSuggestion> suggestions_p)
	    : suggestions(std::move(suggestions_p)) {
	}

	vector<AutoCompleteSuggestion> suggestions;
};

struct SQLAutoCompleteData : public GlobalTableFunctionState {
	SQLAutoCompleteData() : offset(0) {
	}

	idx_t offset;
};

// AutoCompleteParameters is now in autocomplete_catalog_provider.hpp

// The following functions have been moved to autocomplete_core.cpp:
// - GetSuggestionType (now non-static, declared in autocomplete_catalog_provider.hpp)
// - PreferCaseMatching
// - ComputeSuggestions

static vector<shared_ptr<AttachedDatabase>> GetAllCatalogs(ClientContext &context) {
	vector<shared_ptr<AttachedDatabase>> result;

	auto &database_manager = DatabaseManager::Get(context);
	auto databases = database_manager.GetDatabases(context);
	for (auto &database : databases) {
		result.push_back(database);
	}
	return result;
}

static vector<reference<SchemaCatalogEntry>> GetAllSchemas(ClientContext &context) {
	return Catalog::GetAllSchemas(context);
}

static vector<reference<CatalogEntry>> GetAllTables(ClientContext &context, bool for_table_names) {
	vector<reference<CatalogEntry>> result;
	// scan all the schemas for tables and collect them and collect them
	// for column names we avoid adding internal entries, because it pollutes the auto-complete too much
	// for table names this is generally fine, however
	auto schemas = Catalog::GetAllSchemas(context);
	for (auto &schema_ref : schemas) {
		auto &schema = schema_ref.get();
		schema.Scan(context, CatalogType::TABLE_ENTRY, [&](CatalogEntry &entry) {
			if (!entry.internal || for_table_names) {
				result.push_back(entry);
			}
		});
	};
	if (for_table_names) {
		for (auto &schema_ref : schemas) {
			auto &schema = schema_ref.get();
			schema.Scan(context, CatalogType::TABLE_FUNCTION_ENTRY,
			            [&](CatalogEntry &entry) { result.push_back(entry); });
		};
	} else {
		for (auto &schema_ref : schemas) {
			auto &schema = schema_ref.get();
			schema.Scan(context, CatalogType::SCALAR_FUNCTION_ENTRY,
			            [&](CatalogEntry &entry) { result.push_back(entry); });
		};
	}
	return result;
}

static vector<reference<CatalogEntry>> GetAllTypes(ClientContext &context) {
	vector<reference<CatalogEntry>> result;
	// scan all the schemas for types and collect them
	auto schemas = Catalog::GetAllSchemas(context);
	for (auto &schema_ref : schemas) {
		auto &schema = schema_ref.get();
		schema.Scan(context, CatalogType::TYPE_ENTRY, [&](CatalogEntry &entry) { result.push_back(entry); });
	};
	return result;
}

static vector<AutoCompleteCandidate> SuggestCatalogName(ClientContext &context) {
	vector<AutoCompleteCandidate> suggestions;
	auto all_entries = GetAllCatalogs(context);
	for (auto &entry_ref : all_entries) {
		auto &entry = *entry_ref;
		AutoCompleteCandidate candidate(entry.name, SuggestionState::SUGGEST_CATALOG_NAME, 0);
		candidate.extra_char = '.';
		suggestions.push_back(std::move(candidate));
	}
	return suggestions;
}

static vector<AutoCompleteCandidate> SuggestSchemaName(ClientContext &context) {
	vector<AutoCompleteCandidate> suggestions;
	auto all_entries = GetAllSchemas(context);
	for (auto &entry_ref : all_entries) {
		auto &entry = entry_ref.get();
		AutoCompleteCandidate candidate(entry.name, SuggestionState::SUGGEST_SCHEMA_NAME, 0);
		candidate.extra_char = '.';
		suggestions.push_back(std::move(candidate));
	}
	return suggestions;
}

static vector<AutoCompleteCandidate> SuggestTableName(ClientContext &context) {
	vector<AutoCompleteCandidate> suggestions;
	auto all_entries = GetAllTables(context, true);
	for (auto &entry_ref : all_entries) {
		auto &entry = entry_ref.get();
		// prioritize user-defined entries (views & tables)
		int32_t bonus = (entry.internal || entry.type == CatalogType::TABLE_FUNCTION_ENTRY) ? 0 : 1;
		suggestions.emplace_back(entry.name, SuggestionState::SUGGEST_TABLE_NAME, bonus);
	}
	return suggestions;
}

static vector<AutoCompleteCandidate> SuggestType(ClientContext &context) {
	vector<AutoCompleteCandidate> suggestions;
	auto all_entries = GetAllTypes(context);
	for (auto &entry_ref : all_entries) {
		auto &entry = entry_ref.get();
		// prioritize user-defined types
		int32_t bonus = (entry.internal) ? 0 : 1;
		suggestions.emplace_back(entry.name, SuggestionState::SUGGEST_TYPE_NAME, bonus, CandidateType::KEYWORD);
	}
	return suggestions;
}
static vector<AutoCompleteCandidate> SuggestColumnName(ClientContext &context) {
	vector<AutoCompleteCandidate> suggestions;
	auto all_entries = GetAllTables(context, false);
	for (auto &entry_ref : all_entries) {
		auto &entry = entry_ref.get();
		if (entry.type == CatalogType::TABLE_ENTRY) {
			auto &table = entry.Cast<TableCatalogEntry>();
			int32_t bonus = entry.internal ? 0 : 3;
			for (auto &col : table.GetColumns().Logical()) {
				suggestions.emplace_back(col.GetName(), SuggestionState::SUGGEST_COLUMN_NAME, bonus);
			}
		} else if (entry.type == CatalogType::VIEW_ENTRY) {
			auto &view = entry.Cast<ViewCatalogEntry>();
			int32_t bonus = entry.internal ? 0 : 3;
			auto column_info = view.GetColumnInfo();
			if (column_info) {
				// view has names
				for (idx_t n = 0; n < column_info->names.size(); n++) {
					auto &name = n < view.aliases.size() ? view.aliases[n] : column_info->names[n];
					suggestions.emplace_back(name, SuggestionState::SUGGEST_COLUMN_NAME, bonus);
				}
			} else {
				// add only aliases
				for (auto &col : view.aliases) {
					suggestions.emplace_back(col, SuggestionState::SUGGEST_COLUMN_NAME, bonus);
				}
			}
		} else {
			if (StringUtil::CharacterIsOperator(entry.name[0])) {
				continue;
			}
			int32_t bonus = entry.internal ? 0 : 2;
			suggestions.emplace_back(entry.name, SuggestionState::SUGGEST_COLUMN_NAME, bonus);
		};
	}
	return suggestions;
}

static bool KnownExtension(const string &fname) {
	vector<string> known_extensions {".parquet", ".csv", ".tsv", ".csv.gz", ".tsv.gz", ".tbl"};
	for (auto &ext : known_extensions) {
		if (StringUtil::EndsWith(fname, ext)) {
			return true;
		}
	}
	return false;
}

static vector<AutoCompleteCandidate> SuggestPragmaName(ClientContext &context) {
	vector<AutoCompleteCandidate> suggestions;
	auto all_pragmas = Catalog::GetAllEntries(context, CatalogType::PRAGMA_FUNCTION_ENTRY);
	for (const auto &pragma : all_pragmas) {
		AutoCompleteCandidate candidate(pragma.get().name, SuggestionState::SUGGEST_PRAGMA_NAME, 0);
		suggestions.push_back(std::move(candidate));
	}
	return suggestions;
}

static vector<AutoCompleteCandidate> SuggestSettingName(ClientContext &context) {
	auto &db_config = DBConfig::GetConfig(context);
	const auto &options = db_config.GetOptions();
	vector<AutoCompleteCandidate> suggestions;
	for (const auto &option : options) {
		AutoCompleteCandidate candidate(option.name, SuggestionState::SUGGEST_SETTING_NAME, 0);
		suggestions.push_back(std::move(candidate));
	}
	const auto &option_aliases = db_config.GetAliases();
	for (const auto &option_alias : option_aliases) {
		AutoCompleteCandidate candidate(option_alias.alias, SuggestionState::SUGGEST_SETTING_NAME, 0);
		suggestions.push_back(std::move(candidate));
	}
	for (auto &entry : db_config.GetExtensionSettings()) {
		AutoCompleteCandidate candidate(entry.first, SuggestionState::SUGGEST_SETTING_NAME, 0);
		suggestions.push_back(std::move(candidate));
	}
	return suggestions;
}

static vector<AutoCompleteCandidate> SuggestScalarFunctionName(ClientContext &context) {
	vector<AutoCompleteCandidate> suggestions;
	auto scalar_functions = Catalog::GetAllEntries(context, CatalogType::SCALAR_FUNCTION_ENTRY);
	for (const auto &scalar_function : scalar_functions) {
		AutoCompleteCandidate candidate(scalar_function.get().name, SuggestionState::SUGGEST_SCALAR_FUNCTION_NAME, 0);
		suggestions.push_back(std::move(candidate));
	}

	return suggestions;
}

static vector<AutoCompleteCandidate> SuggestTableFunctionName(ClientContext &context) {
	vector<AutoCompleteCandidate> suggestions;
	auto table_functions = Catalog::GetAllEntries(context, CatalogType::TABLE_FUNCTION_ENTRY);
	for (const auto &table_function : table_functions) {
		AutoCompleteCandidate candidate(table_function.get().name, SuggestionState::SUGGEST_TABLE_FUNCTION_NAME, 0);
		suggestions.push_back(std::move(candidate));
	}

	return suggestions;
}

static vector<AutoCompleteCandidate> SuggestFileName(ClientContext &context, string &prefix, idx_t &last_pos) {
	vector<AutoCompleteCandidate> result;
	if (!Settings::Get<EnableExternalAccessSetting>(context)) {
		// if enable_external_access is disabled we don't search the file system
		return result;
	}
	auto &fs = FileSystem::GetFileSystem(context);
	string search_dir;
	auto is_path_absolute = fs.IsPathAbsolute(prefix);
	last_pos += prefix.size();
	for (idx_t i = prefix.size(); i > 0; i--, last_pos--) {
		if (prefix[i - 1] == '/' || prefix[i - 1] == '\\') {
			search_dir = prefix.substr(0, i - 1);
			prefix = prefix.substr(i);
			break;
		}
	}
	if (search_dir.empty()) {
		search_dir = is_path_absolute ? "/" : ".";
	} else {
		search_dir = fs.ExpandPath(search_dir);
	}
	fs.ListFiles(search_dir, [&](const string &fname, bool is_dir) {
		string suggestion;
		char extra_char;
		if (is_dir) {
			extra_char = fs.PathSeparator(fname)[0];
		} else {
			extra_char = '\'';
		}
		int score = 0;
		if (is_dir && fname[0] != '.') {
			score = 2;
		}
		if (KnownExtension(fname)) {
			score = 1;
		}
		auto state = is_dir ? SuggestionState::SUGGEST_DIRECTORY : SuggestionState::SUGGEST_FILE_NAME;
		result.emplace_back(fname, state, score);
		result.back().extra_char = extra_char;
		result.back().candidate_type = CandidateType::LITERAL;
	});
	return result;
}

// The following functions have been moved to autocomplete_core.cpp:
// - UnicodeSpace struct
// - ReplaceUnicodeSpaces
// - IsValidDollarQuotedStringTagFirstChar
// - IsValidDollarQuotedStringTagSubsequentChar
// - StripUnicodeSpaces
// - AutoCompleteTokenizer class
// - GenerateAutoCompleteSuggestions

// Forward declaration for StripUnicodeSpaces (defined in autocomplete_core.cpp)
bool StripUnicodeSpaces(const string &query_str, string &new_query);

// ClientContext-based provider — wraps existing Suggest* functions for in-process use.
class ClientContextCatalogProvider : public AutoCompleteCatalogProvider {
public:
	explicit ClientContextCatalogProvider(ClientContext &context) : context(context) {
	}
	vector<AutoCompleteCandidate> SuggestCatalogName() override {
		return ::duckdb::SuggestCatalogName(context);
	}
	vector<AutoCompleteCandidate> SuggestSchemaName() override {
		return ::duckdb::SuggestSchemaName(context);
	}
	vector<AutoCompleteCandidate> SuggestTableName() override {
		return ::duckdb::SuggestTableName(context);
	}
	vector<AutoCompleteCandidate> SuggestType() override {
		return ::duckdb::SuggestType(context);
	}
	vector<AutoCompleteCandidate> SuggestColumnName() override {
		return ::duckdb::SuggestColumnName(context);
	}
	vector<AutoCompleteCandidate> SuggestFileName(string &prefix, idx_t &last_pos) override {
		return ::duckdb::SuggestFileName(context, prefix, last_pos);
	}
	vector<AutoCompleteCandidate> SuggestScalarFunctionName() override {
		return ::duckdb::SuggestScalarFunctionName(context);
	}
	vector<AutoCompleteCandidate> SuggestTableFunctionName() override {
		return ::duckdb::SuggestTableFunctionName(context);
	}
	vector<AutoCompleteCandidate> SuggestPragmaName() override {
		return ::duckdb::SuggestPragmaName(context);
	}
	vector<AutoCompleteCandidate> SuggestSettingName() override {
		return ::duckdb::SuggestSettingName(context);
	}
	shared_ptr<PEGMatcher> GetPEGMatcher() override {
		return PEGMatcher::Get(context);
	}

private:
	ClientContext &context;
};

static duckdb::unique_ptr<SQLAutoCompleteFunctionData> GenerateSuggestions(ClientContext &context, const string &sql,
                                                                           AutoCompleteParameters &parameters) {
	ClientContextCatalogProvider provider(context);
	auto result = GenerateAutoCompleteSuggestions(provider, sql, parameters);
	return make_uniq<SQLAutoCompleteFunctionData>(std::move(result));
}

static duckdb::unique_ptr<FunctionData> SQLAutoCompleteBind(ClientContext &context, TableFunctionBindInput &input,
                                                            vector<LogicalType> &return_types, vector<string> &names) {
	if (input.inputs[0].IsNull()) {
		throw BinderException("sql_auto_complete first parameter cannot be NULL");
	}
	AutoCompleteParameters parameters;
	for (auto &param : input.named_parameters) {
		if (param.first == "max_suggestion_count") {
			parameters.max_suggestion_count = UBigIntValue::Get(param.second);
		} else if (param.first == "max_file_suggestion_count") {
			parameters.max_file_suggestion_count = UBigIntValue::Get(param.second);
		} else if (param.first == "max_exact_suggestion_count") {
			parameters.max_exact_suggestion_count = UBigIntValue::Get(param.second);
		} else {
			throw InternalException("Unsupported parameter for SQL auto complete");
		}
	}

	names.emplace_back("suggestion");
	return_types.emplace_back(LogicalType::VARCHAR);

	names.emplace_back("suggestion_start");
	return_types.emplace_back(LogicalType::INTEGER);

	names.emplace_back("suggestion_type");
	return_types.emplace_back(LogicalType::VARCHAR);

	names.emplace_back("suggestion_score");
	return_types.emplace_back(LogicalType::UBIGINT);

	names.emplace_back("extra_char");
	return_types.emplace_back(LogicalType::VARCHAR);

	return GenerateSuggestions(context, StringValue::Get(input.inputs[0]), parameters);
}

unique_ptr<GlobalTableFunctionState> SQLAutoCompleteInit(ClientContext &context, TableFunctionInitInput &input) {
	return make_uniq<SQLAutoCompleteData>();
}

void SQLAutoCompleteFunction(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &bind_data = data_p.bind_data->Cast<SQLAutoCompleteFunctionData>();
	auto &data = data_p.global_state->Cast<SQLAutoCompleteData>();
	if (data.offset >= bind_data.suggestions.size()) {
		// finished returning values
		return;
	}
	// start returning values
	// either fill up the chunk or return all the remaining columns
	idx_t count = 0;

	// suggestion, VARCHAR
	auto &suggestion = output.data[0];
	// suggestion_start, INTEGER
	auto &suggestion_start = output.data[1];
	// suggestion_type, VARCHAR
	auto &suggestion_type = output.data[2];
	// suggestion_score, VARCHAR
	auto &suggestion_score = output.data[3];
	// extra_char, VARCHAR
	auto &extra_char = output.data[4];

	while (data.offset < bind_data.suggestions.size() && count < STANDARD_VECTOR_SIZE) {
		auto &entry = bind_data.suggestions[data.offset++];

		suggestion.Append(Value(entry.text));
		suggestion_start.Append(Value::INTEGER(NumericCast<int32_t>(entry.pos)));
		suggestion_type.Append(Value(entry.type));
		suggestion_score.Append(Value::UBIGINT(entry.score));
		extra_char.Append(entry.extra_char == '\0' ? Value() : Value(string(1, entry.extra_char)));
		count++;
	}
	output.SetCardinality(count);
}

static unique_ptr<SQLTokenizeFunctionData> GenerateTokens(ClientContext &context, const string &sql) {
	HighlightTokenizer tokenizer(sql);
	tokenizer.TokenizeInput();

	// use the parser to annotate any tokens
	vector<MatcherSuggestion> suggestions;
	ParseResultAllocator parse_allocator;
	idx_t max_token_index = 0;
	MatchState state(tokenizer.tokens, suggestions, parse_allocator, max_token_index);

	auto peg_matcher = PEGMatcher::Get(context);
	peg_matcher->Root().Match(state);

	return make_uniq<SQLTokenizeFunctionData>(tokenizer.tokens);
}

unique_ptr<GlobalTableFunctionState> SQLTokenizeInit(ClientContext &context, TableFunctionInitInput &input) {
	return make_uniq<SQLTokenizeData>();
}

static unique_ptr<FunctionData> SQLTokenizeBind(ClientContext &context, TableFunctionBindInput &input,
                                                vector<LogicalType> &return_types, vector<string> &names) {
	if (input.inputs[0].IsNull()) {
		throw BinderException("sql_auto_complete first parameter cannot be NULL");
	}

	names.emplace_back("start");
	return_types.emplace_back(LogicalType::INTEGER);

	names.emplace_back("token_type");
	return_types.emplace_back(LogicalType::VARCHAR);

	names.emplace_back("word");
	return_types.emplace_back(LogicalType::VARCHAR);

	return GenerateTokens(context, StringValue::Get(input.inputs[0]));
}

void SQLTokenizeFunction(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &bind_data = data_p.bind_data->Cast<SQLTokenizeFunctionData>();
	auto &data = data_p.global_state->Cast<SQLTokenizeData>();
	if (data.offset >= bind_data.tokens.size()) {
		// finished returning values
		return;
	}

	// start returning values
	// either fill up the chunk or return all the remaining columns
	idx_t count = 0;

	// offset, INTEGER
	auto &offset_col = output.data[0];
	// token_type, VARCHAR
	auto &token_type = output.data[1];
	// word, VARCHAR
	auto &word = output.data[2];

	while (data.offset < bind_data.tokens.size() && count < STANDARD_VECTOR_SIZE) {
		auto &entry = bind_data.tokens[data.offset++];

		offset_col.Append(Value::INTEGER(NumericCast<int32_t>(entry.offset)));
		token_type.Append(Value(TokenTypeToString(entry.type)));
		word.Append(Value(entry.text));
		count++;
	}
	output.SetCardinality(count);
}

static duckdb::unique_ptr<FunctionData> CheckPEGParserBind(ClientContext &context, TableFunctionBindInput &input,
                                                           vector<LogicalType> &return_types, vector<string> &names) {
	if (input.inputs[0].IsNull()) {
		throw BinderException("sql_auto_complete first parameter cannot be NULL");
	}
	names.emplace_back("success");
	return_types.emplace_back(LogicalType::BOOLEAN);

	const auto sql = StringValue::Get(input.inputs[0]);

	vector<MatcherToken> root_tokens;
	string clean_sql;
	const string &sql_ref = Parser::StripUnicodeSpaces(sql, clean_sql) ? clean_sql : sql;
	ParserTokenizer tokenizer(sql_ref, root_tokens);

	auto allow_complete = tokenizer.TokenizeInput();
	if (!allow_complete) {
		return nullptr;
	}

	if (root_tokens.empty()) {
		return nullptr;
	}

	vector<MatcherSuggestion> suggestions;
	ParseResultAllocator parse_allocator;
	idx_t max_token_index = 0;
	MatchState state(root_tokens, suggestions, parse_allocator, max_token_index);

	auto peg_matcher = PEGMatcher::Get(context);
	auto match_result = peg_matcher->Root().Match(state);
	if (match_result != MatchResultType::SUCCESS || state.token_index < root_tokens.size()) {
		string token_list;
		for (idx_t i = 0; i < root_tokens.size(); i++) {
			if (!token_list.empty()) {
				token_list += "\n";
			}
			if (i < 10) {
				token_list += " ";
			}
			token_list += to_string(i) + ":" + root_tokens[i].text;
		}
		throw BinderException(
		    "Failed to parse query \"%s\" - did not consume all tokens (got to token %d - %s)\nTokens:\n%s", sql,
		    state.token_index, root_tokens[state.token_index].text, token_list);
	}
	return nullptr;
}

void CheckPEGParserFunction(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
}

struct FormatSQLBindData : public FunctionData {
	FormatterConfig config;

	unique_ptr<FunctionData> Copy() const override {
		auto result = make_uniq<FormatSQLBindData>();
		result->config = config;
		return std::move(result);
	}
	bool Equals(const FunctionData &other_p) const override {
		auto &other = other_p.Cast<FormatSQLBindData>();
		return config.indent_size == other.config.indent_size &&
		       config.inline_threshold == other.config.inline_threshold &&
		       config.keyword_case == other.config.keyword_case;
	}
};

//! Parse the MAP(VARCHAR, VARCHAR) config argument and populate FormatterConfig.
//! Recognised keys: "indent_size", "inline_threshold", "keyword_case".
static FormatterConfig ParseFormatterConfig(ClientContext &context, vector<unique_ptr<Expression>> &arguments) {
	FormatterConfig config;
	if (arguments.size() < 2) {
		return config;
	}
	auto &map_expr = *arguments[1];
	if (!map_expr.IsFoldable()) {
		throw InvalidInputException("duckdb_format_sql: config map must be a constant expression");
	}
	Value map_val = ExpressionExecutor::EvaluateScalar(context, map_expr);
	if (map_val.IsNull()) {
		return config;
	}
	for (const auto &pair : MapValue::GetChildren(map_val)) {
		const auto &kv = StructValue::GetChildren(pair);
		const auto key = StringUtil::Lower(kv[0].ToString());
		const auto val_str = kv[1].ToString();
		if (key == "indent_size") {
			config.indent_size = std::stoull(val_str);
		} else if (key == "inline_threshold") {
			config.inline_threshold = std::stoull(val_str);
		} else if (key == "keyword_case") {
			const string kc = StringUtil::Lower(val_str);
			if (kc == "upper") {
				config.keyword_case = KeywordCase::UPPER;
			} else if (kc == "lower") {
				config.keyword_case = KeywordCase::LOWER;
			} else if (kc == "preserve") {
				config.keyword_case = KeywordCase::PRESERVE;
			} else {
				throw InvalidInputException(
				    "duckdb_format_sql: keyword_case must be 'upper', 'lower', or 'preserve'; got '%s'", val_str);
			}
		} else {
			throw InvalidInputException("duckdb_format_sql: unknown config key '%s'", key);
		}
	}
	return config;
}

static unique_ptr<FunctionData> FormatSQLBind(BindScalarFunctionInput &input) {
	auto &context = input.GetClientContext();
	auto &arguments = input.GetArguments();

	auto bind_data = make_uniq<FormatSQLBindData>();
	bind_data->config = ParseFormatterConfig(context, arguments);
	return std::move(bind_data);
}

static void FormatSQLExecute(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &info = state.expr.Cast<BoundFunctionExpression>().bind_info->Cast<FormatSQLBindData>();
	auto &heap = StringVector::GetStringHeap(result);
	UnaryExecutor::Execute<string_t, string_t>(args.data[0], result, args.size(), [&](string_t input) {
		return heap.AddString(FormatSQL(input.GetString(), info.config));
	});
}

static void LoadInternal(ExtensionLoader &loader) {
	TableFunction auto_complete_fun("sql_auto_complete", {LogicalType::VARCHAR}, SQLAutoCompleteFunction,
	                                SQLAutoCompleteBind, SQLAutoCompleteInit);
	auto_complete_fun.named_parameters["max_suggestion_count"] = LogicalType::UBIGINT;
	auto_complete_fun.named_parameters["max_file_suggestion_count"] = LogicalType::UBIGINT;
	auto_complete_fun.named_parameters["max_exact_suggestion_count"] = LogicalType::UBIGINT;
	loader.RegisterFunction(auto_complete_fun);

	TableFunction check_peg_parser_fun("check_peg_parser", {LogicalType::VARCHAR}, CheckPEGParserFunction,
	                                   CheckPEGParserBind, nullptr);
	loader.RegisterFunction(check_peg_parser_fun);

	TableFunction tokenize_fun("sql_tokenize", {LogicalType::VARCHAR}, SQLTokenizeFunction, SQLTokenizeBind,
	                           SQLTokenizeInit);

	loader.RegisterFunction(tokenize_fun);

	const auto map_config_type = LogicalType::MAP(LogicalType::VARCHAR, LogicalType::VARCHAR);
	ScalarFunctionSet format_sql_set("duckdb_format_sql");
	// duckdb_format_sql(sql)
	format_sql_set.AddFunction(
	    ScalarFunction({LogicalType::VARCHAR}, LogicalType::VARCHAR, FormatSQLExecute, FormatSQLBind));
	// duckdb_format_sql(sql, config => MAP {'indent_size':'4', 'inline_threshold':'60'})
	format_sql_set.AddFunction(
	    ScalarFunction({LogicalType::VARCHAR, map_config_type}, LogicalType::VARCHAR, FormatSQLExecute, FormatSQLBind));
	loader.RegisterFunction(format_sql_set);
}

void AutocompleteExtension::Load(ExtensionLoader &loader) {
	LoadInternal(loader);
}

std::string AutocompleteExtension::Name() {
	return "autocomplete";
}

std::string AutocompleteExtension::Version() const {
	return DefaultVersion();
}

} // namespace duckdb
extern "C" {

DUCKDB_CPP_EXTENSION_ENTRY(autocomplete, loader) {
	LoadInternal(loader);
}
}
