#define DUCKDB_EXTENSION_MAIN

#include "autocomplete_extension.hpp"

#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/view_catalog_entry.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/file_opener.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/client_data.hpp"
#include "duckdb/main/extension_util.hpp"
#include "duckdb/parser/keyword_helper.hpp"
#include "duckdb/parser/parser.hpp"
#include "matcher.hpp"
#include "duckdb/catalog/default/builtin_types/types.hpp"
#include "duckdb/main/attached_database.hpp"
#include "tokenizer.hpp"

namespace duckdb {

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

static vector<AutoCompleteSuggestion> ComputeSuggestions(vector<AutoCompleteCandidate> available_suggestions,
                                                         const string &prefix) {
	vector<pair<string, idx_t>> scores;
	scores.reserve(available_suggestions.size());

	case_insensitive_map_t<idx_t> matches;
	bool prefix_is_lower = StringUtil::IsLower(prefix);
	bool prefix_is_upper = StringUtil::IsUpper(prefix);
	for (idx_t i = 0; i < available_suggestions.size(); i++) {
		auto &suggestion = available_suggestions[i];
		const int32_t BASE_SCORE = 10;
		auto str = suggestion.candidate;
		if (suggestion.extra_char != '\0') {
			str += suggestion.extra_char;
		}
		auto bonus = suggestion.score_bonus;
		if (matches.find(str) != matches.end()) {
			// entry already exists
			continue;
		}
		matches[str] = i;

		D_ASSERT(BASE_SCORE - bonus >= 0);
		auto score = idx_t(BASE_SCORE - bonus);
		if (prefix.size() == 0) {
		} else if (prefix.size() < str.size()) {
			score += StringUtil::SimilarityScore(str.substr(0, prefix.size()), prefix);
		} else {
			score += StringUtil::SimilarityScore(str, prefix);
		}
		scores.emplace_back(str, score);
	}
	vector<AutoCompleteSuggestion> results;
	auto top_strings = StringUtil::TopNStrings(scores, 20, 999);
	for (auto &result : top_strings) {
		auto entry = matches.find(result);
		if (entry == matches.end()) {
			throw InternalException("Auto-complete match not found");
		}
		auto &suggestion = available_suggestions[entry->second];
		if (suggestion.extra_char != '\0') {
			result.pop_back();
		}
		if (suggestion.candidate_type == CandidateType::KEYWORD) {
			if (prefix_is_lower) {
				result = StringUtil::Lower(result);
			} else if (prefix_is_upper) {
				result = StringUtil::Upper(result);
			}
		} else if (suggestion.candidate_type == CandidateType::IDENTIFIER) {
			result = KeywordHelper::WriteOptionallyQuoted(result, '"');
		}
		if (suggestion.extra_char != '\0') {
			result += suggestion.extra_char;
		}
		results.emplace_back(std::move(result), suggestion.suggestion_pos);
	}
	return results;
}

static vector<reference<AttachedDatabase>> GetAllCatalogs(ClientContext &context) {
	vector<reference<AttachedDatabase>> result;

	auto &database_manager = DatabaseManager::Get(context);
	auto databases = database_manager.GetDatabases(context);
	for (auto &database : databases) {
		result.push_back(database.get());
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

static vector<AutoCompleteCandidate> SuggestCatalogName(ClientContext &context) {
	vector<AutoCompleteCandidate> suggestions;
	auto all_entries = GetAllCatalogs(context);
	for (auto &entry_ref : all_entries) {
		auto &entry = entry_ref.get();
		AutoCompleteCandidate candidate(entry.name, 0);
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
		AutoCompleteCandidate candidate(entry.name, 0);
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
		suggestions.emplace_back(entry.name, bonus);
	}
	return suggestions;
}

static vector<AutoCompleteCandidate> SuggestType(ClientContext &) {
	vector<AutoCompleteCandidate> suggestions;
	for (auto &type_entry : BUILTIN_TYPES) {
		suggestions.emplace_back(type_entry.name, 0, CandidateType::KEYWORD);
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
				suggestions.emplace_back(col.GetName(), bonus);
			}
		} else if (entry.type == CatalogType::VIEW_ENTRY) {
			auto &view = entry.Cast<ViewCatalogEntry>();
			int32_t bonus = entry.internal ? 0 : 3;
			for (auto &col : view.aliases) {
				suggestions.emplace_back(col, bonus);
			}
		} else {
			if (StringUtil::CharacterIsOperator(entry.name[0])) {
				continue;
			}
			int32_t bonus = entry.internal ? 0 : 2;
			suggestions.emplace_back(entry.name, bonus);
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

static vector<AutoCompleteCandidate> SuggestFileName(ClientContext &context, string &prefix, idx_t &last_pos) {
	vector<AutoCompleteCandidate> result;
	auto &config = DBConfig::GetConfig(context);
	if (!config.options.enable_external_access) {
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
		if (is_dir) {
			suggestion = fname + fs.PathSeparator(fname);
		} else {
			suggestion = fname + "'";
		}
		int score = 0;
		if (is_dir && fname[0] != '.') {
			score = 2;
		}
		if (KnownExtension(fname)) {
			score = 1;
		}
		result.emplace_back(std::move(suggestion), score);
		result.back().candidate_type = CandidateType::LITERAL;
	});
	return result;
}

class AutoCompleteTokenizer : public BaseTokenizer {
public:
	AutoCompleteTokenizer(const string &sql, MatchState &state)
	    : BaseTokenizer(sql, state.tokens), suggestions(state.suggestions) {
	}

	void OnLastToken(TokenizeState state, string last_word_p, idx_t last_pos_p) override {
		if (state == TokenizeState::STRING_LITERAL) {
			suggestions.emplace_back(SuggestionState::SUGGEST_FILE_NAME);
		}
		last_word = std::move(last_word_p);
		last_pos = last_pos_p;
	}

	vector<MatcherSuggestion> &suggestions;
	string last_word;
	idx_t last_pos;
};

static duckdb::unique_ptr<SQLAutoCompleteFunctionData> GenerateSuggestions(ClientContext &context, const string &sql) {
	// tokenize the input
	vector<MatcherToken> tokens;
	vector<MatcherSuggestion> suggestions;
	MatchState state(tokens, suggestions);

	AutoCompleteTokenizer tokenizer(sql, state);
	auto allow_complete = tokenizer.TokenizeInput();
	if (!allow_complete) {
		return make_uniq<SQLAutoCompleteFunctionData>(vector<AutoCompleteSuggestion>());
	}
	if (state.suggestions.empty()) {
		// no suggestions found during tokenizing
		// run the root matcher
		MatcherAllocator allocator;
		auto &matcher = Matcher::RootMatcher(allocator);
		matcher.Match(state);
	}
	if (state.suggestions.empty()) {
		// still no suggestions - return
		return make_uniq<SQLAutoCompleteFunctionData>(vector<AutoCompleteSuggestion>());
	}
	vector<AutoCompleteCandidate> available_suggestions;
	for (auto &suggestion : suggestions) {
		idx_t suggestion_pos = tokenizer.last_pos;
		// run the suggestions
		vector<AutoCompleteCandidate> new_suggestions;
		switch (suggestion.type) {
		case SuggestionState::SUGGEST_VARIABLE:
			// variables have no suggestions available
			break;
		case SuggestionState::SUGGEST_KEYWORD:
			new_suggestions.emplace_back(suggestion.keyword);
			break;
		case SuggestionState::SUGGEST_CATALOG_NAME:
			new_suggestions = SuggestCatalogName(context);
			break;
		case SuggestionState::SUGGEST_SCHEMA_NAME:
			new_suggestions = SuggestSchemaName(context);
			break;
		case SuggestionState::SUGGEST_TABLE_NAME:
			new_suggestions = SuggestTableName(context);
			break;
		case SuggestionState::SUGGEST_COLUMN_NAME:
			new_suggestions = SuggestColumnName(context);
			break;
		case SuggestionState::SUGGEST_TYPE_NAME:
			new_suggestions = SuggestType(context);
			break;
		case SuggestionState::SUGGEST_FILE_NAME:
			new_suggestions = SuggestFileName(context, tokenizer.last_word, suggestion_pos);
			break;
		case SuggestionState::SUGGEST_SCALAR_FUNCTION_NAME:
		case SuggestionState::SUGGEST_TABLE_FUNCTION_NAME:
		case SuggestionState::SUGGEST_PRAGMA_NAME:
		case SuggestionState::SUGGEST_SETTING_NAME:
			// TODO:
			break;
		default:
			throw InternalException("Unrecognized suggestion state");
		}
		for (auto &new_suggestion : new_suggestions) {
			if (new_suggestion.extra_char == '\0') {
				new_suggestion.extra_char = suggestion.extra_char;
			}
			new_suggestion.suggestion_pos = suggestion_pos;
			available_suggestions.push_back(std::move(new_suggestion));
		}
	}
	auto result_suggestions = ComputeSuggestions(available_suggestions, tokenizer.last_word);
	return make_uniq<SQLAutoCompleteFunctionData>(std::move(result_suggestions));
}

static duckdb::unique_ptr<FunctionData> SQLAutoCompleteBind(ClientContext &context, TableFunctionBindInput &input,
                                                            vector<LogicalType> &return_types, vector<string> &names) {
	if (input.inputs[0].IsNull()) {
		throw BinderException("sql_auto_complete first parameter cannot be NULL");
	}
	names.emplace_back("suggestion");
	return_types.emplace_back(LogicalType::VARCHAR);

	names.emplace_back("suggestion_start");
	return_types.emplace_back(LogicalType::INTEGER);

	return GenerateSuggestions(context, StringValue::Get(input.inputs[0]));
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
	while (data.offset < bind_data.suggestions.size() && count < STANDARD_VECTOR_SIZE) {
		auto &entry = bind_data.suggestions[data.offset++];

		// suggestion, VARCHAR
		output.SetValue(0, count, Value(entry.text));

		// suggestion_start, INTEGER
		output.SetValue(1, count, Value::INTEGER(NumericCast<int32_t>(entry.pos)));

		count++;
	}
	output.SetCardinality(count);
}

class ParserTokenizer : public BaseTokenizer {
public:
	ParserTokenizer(const string &sql, vector<MatcherToken> &tokens) : BaseTokenizer(sql, tokens) {
	}
	void OnStatementEnd(idx_t pos) override {
		statements.push_back(std::move(tokens));
		tokens.clear();
	}
	void OnLastToken(TokenizeState state, string last_word, idx_t) override {
		if (last_word.empty()) {
			return;
		}
		tokens.push_back(std::move(last_word));
	}

	vector<vector<MatcherToken>> statements;
};

static duckdb::unique_ptr<FunctionData> CheckPEGParserBind(ClientContext &context, TableFunctionBindInput &input,
                                                           vector<LogicalType> &return_types, vector<string> &names) {
	if (input.inputs[0].IsNull()) {
		throw BinderException("sql_auto_complete first parameter cannot be NULL");
	}
	names.emplace_back("success");
	return_types.emplace_back(LogicalType::BOOLEAN);

	auto sql = StringValue::Get(input.inputs[0]);

	vector<MatcherToken> root_tokens;
	ParserTokenizer tokenizer(sql, root_tokens);

	auto allow_complete = tokenizer.TokenizeInput();
	if (!allow_complete) {
		return nullptr;
	}
	tokenizer.statements.push_back(std::move(root_tokens));

	for (auto &tokens : tokenizer.statements) {
		if (tokens.empty()) {
			continue;
		}
		vector<MatcherSuggestion> suggestions;
		MatchState state(tokens, suggestions);

		MatcherAllocator allocator;
		auto &matcher = Matcher::RootMatcher(allocator);
		auto match_result = matcher.Match(state);
		if (match_result != MatchResultType::SUCCESS || state.token_index < tokens.size()) {
			string token_list;
			for (idx_t i = 0; i < tokens.size(); i++) {
				if (!token_list.empty()) {
					token_list += "\n";
				}
				if (i < 10) {
					token_list += " ";
				}
				token_list += to_string(i) + ":" + tokens[i].text;
			}
			throw BinderException(
			    "Failed to parse query \"%s\" - did not consume all tokens (got to token %d - %s)\nTokens:\n%s", sql,
			    state.token_index, tokens[state.token_index].text, token_list);
		}
	}
	return nullptr;
}

void CheckPEGParserFunction(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
}

static void LoadInternal(DatabaseInstance &db) {
	TableFunction auto_complete_fun("sql_auto_complete", {LogicalType::VARCHAR}, SQLAutoCompleteFunction,
	                                SQLAutoCompleteBind, SQLAutoCompleteInit);
	ExtensionUtil::RegisterFunction(db, auto_complete_fun);

	TableFunction check_peg_parser_fun("check_peg_parser", {LogicalType::VARCHAR}, CheckPEGParserFunction,
	                                   CheckPEGParserBind, nullptr);
	ExtensionUtil::RegisterFunction(db, check_peg_parser_fun);
}

void AutocompleteExtension::Load(DuckDB &db) {
	LoadInternal(*db.instance);
}

std::string AutocompleteExtension::Name() {
	return "autocomplete";
}

std::string AutocompleteExtension::Version() const {
	return DefaultVersion();
}

} // namespace duckdb
extern "C" {

DUCKDB_EXTENSION_API void autocomplete_init(duckdb::DatabaseInstance &db) {
	LoadInternal(db);
}

DUCKDB_EXTENSION_API const char *autocomplete_version() {
	return duckdb::AutocompleteExtension::DefaultVersion();
}
}

#ifndef DUCKDB_EXTENSION_MAIN
#error DUCKDB_EXTENSION_MAIN not defined
#endif
