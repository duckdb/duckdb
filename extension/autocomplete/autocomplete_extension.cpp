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
#include "duckdb/main/extension/extension_loader.hpp"
#include "transformer/peg_transformer.hpp"
#include "duckdb/parser/keyword_helper.hpp"
#include "matcher.hpp"
#include "duckdb/catalog/default/builtin_types/types.hpp"
#include "duckdb/main/attached_database.hpp"
#include "tokenizer.hpp"
#include "duckdb/catalog/catalog_entry/pragma_function_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/scalar_function_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/table_function_catalog_entry.hpp"

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

struct AutoCompleteParameters {
	idx_t max_suggestion_count = 20;
	idx_t max_file_suggestion_count = 100;
	bool suggestion_contains_files = false;
};

static string GetSuggestionType(SuggestionState type) {
	switch (type) {
	case SuggestionState::SUGGEST_KEYWORD:
		return "keyword";
	case SuggestionState::SUGGEST_CATALOG_NAME:
		return "catalog";
	case SuggestionState::SUGGEST_SCHEMA_NAME:
		return "schema";
	case SuggestionState::SUGGEST_TABLE_NAME:
		return "table";
	case SuggestionState::SUGGEST_TYPE_NAME:
		return "type";
	case SuggestionState::SUGGEST_COLUMN_NAME:
		return "column";
	case SuggestionState::SUGGEST_FILE_NAME:
		return "file_name";
	case SuggestionState::SUGGEST_DIRECTORY:
		return "directory";
	case SuggestionState::SUGGEST_SCALAR_FUNCTION_NAME:
		return "scalar_function";
	case SuggestionState::SUGGEST_TABLE_FUNCTION_NAME:
		return "table_function";
	case SuggestionState::SUGGEST_PRAGMA_NAME:
		return "pragma_function";
	case SuggestionState::SUGGEST_SETTING_NAME:
		return "setting";
	case SuggestionState::SUGGEST_RESERVED_VARIABLE:
	case SuggestionState::SUGGEST_VARIABLE:
	default:
		return "";
	}
}

bool PreferCaseMatching(SuggestionState suggestion_state) {
	switch (suggestion_state) {
	case SuggestionState::SUGGEST_SCALAR_FUNCTION_NAME:
	case SuggestionState::SUGGEST_TABLE_FUNCTION_NAME:
	case SuggestionState::SUGGEST_PRAGMA_NAME:
	case SuggestionState::SUGGEST_SETTING_NAME:
		return true;
	default:
		return false;
	}
}

static vector<AutoCompleteSuggestion> ComputeSuggestions(vector<AutoCompleteCandidate> available_suggestions,
                                                         const string &prefix, AutoCompleteParameters &parameters) {
	vector<pair<string, idx_t>> scores;
	scores.reserve(available_suggestions.size());

	case_insensitive_map_t<idx_t> matches;
	bool prefix_is_lower = StringUtil::IsLower(prefix);
	bool prefix_is_upper = StringUtil::IsUpper(prefix);
	auto lower_prefix = StringUtil::Lower(prefix);
	for (idx_t i = 0; i < available_suggestions.size(); i++) {
		auto &suggestion = available_suggestions[i];
		const int32_t BASE_SCORE = 10;
		const int32_t SUBSTRING_PENALTY = 10;
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
		if (prefix.empty()) {
		} else if (prefix.size() < str.size()) {
			score += StringUtil::SimilarityScore(str.substr(0, prefix.size()), prefix);
		} else {
			score += StringUtil::SimilarityScore(str, prefix);
		}
		auto type = available_suggestions[i].suggestion_type;
		if (str[0] == '.') {
			if (type == SuggestionState::SUGGEST_DIRECTORY || type == SuggestionState::SUGGEST_FILE_NAME) {
				score++;
			}
		} else if (type == SuggestionState::SUGGEST_DIRECTORY && score > 0) {
			score--;
		}
		if (!StringUtil::Contains(StringUtil::Lower(str), lower_prefix)) {
			score += SUBSTRING_PENALTY;
		} else if (PreferCaseMatching(type) && !StringUtil::Contains(str, prefix)) {
			// for types for which we prefer case matching - add a small penalty if we are not matching case
			score++;
		}
		suggestion.score = score;
		scores.emplace_back(str, score);
	}
	idx_t suggestion_count = parameters.max_suggestion_count;
	if (parameters.suggestion_contains_files) {
		suggestion_count = parameters.max_file_suggestion_count;
	}

	vector<AutoCompleteSuggestion> results;
	auto top_strings = StringUtil::TopNStrings(scores, suggestion_count, 999);
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
		string type = GetSuggestionType(suggestion.suggestion_type);
		results.emplace_back(std::move(result), suggestion.suggestion_pos, std::move(type), suggestion.score.GetIndex(),
		                     suggestion.extra_char);
	}
	return results;
}

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

static vector<AutoCompleteCandidate> SuggestType(ClientContext &) {
	vector<AutoCompleteCandidate> suggestions;
	for (auto &type_entry : BUILTIN_TYPES) {
		suggestions.emplace_back(type_entry.name, SuggestionState::SUGGEST_TYPE_NAME, 0, CandidateType::KEYWORD);
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
			for (auto &col : view.aliases) {
				suggestions.emplace_back(col, SuggestionState::SUGGEST_COLUMN_NAME, bonus);
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
	for (auto &entry : db_config.extension_parameters) {
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

class AutoCompleteTokenizer : public BaseTokenizer {
public:
	AutoCompleteTokenizer(const string &sql, MatchState &state)
	    : BaseTokenizer(sql, state.tokens), suggestions(state.suggestions) {
		last_pos = 0;
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

struct UnicodeSpace {
	UnicodeSpace(idx_t pos, idx_t bytes) : pos(pos), bytes(bytes) {
	}

	idx_t pos;
	idx_t bytes;
};

bool ReplaceUnicodeSpaces(const string &query, string &new_query, const vector<UnicodeSpace> &unicode_spaces) {
	if (unicode_spaces.empty()) {
		// no unicode spaces found
		return false;
	}
	idx_t prev = 0;
	for (auto &usp : unicode_spaces) {
		new_query += query.substr(prev, usp.pos - prev);
		new_query += " ";
		prev = usp.pos + usp.bytes;
	}
	new_query += query.substr(prev, query.size() - prev);
	return true;
}

bool IsValidDollarQuotedStringTagFirstChar(const unsigned char &c) {
	// the first character can be between A-Z, a-z, or \200 - \377
	return (c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z') || c >= 0x80;
}

bool IsValidDollarQuotedStringTagSubsequentChar(const unsigned char &c) {
	// subsequent characters can also be between 0-9
	return IsValidDollarQuotedStringTagFirstChar(c) || (c >= '0' && c <= '9');
}

// This function strips unicode space characters from the query and replaces them with regular spaces
// It returns true if any unicode space characters were found and stripped
// See here for a list of unicode space characters - https://jkorpela.fi/chars/spaces.html
bool StripUnicodeSpaces(const string &query_str, string &new_query) {
	const idx_t NBSP_LEN = 2;
	const idx_t USP_LEN = 3;
	idx_t pos = 0;
	unsigned char quote;
	string_t dollar_quote_tag;
	vector<UnicodeSpace> unicode_spaces;
	auto query = const_uchar_ptr_cast(query_str.c_str());
	auto qsize = query_str.size();

regular:
	for (; pos + 2 < qsize; pos++) {
		if (query[pos] == 0xC2) {
			if (query[pos + 1] == 0xA0) {
				// U+00A0 - C2A0
				unicode_spaces.emplace_back(pos, NBSP_LEN);
			}
		}
		if (query[pos] == 0xE2) {
			if (query[pos + 1] == 0x80) {
				if (query[pos + 2] >= 0x80 && query[pos + 2] <= 0x8B) {
					// U+2000 to U+200B
					// E28080 - E2808B
					unicode_spaces.emplace_back(pos, USP_LEN);
				} else if (query[pos + 2] == 0xAF) {
					// U+202F - E280AF
					unicode_spaces.emplace_back(pos, USP_LEN);
				}
			} else if (query[pos + 1] == 0x81) {
				if (query[pos + 2] == 0x9F) {
					// U+205F - E2819f
					unicode_spaces.emplace_back(pos, USP_LEN);
				} else if (query[pos + 2] == 0xA0) {
					// U+2060 - E281A0
					unicode_spaces.emplace_back(pos, USP_LEN);
				}
			}
		} else if (query[pos] == 0xE3) {
			if (query[pos + 1] == 0x80 && query[pos + 2] == 0x80) {
				// U+3000 - E38080
				unicode_spaces.emplace_back(pos, USP_LEN);
			}
		} else if (query[pos] == 0xEF) {
			if (query[pos + 1] == 0xBB && query[pos + 2] == 0xBF) {
				// U+FEFF - EFBBBF
				unicode_spaces.emplace_back(pos, USP_LEN);
			}
		} else if (query[pos] == '"' || query[pos] == '\'') {
			quote = query[pos];
			pos++;
			goto in_quotes;
		} else if (query[pos] == '$' &&
		           (query[pos + 1] == '$' || IsValidDollarQuotedStringTagFirstChar(query[pos + 1]))) {
			// (optionally tagged) dollar-quoted string
			auto start = &query[++pos];
			for (; pos + 2 < qsize; pos++) {
				if (query[pos] == '$') {
					// end of tag
					dollar_quote_tag =
					    string_t(const_char_ptr_cast(start), NumericCast<uint32_t, int64_t>(&query[pos] - start));
					goto in_dollar_quotes;
				}

				if (!IsValidDollarQuotedStringTagSubsequentChar(query[pos])) {
					// invalid char in dollar-quoted string, continue as normal
					goto regular;
				}
			}
			goto end;
		} else if (query[pos] == '-' && query[pos + 1] == '-') {
			goto in_comment;
		}
	}
	goto end;
in_quotes:
	for (; pos + 1 < qsize; pos++) {
		if (query[pos] == quote) {
			if (query[pos + 1] == quote) {
				// escaped quote
				pos++;
				continue;
			}
			pos++;
			goto regular;
		}
	}
	goto end;
in_dollar_quotes:
	for (; pos + 2 < qsize; pos++) {
		if (query[pos] == '$' &&
		    qsize - (pos + 1) >= dollar_quote_tag.GetSize() + 1 && // found '$' and enough space left
		    query[pos + dollar_quote_tag.GetSize() + 1] == '$' &&  // ending '$' at the right spot
		    memcmp(&query[pos + 1], dollar_quote_tag.GetData(), dollar_quote_tag.GetSize()) == 0) { // tags match
			pos += dollar_quote_tag.GetSize() + 1;
			goto regular;
		}
	}
	goto end;
in_comment:
	for (; pos < qsize; pos++) {
		if (query[pos] == '\n' || query[pos] == '\r') {
			goto regular;
		}
	}
	goto end;
end:
	return ReplaceUnicodeSpaces(query_str, new_query, unicode_spaces);
}

static duckdb::unique_ptr<SQLAutoCompleteFunctionData> GenerateSuggestions(ClientContext &context, const string &sql,
                                                                           AutoCompleteParameters &parameters) {
	// tokenize the input
	vector<MatcherToken> tokens;
	vector<MatcherSuggestion> suggestions;
	ParseResultAllocator parse_allocator;
	MatchState state(tokens, suggestions, parse_allocator);
	vector<UnicodeSpace> unicode_spaces;
	string clean_sql;
	const string &sql_ref = StripUnicodeSpaces(sql, clean_sql) ? clean_sql : sql;
	AutoCompleteTokenizer tokenizer(sql_ref, state);
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
			if (parameters.max_file_suggestion_count > 0) {
				new_suggestions = SuggestFileName(context, tokenizer.last_word, suggestion_pos);
				parameters.suggestion_contains_files = true;
			}
			break;
		case SuggestionState::SUGGEST_SCALAR_FUNCTION_NAME:
			new_suggestions = SuggestScalarFunctionName(context);
			break;
		case SuggestionState::SUGGEST_TABLE_FUNCTION_NAME:
			new_suggestions = SuggestTableFunctionName(context);
			break;
		case SuggestionState::SUGGEST_PRAGMA_NAME:
			new_suggestions = SuggestPragmaName(context);
			break;
		case SuggestionState::SUGGEST_SETTING_NAME:
			new_suggestions = SuggestSettingName(context);
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
	auto result_suggestions = ComputeSuggestions(available_suggestions, tokenizer.last_word, parameters);
	return make_uniq<SQLAutoCompleteFunctionData>(std::move(result_suggestions));
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
	while (data.offset < bind_data.suggestions.size() && count < STANDARD_VECTOR_SIZE) {
		auto &entry = bind_data.suggestions[data.offset++];

		// suggestion, VARCHAR
		output.SetValue(0, count, Value(entry.text));

		// suggestion_start, INTEGER
		output.SetValue(1, count, Value::INTEGER(NumericCast<int32_t>(entry.pos)));

		// suggestion_type, VARCHAR
		output.SetValue(2, count, Value(entry.type));

		// suggestion-score, VARCHAR
		output.SetValue(3, count, Value::UBIGINT(entry.score));

		// extra_char, VARCHAR
		output.SetValue(4, count, entry.extra_char == '\0' ? Value() : Value(string(1, entry.extra_char)));
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
	void OnLastToken(TokenizeState state, string last_word, idx_t last_pos) override {
		if (last_word.empty()) {
			return;
		}
		tokens.emplace_back(std::move(last_word), last_pos);
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

	const auto sql = StringValue::Get(input.inputs[0]);

	vector<MatcherToken> root_tokens;
	string clean_sql;
	const string &sql_ref = StripUnicodeSpaces(sql, clean_sql) ? clean_sql : sql;
	ParserTokenizer tokenizer(sql_ref, root_tokens);

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
		ParseResultAllocator parse_allocator;
		MatchState state(tokens, suggestions, parse_allocator);

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

class PEGParserExtension : public ParserExtension {
public:
	PEGParserExtension() {
		parser_override = PEGParser;
	}

	static ParserOverrideResult PEGParser(ParserExtensionInfo *info, const string &query) {
		vector<MatcherToken> root_tokens;
		string clean_sql;

		ParserTokenizer tokenizer(query, root_tokens);
		tokenizer.TokenizeInput();
		tokenizer.statements.push_back(std::move(root_tokens));

		vector<unique_ptr<SQLStatement>> result;
		try {
			for (auto &tokenized_statement : tokenizer.statements) {
				if (tokenized_statement.empty()) {
					continue;
				}
				auto &transformer = PEGTransformerFactory::GetInstance();
				auto statement = transformer.Transform(tokenized_statement, "Statement");
				if (statement) {
					statement->stmt_location = NumericCast<idx_t>(tokenized_statement[0].offset);
					statement->stmt_length =
					    NumericCast<idx_t>(tokenized_statement[tokenized_statement.size() - 1].offset +
					                       tokenized_statement[tokenized_statement.size() - 1].length);
				}
				statement->query = query;
				result.push_back(std::move(statement));
			}
			return ParserOverrideResult(std::move(result));
		} catch (std::exception &e) {
			return ParserOverrideResult(e);
		}
	}
};

static void LoadInternal(ExtensionLoader &loader) {
	TableFunction auto_complete_fun("sql_auto_complete", {LogicalType::VARCHAR}, SQLAutoCompleteFunction,
	                                SQLAutoCompleteBind, SQLAutoCompleteInit);
	auto_complete_fun.named_parameters["max_suggestion_count"] = LogicalType::UBIGINT;
	auto_complete_fun.named_parameters["max_file_suggestion_count"] = LogicalType::UBIGINT;
	loader.RegisterFunction(auto_complete_fun);

	TableFunction check_peg_parser_fun("check_peg_parser", {LogicalType::VARCHAR}, CheckPEGParserFunction,
	                                   CheckPEGParserBind, nullptr);
	loader.RegisterFunction(check_peg_parser_fun);

	auto &config = DBConfig::GetConfig(loader.GetDatabaseInstance());
	config.parser_extensions.push_back(PEGParserExtension());
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
