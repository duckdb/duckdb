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

namespace duckdb {

struct SQLAutoCompleteFunctionData : public TableFunctionData {
	explicit SQLAutoCompleteFunctionData(vector<string> suggestions_p, idx_t start_pos)
	    : suggestions(std::move(suggestions_p)), start_pos(start_pos) {
	}

	vector<string> suggestions;
	idx_t start_pos;
};

struct SQLAutoCompleteData : public GlobalTableFunctionState {
	SQLAutoCompleteData() : offset(0) {
	}

	idx_t offset;
};

struct AutoCompleteCandidate {
	explicit AutoCompleteCandidate(string candidate_p, int32_t score_bonus = 0)
	    : candidate(std::move(candidate_p)), score_bonus(score_bonus) {
	}

	string candidate;
	//! The higher the score bonus, the more likely this candidate will be chosen
	int32_t score_bonus;
};

static vector<string> ComputeSuggestions(vector<AutoCompleteCandidate> available_suggestions, const string &prefix,
                                         const unordered_set<string> &extra_keywords, bool add_quotes = false) {
	for (auto &kw : extra_keywords) {
		available_suggestions.emplace_back(std::move(kw));
	}
	vector<pair<string, idx_t>> scores;
	scores.reserve(available_suggestions.size());
	for (auto &suggestion : available_suggestions) {
		const int32_t BASE_SCORE = 10;
		auto &str = suggestion.candidate;
		auto bonus = suggestion.score_bonus;

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
	auto results = StringUtil::TopNStrings(scores, 20, 999);
	if (add_quotes) {
		for (auto &result : results) {
			if (extra_keywords.find(result) == extra_keywords.end()) {
				result = KeywordHelper::WriteOptionallyQuoted(result, '"', true);
			} else {
				result = result + " ";
			}
		}
	}
	return results;
}

static vector<string> InitialKeywords() {
	return vector<string> {"SELECT",     "INSERT",   "DELETE",  "UPDATE",  "CREATE",   "DROP",      "COPY",
	                       "ALTER",      "WITH",     "EXPORT",  "BEGIN",   "VACUUM",   "PREPARE",   "EXECUTE",
	                       "DEALLOCATE", "CALL",     "ANALYZE", "EXPLAIN", "DESCRIBE", "SUMMARIZE", "LOAD",
	                       "CHECKPOINT", "ROLLBACK", "COMMIT",  "CALL",    "FROM",     "PIVOT",     "UNPIVOT"};
}

static vector<AutoCompleteCandidate> SuggestKeyword(ClientContext &context) {
	auto keywords = InitialKeywords();
	vector<AutoCompleteCandidate> result;
	for (auto &kw : keywords) {
		auto score = 0;
		if (kw == "SELECT") {
			score = 2;
		}
		if (kw == "FROM" || kw == "DELETE" || kw == "INSERT" || kw == "UPDATE") {
			score = 1;
		}
		result.emplace_back(kw + " ", score);
	}
	return result;
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

static vector<AutoCompleteCandidate> SuggestColumnName(ClientContext &context) {
	vector<AutoCompleteCandidate> suggestions;
	auto all_entries = GetAllTables(context, false);
	for (auto &entry_ref : all_entries) {
		auto &entry = entry_ref.get();
		if (entry.type == CatalogType::TABLE_ENTRY) {
			auto &table = entry.Cast<TableCatalogEntry>();
			for (auto &col : table.GetColumns().Logical()) {
				suggestions.emplace_back(col.GetName(), 1);
			}
		} else if (entry.type == CatalogType::VIEW_ENTRY) {
			auto &view = entry.Cast<ViewCatalogEntry>();
			for (auto &col : view.aliases) {
				suggestions.emplace_back(col, 1);
			}
		} else {
			if (StringUtil::CharacterIsOperator(entry.name[0])) {
				continue;
			}
			suggestions.emplace_back(entry.name);
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
	auto &fs = FileSystem::GetFileSystem(context);
	string search_dir;
	D_ASSERT(last_pos >= prefix.size());
	auto is_path_absolute = fs.IsPathAbsolute(prefix);
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
	vector<AutoCompleteCandidate> result;
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
	});
	return result;
}

enum class SuggestionState : uint8_t { SUGGEST_KEYWORD, SUGGEST_TABLE_NAME, SUGGEST_COLUMN_NAME, SUGGEST_FILE_NAME };

static duckdb::unique_ptr<SQLAutoCompleteFunctionData> GenerateSuggestions(ClientContext &context, const string &sql) {
	// for auto-completion, we consider 4 scenarios
	// * there is nothing in the buffer, or only one word -> suggest a keyword
	// * the previous keyword is SELECT, WHERE, BY, HAVING, ... -> suggest a column name
	// * the previous keyword is FROM, INSERT, UPDATE ,... -> select a table name
	// * we are in a string constant -> suggest a filename
	// figure out which state we are in by doing a run through the query
	idx_t pos = 0;
	idx_t last_pos = 0;
	idx_t pos_offset = 0;
	bool seen_word = false;
	unordered_set<string> suggested_keywords;
	SuggestionState suggest_state = SuggestionState::SUGGEST_KEYWORD;
	case_insensitive_set_t column_name_keywords = {"SELECT", "WHERE", "BY",    "HAVING", "QUALIFY",
	                                               "LIMIT",  "SET",   "USING", "ON"};
	case_insensitive_set_t table_name_keywords = {"FROM",  "JOIN", "INSERT", "UPDATE",  "DELETE",
	                                              "ALTER", "DROP", "CALL",   "DESCRIBE"};
	case_insensitive_map_t<unordered_set<string>> next_keyword_map;
	next_keyword_map["SELECT"] = {"FROM",    "WHERE",  "GROUP",  "HAVING", "WINDOW", "ORDER",     "LIMIT",
	                              "QUALIFY", "SAMPLE", "VALUES", "UNION",  "EXCEPT", "INTERSECT", "DISTINCT"};
	next_keyword_map["WITH"] = {"RECURSIVE", "SELECT", "AS"};
	next_keyword_map["INSERT"] = {"INTO", "VALUES", "SELECT", "DEFAULT"};
	next_keyword_map["DELETE"] = {"FROM", "WHERE", "USING"};
	next_keyword_map["UPDATE"] = {"SET", "WHERE"};
	next_keyword_map["CREATE"] = {"TABLE", "SCHEMA", "VIEW", "SEQUENCE", "MACRO", "FUNCTION", "SECRET", "TYPE"};
	next_keyword_map["DROP"] = next_keyword_map["CREATE"];
	next_keyword_map["ALTER"] = {"TABLE", "VIEW", "ADD", "DROP", "COLUMN", "SET", "TYPE", "DEFAULT", "DATA", "RENAME"};

regular_scan:
	for (; pos < sql.size(); pos++) {
		if (sql[pos] == '\'') {
			pos++;
			last_pos = pos;
			goto in_string_constant;
		}
		if (sql[pos] == '"') {
			pos++;
			last_pos = pos;
			goto in_quotes;
		}
		if (sql[pos] == '-' && pos + 1 < sql.size() && sql[pos + 1] == '-') {
			goto in_comment;
		}
		if (sql[pos] == ';') {
			// semicolon: restart suggestion flow
			suggest_state = SuggestionState::SUGGEST_KEYWORD;
			suggested_keywords.clear();
			last_pos = pos + 1;
			continue;
		}
		if (StringUtil::CharacterIsSpace(sql[pos]) || StringUtil::CharacterIsOperator(sql[pos])) {
			if (seen_word) {
				goto process_word;
			}
		} else {
			seen_word = true;
		}
	}
	goto standard_suggestion;
in_comment:
	for (; pos < sql.size(); pos++) {
		if (sql[pos] == '\n' || sql[pos] == '\r') {
			pos++;
			goto regular_scan;
		}
	}
	// no suggestions inside comments
	return make_uniq<SQLAutoCompleteFunctionData>(vector<string>(), 0);
in_quotes:
	for (; pos < sql.size(); pos++) {
		if (sql[pos] == '"') {
			pos++;
			last_pos = pos;
			seen_word = true;
			goto regular_scan;
		}
	}
	pos_offset = 1;
	goto standard_suggestion;
in_string_constant:
	for (; pos < sql.size(); pos++) {
		if (sql[pos] == '\'') {
			pos++;
			last_pos = pos;
			seen_word = true;
			goto regular_scan;
		}
	}
	suggest_state = SuggestionState::SUGGEST_FILE_NAME;
	goto standard_suggestion;
process_word : {
	while ((last_pos < sql.size()) &&
	       (StringUtil::CharacterIsSpace(sql[last_pos]) || StringUtil::CharacterIsOperator(sql[last_pos]))) {
		last_pos++;
	}
	auto next_word = sql.substr(last_pos, pos - last_pos);
	if (table_name_keywords.find(next_word) != table_name_keywords.end()) {
		suggest_state = SuggestionState::SUGGEST_TABLE_NAME;
	} else if (column_name_keywords.find(next_word) != column_name_keywords.end()) {
		suggest_state = SuggestionState::SUGGEST_COLUMN_NAME;
	}
	auto entry = next_keyword_map.find(next_word);
	if (entry != next_keyword_map.end()) {
		suggested_keywords = entry->second;
	} else {
		suggested_keywords.erase(next_word);
	}
	if (std::all_of(next_word.begin(), next_word.end(), ::isdigit)) {
		// Numbers are OK
		suggested_keywords.clear();
	}
	seen_word = false;
	last_pos = pos;
	goto regular_scan;
}
standard_suggestion:
	if (suggest_state != SuggestionState::SUGGEST_FILE_NAME) {
		while ((last_pos < sql.size()) &&
		       (StringUtil::CharacterIsSpace(sql[last_pos]) || StringUtil::CharacterIsOperator(sql[last_pos]))) {
			last_pos++;
		}
	}
	auto last_word = sql.substr(last_pos, pos - last_pos);
	last_pos -= pos_offset;
	vector<string> suggestions;
	switch (suggest_state) {
	case SuggestionState::SUGGEST_KEYWORD:
		suggestions = ComputeSuggestions(SuggestKeyword(context), last_word, suggested_keywords);
		break;
	case SuggestionState::SUGGEST_TABLE_NAME:
		suggestions = ComputeSuggestions(SuggestTableName(context), last_word, suggested_keywords, true);
		break;
	case SuggestionState::SUGGEST_COLUMN_NAME:
		suggestions = ComputeSuggestions(SuggestColumnName(context), last_word, suggested_keywords, true);
		break;
	case SuggestionState::SUGGEST_FILE_NAME:
		last_pos = pos;
		suggestions =
		    ComputeSuggestions(SuggestFileName(context, last_word, last_pos), last_word, unordered_set<string>());
		break;
	default:
		throw InternalException("Unrecognized suggestion state");
	}
	if (last_pos > sql.size()) {
		D_ASSERT(false);
		throw NotImplementedException("last_pos out of range");
	}
	if (!last_word.empty() && std::all_of(last_word.begin(), last_word.end(), ::isdigit)) {
		// avoid giving auto-complete suggestion for digits
		suggestions.clear();
	}
	return make_uniq<SQLAutoCompleteFunctionData>(std::move(suggestions), last_pos);
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
		output.SetValue(0, count, Value(entry));

		// suggestion_start, INTEGER
		output.SetValue(1, count, Value::INTEGER(bind_data.start_pos));

		count++;
	}
	output.SetCardinality(count);
}

static void LoadInternal(DatabaseInstance &db) {
	TableFunction auto_complete_fun("sql_auto_complete", {LogicalType::VARCHAR}, SQLAutoCompleteFunction,
	                                SQLAutoCompleteBind, SQLAutoCompleteInit);
	ExtensionUtil::RegisterFunction(db, auto_complete_fun);
}
void AutocompleteExtension::Load(DuckDB &db) {
	LoadInternal(*db.instance);
}

std::string AutocompleteExtension::Name() {
	return "autocomplete";
}

std::string AutocompleteExtension::Version() const {
#ifdef EXT_VERSION_AUTOCOMPLETE
	return EXT_VERSION_AUTOCOMPLETE;
#else
	return "";
#endif
}

} // namespace duckdb
extern "C" {

DUCKDB_EXTENSION_API void autocomplete_init(duckdb::DatabaseInstance &db) {
	LoadInternal(db);
}

DUCKDB_EXTENSION_API const char *autocomplete_version() {
	return duckdb::DuckDB::LibraryVersion();
}
}

#ifndef DUCKDB_EXTENSION_MAIN
#error DUCKDB_EXTENSION_MAIN not defined
#endif
