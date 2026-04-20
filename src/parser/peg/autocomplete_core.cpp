#include "autocomplete_catalog_provider.hpp"
#include "matcher.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "duckdb/parser/keyword_helper.hpp"
#include "parser/tokenizer/parser_tokenizer.hpp"
#include "include/parser/tokenizer/base_tokenizer.hpp"

namespace duckdb {

string GetSuggestionType(SuggestionState type) {
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
	case SuggestionState::SUGGEST_FILE_NAME:
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
		idx_t match_score = 0;
		if (prefix.empty()) {
		} else if (prefix.size() < str.size()) {
			match_score = StringUtil::SimilarityScore(str.substr(0, prefix.size()), prefix);
		} else {
			match_score = StringUtil::SimilarityScore(str, prefix);
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
			match_score++;
		}
		score += match_score;
		suggestion.score = match_score;
		scores.emplace_back(str, score);
	}
	idx_t fuzzy_suggestion_count = parameters.max_suggestion_count;
	if (parameters.suggestion_contains_files) {
		fuzzy_suggestion_count = parameters.max_file_suggestion_count;
	}
	idx_t suggestion_count = MaxValue<idx_t>(parameters.max_exact_suggestion_count, fuzzy_suggestion_count);

	vector<AutoCompleteSuggestion> results;
	auto top_strings = StringUtil::TopNStrings(scores, suggestion_count, 999);
	for (auto &result : top_strings) {
		auto entry = matches.find(result);
		if (entry == matches.end()) {
			throw InternalException("Auto-complete match not found");
		}
		if (results.size() > fuzzy_suggestion_count) {
			// after we exceed the "fuzzy_suggestion_count" we only accept exact suggestion matches
			if (!StringUtil::StartsWith(StringUtil::Lower(result), lower_prefix)) {
				break;
			}
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
	// the first character can be between A-Z, a-z, underscore, or \200 - \377
	return (c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z') || c == '_' || c >= 0x80;
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

class AutoCompleteTokenizer : public BaseTokenizer {
public:
	AutoCompleteTokenizer(const string &sql, MatchState &state)
	    : BaseTokenizer(sql, state.tokens), suggestions(state.suggestions) {
		last_pos = 0;
	}

	void OnLastToken(TokenizeState state, string last_word_p, idx_t last_pos_p) override {
		if (TokenizeStateToType(state) == TokenType::STRING_LITERAL) {
			suggestions.emplace_back(SuggestionState::SUGGEST_FILE_NAME);
		}
		if (StringUtil::StartsWith(last_word_p, "'")) {
			last_word_p = last_word_p.substr(1, last_word_p.size() - 1);
			last_pos_p += 1;
		}
		last_word = std::move(last_word_p);
		last_pos = last_pos_p;
	}

	void OnStatementEnd(idx_t pos) override {
		tokens.clear();
	}

	vector<MatcherSuggestion> &suggestions;
	string last_word;
	idx_t last_pos;
};

vector<AutoCompleteSuggestion> GenerateAutoCompleteSuggestions(AutoCompleteCatalogProvider &provider, const string &sql,
                                                               AutoCompleteParameters &parameters) {
	// tokenize the input
	vector<MatcherToken> tokens;
	vector<MatcherSuggestion> suggestions;
	ParseResultAllocator parse_allocator;
	idx_t max_token_index = 0;
	MatchState state(tokens, suggestions, parse_allocator, max_token_index);
	vector<UnicodeSpace> unicode_spaces;
	string clean_sql;
	const string &sql_ref = StripUnicodeSpaces(sql, clean_sql) ? clean_sql : sql;
	AutoCompleteTokenizer tokenizer(sql_ref, state);
	auto allow_complete = tokenizer.TokenizeInput();
	if (!allow_complete) {
		return {};
	}
	if (state.suggestions.empty()) {
		// no suggestions found during tokenizing
		// run the root matcher
		auto peg_matcher = provider.GetPEGMatcher();
		peg_matcher->Root().Match(state);
	}
	if (state.suggestions.empty()) {
		return {};
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
			new_suggestions = provider.SuggestCatalogName();
			break;
		case SuggestionState::SUGGEST_SCHEMA_NAME:
			new_suggestions = provider.SuggestSchemaName();
			break;
		case SuggestionState::SUGGEST_TABLE_NAME:
			new_suggestions = provider.SuggestTableName();
			break;
		case SuggestionState::SUGGEST_COLUMN_NAME:
			new_suggestions = provider.SuggestColumnName();
			break;
		case SuggestionState::SUGGEST_TYPE_NAME:
			new_suggestions = provider.SuggestType();
			break;
		case SuggestionState::SUGGEST_FILE_NAME:
			if (parameters.max_file_suggestion_count > 0) {
				new_suggestions = provider.SuggestFileName(tokenizer.last_word, suggestion_pos);
				parameters.suggestion_contains_files = true;
			}
			break;
		case SuggestionState::SUGGEST_SCALAR_FUNCTION_NAME:
			new_suggestions = provider.SuggestScalarFunctionName();
			break;
		case SuggestionState::SUGGEST_TABLE_FUNCTION_NAME:
			new_suggestions = provider.SuggestTableFunctionName();
			break;
		case SuggestionState::SUGGEST_PRAGMA_NAME:
			new_suggestions = provider.SuggestPragmaName();
			break;
		case SuggestionState::SUGGEST_SETTING_NAME:
			new_suggestions = provider.SuggestSettingName();
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
	return ComputeSuggestions(available_suggestions, tokenizer.last_word, parameters);
}

} // namespace duckdb
