//===----------------------------------------------------------------------===//
//                         DuckDB
//
// sql_formatter.cpp
//
// SQL pretty-printer that uses the PEG tokenizer for lexical analysis.
//
//===----------------------------------------------------------------------===//

#include "sql_formatter.hpp"
#include "parser/tokenizer/highlight_tokenizer.hpp"
#include "matcher.hpp"
#include "duckdb/common/string_util.hpp"

#include <unordered_set>

namespace duckdb {

// ─────────────────────────────────────────────────────────────────────────────
// Keyword classification helpers
// ─────────────────────────────────────────────────────────────────────────────

//! Returns true if kw (already uppercased) is a structural clause keyword that
//! starts a new line in the formatted output.
static bool IsClauseKeyword(const string &kw) {
	static const std::unordered_set<string> clause_keywords = {
	    "SELECT",  "FROM",      "WHERE",   "HAVING",  "LIMIT",   "OFFSET", "JOIN",
	    "UNION",   "INTERSECT", "EXCEPT",  "WITH",    "INSERT",  "UPDATE", "DELETE",
	    "SET",     "RETURNING", "VALUES",  "CREATE",  "DROP",    "ALTER",  "TRUNCATE",
	    "QUALIFY", "PIVOT",     "UNPIVOT", "REFRESH", "INSTALL", "LOAD",   "ATTACH",
	    "DETACH",  "CHECKPOINT"};
	return clause_keywords.count(kw) > 0;
}

static bool IsJoinModifier(const string &kw) {
	return kw == "INNER" || kw == "LEFT" || kw == "RIGHT" || kw == "FULL" || kw == "CROSS" || kw == "NATURAL";
}

//! Complete set of clause keyword *strings* as they appear in the formatted
//! output (single words AND compound forms).  Used by the post-processing pass.
static bool IsClauseKeywordLine(const string &trimmed) {
	static const std::unordered_set<string> all_clause_strings = {
	    // Single-word clause starters
	    "SELECT",    "FROM",        "WHERE",     "HAVING",     "LIMIT",      "OFFSET",
	    "JOIN",      "UNION",       "INTERSECT", "EXCEPT",     "WITH",       "INSERT",
	    "UPDATE",    "DELETE",      "SET",       "RETURNING",  "VALUES",     "CREATE",
	    "DROP",      "ALTER",       "TRUNCATE",  "QUALIFY",    "PIVOT",      "UNPIVOT",
	    "REFRESH",   "INSTALL",     "LOAD",      "ATTACH",     "DETACH",     "CHECKPOINT",
	    // Compound clause keywords
	    "GROUP BY",       "ORDER BY",        "UNION ALL",       "UNION DISTINCT",
	    "INTERSECT ALL",  "INTERSECT DISTINCT", "EXCEPT ALL",  "EXCEPT DISTINCT",
	    "INNER JOIN",     "CROSS JOIN",      "NATURAL JOIN",
	    "LEFT JOIN",      "LEFT OUTER JOIN",
	    "RIGHT JOIN",     "RIGHT OUTER JOIN",
	    "FULL JOIN",      "FULL OUTER JOIN",
	    "INSERT INTO",    "DELETE FROM",     "ON CONFLICT"};
	// Compare case-insensitively by using the already-uppercased trimmed line.
	return all_clause_strings.count(StringUtil::Upper(trimmed)) > 0;
}

//! Structural keywords that are always uppercased.  Unreserved keywords used
//! as identifiers (e.g. "name", "value", "type") are excluded so their
//! original casing is preserved.
static bool ShouldUppercase(const string &kw) {
	static const std::unordered_set<string> uppercase_set = {
	    // Clause starters & set operators
	    "SELECT", "DISTINCT", "FROM", "WHERE", "HAVING", "LIMIT", "OFFSET", "GROUP", "BY", "ORDER", "UNION",
	    "INTERSECT", "EXCEPT", "ALL", "JOIN", "INNER", "LEFT", "RIGHT", "FULL", "OUTER", "CROSS", "NATURAL", "ON",
	    "USING", "WITH", "RECURSIVE",
	    // DML
	    "INSERT", "INTO", "UPDATE", "DELETE", "MERGE", "MATCHED", "SET", "VALUES", "RETURNING", "OVERRIDING",
	    // DDL
	    "CREATE", "DROP", "ALTER", "TRUNCATE", "TABLE", "VIEW", "INDEX", "SCHEMA", "DATABASE", "SEQUENCE",
	    "MATERIALIZED", "TEMP", "TEMPORARY", "IF", "NOT", "EXISTS", "OR", "REPLACE", "UNIQUE", "PRIMARY", "KEY",
	    "FOREIGN", "REFERENCES", "DEFAULT", "CONSTRAINT", "CHECK", "GENERATED", "ALWAYS", "IDENTITY", "STORED",
	    "VIRTUAL", "COLUMN", "ADD", "RENAME", "COPY",
	    // Boolean / NULL operators
	    "AND", "OR", "NOT", "IS", "NULL", "TRUE", "FALSE", "IN", "LIKE", "ILIKE", "GLOB", "BETWEEN", "SIMILAR",
	    "SOME", "ANY", "EXISTS", "OVERLAPS",
	    // CASE expression
	    "CASE", "WHEN", "THEN", "ELSE", "END",
	    // Aliases & ordering
	    "AS", "ASC", "DESC", "NULLS", "FIRST", "LAST",
	    // Window functions
	    "OVER", "PARTITION", "ROWS", "RANGE", "GROUPS", "UNBOUNDED", "PRECEDING", "FOLLOWING", "CURRENT", "ROW",
	    "EXCLUDE", "TIES", "OTHERS",
	    // Type casting
	    "CAST", "TRY_CAST",
	    // Pivoting / qualify
	    "QUALIFY", "PIVOT", "UNPIVOT", "IN",
	    // Transactions
	    "BEGIN", "COMMIT", "ROLLBACK", "TRANSACTION", "SAVEPOINT", "RELEASE",
	    // Misc
	    "EXPLAIN", "ANALYZE", "DESCRIBE", "SHOW", "PRAGMA", "ATTACH", "DETACH", "CHECKPOINT", "VACUUM", "LOAD",
	    "INSTALL", "FORCE", "POSITIONAL", "ASOF", "LATERAL", "TABLESAMPLE", "REPEATABLE", "USING", "SYSTEM",
	    "BERNOULLI", "RESERVOIR"};
	return uppercase_set.count(kw) > 0;
}

// ─────────────────────────────────────────────────────────────────────────────
// Token lookahead helpers
// ─────────────────────────────────────────────────────────────────────────────

static string PeekKeyword(const vector<MatcherToken> &tokens, idx_t i, idx_t offset) {
	idx_t j = i + offset;
	if (j < tokens.size() && tokens[j].type == TokenType::KEYWORD) {
		return StringUtil::Upper(tokens[j].text);
	}
	return "";
}

//! Detect a compound clause keyword starting at tokens[i].
//! Returns the number of *additional* tokens consumed (caller advances by 1+return).
//! Returns static_cast<idx_t>(-1) if tokens[i] is NOT a clause keyword.
//! Fills compound_text with the uppercased keyword string (e.g. "GROUP BY").
//! Fills original_text with the original token text joined by spaces (e.g. "group by").
static idx_t DetectCompoundClause(const vector<MatcherToken> &tokens, idx_t i, string &compound_text,
                                   string &original_text) {
	const string kw = StringUtil::Upper(tokens[i].text);
	const string &orig0 = tokens[i].text;

	if ((kw == "GROUP" || kw == "ORDER") && PeekKeyword(tokens, i, 1) == "BY") {
		compound_text = kw + " BY";
		original_text = orig0 + " " + tokens[i + 1].text;
		return 1;
	}
	if (kw == "UNION" || kw == "INTERSECT" || kw == "EXCEPT") {
		const string next = PeekKeyword(tokens, i, 1);
		if (next == "ALL" || next == "DISTINCT") {
			compound_text = kw + " " + next;
			original_text = orig0 + " " + tokens[i + 1].text;
			return 1;
		}
		compound_text = kw;
		original_text = orig0;
		return 0;
	}
	if (kw == "INNER" || kw == "CROSS" || kw == "NATURAL") {
		if (PeekKeyword(tokens, i, 1) == "JOIN") {
			compound_text = kw + " JOIN";
			original_text = orig0 + " " + tokens[i + 1].text;
			return 1;
		}
	}
	if (kw == "LEFT" || kw == "RIGHT" || kw == "FULL") {
		if (PeekKeyword(tokens, i, 1) == "JOIN") {
			compound_text = kw + " JOIN";
			original_text = orig0 + " " + tokens[i + 1].text;
			return 1;
		}
		if (PeekKeyword(tokens, i, 1) == "OUTER" && PeekKeyword(tokens, i, 2) == "JOIN") {
			compound_text = kw + " OUTER JOIN";
			original_text = orig0 + " " + tokens[i + 1].text + " " + tokens[i + 2].text;
			return 2;
		}
	}
	if (kw == "INSERT" && PeekKeyword(tokens, i, 1) == "INTO") {
		compound_text = "INSERT INTO";
		original_text = orig0 + " " + tokens[i + 1].text;
		return 1;
	}
	if (kw == "DELETE" && PeekKeyword(tokens, i, 1) == "FROM") {
		compound_text = "DELETE FROM";
		original_text = orig0 + " " + tokens[i + 1].text;
		return 1;
	}
	if (kw == "ON" && PeekKeyword(tokens, i, 1) == "CONFLICT") {
		compound_text = "ON CONFLICT";
		original_text = orig0 + " " + tokens[i + 1].text;
		return 1;
	}
	if (IsClauseKeyword(kw)) {
		compound_text = kw;
		original_text = orig0;
		return 0;
	}
	if (IsJoinModifier(kw)) {
		compound_text = kw;
		original_text = orig0;
		return 0;
	}
	return static_cast<idx_t>(-1);
}

// ─────────────────────────────────────────────────────────────────────────────
// Phase 1: token-stream → multiline string
// ─────────────────────────────────────────────────────────────────────────────

static string FormatMultiline(const vector<MatcherToken> &tokens, const FormatterConfig &config) {
	string result;
	result.reserve(tokens.size() * 8);

	bool at_line_start = true;
	bool after_clause = false;
	bool prev_was_keyword = false;

	// Per-paren level state: tracks indentation and whether clauses were emitted inside.
	struct ParenInfo {
		bool has_clauses;
		int32_t close_indent;        //! indent for the closing ')'
		int32_t inner_clause_indent; //! indent for clause keywords inside this paren
	};
	vector<ParenInfo> paren_stack;

	const auto indent_size = static_cast<int32_t>(config.indent_size);

	auto write_indent = [&](int32_t n) {
		for (int32_t j = 0; j < n; j++) {
			result += ' ';
		}
		at_line_start = false;
	};
	auto write_newline = [&]() {
		result += '\n';
		at_line_start = true;
	};
	auto write_space = [&]() {
		if (!at_line_start && !result.empty() && result.back() != ' ' && result.back() != '(' &&
		    result.back() != '.') {
			result += ' ';
		}
	};
	// clause_indent: where clause keywords are placed (column of SELECT/FROM/WHERE/etc.)
	auto clause_indent = [&]() -> int32_t {
		return paren_stack.empty() ? 0 : paren_stack.back().inner_clause_indent;
	};
	// content_indent: one level deeper than the clause keyword
	auto content_indent = [&]() -> int32_t { return clause_indent() + indent_size; };

	// Apply keyword_case to a keyword.  upper_kw is the uppercased form,
	// orig_kw is the original token text (or joined original tokens for compounds).
	// is_structural=true means the keyword is a clause/set-operator keyword that is
	// always uppercased in UPPER mode (bypasses the ShouldUppercase whitelist check).
	auto apply_case = [&](const string &upper_kw, const string &orig_kw, bool is_structural = false) -> string {
		switch (config.keyword_case) {
		case KeywordCase::UPPER:
			return (is_structural || ShouldUppercase(upper_kw)) ? upper_kw : orig_kw;
		case KeywordCase::LOWER:
			return StringUtil::Lower(upper_kw);
		case KeywordCase::PRESERVE:
			return orig_kw;
		default:
			return upper_kw;
		}
	};

	auto emit_clause = [&](const string &kw_text) {
		if (!result.empty()) {
			write_newline();
			write_indent(clause_indent());
		}
		result += kw_text;
		at_line_start = false;
		after_clause = true;
		prev_was_keyword = true;
		if (!paren_stack.empty()) {
			paren_stack.back().has_clauses = true;
		}
	};

	for (idx_t i = 0; i < tokens.size();) {
		const auto &tok = tokens[i];

		// Terminator
		if (tok.type == TokenType::TERMINATOR) {
			after_clause = false;
			prev_was_keyword = false;
			result += ';';
			at_line_start = false;
			i++;
			continue;
		}

		// Comment
		if (tok.type == TokenType::COMMENT) {
			write_space();
			result += tok.text;
			if (tok.text.size() >= 2 && tok.text[0] == '-' && tok.text[1] == '-') {
				write_newline();
			}
			after_clause = false;
			prev_was_keyword = false;
			i++;
			continue;
		}

		// Opening paren
		if (tok.type == TokenType::OPERATOR && tok.text == "(") {
			if (prev_was_keyword && !at_line_start) {
				write_space();
			}
			after_clause = false;
			prev_was_keyword = false;
			result += '(';
			at_line_start = false;
			// Record the current content level: closing ')' aligns with it,
			// and clause keywords inside are one level deeper still.
			int32_t ci = content_indent();
			paren_stack.push_back({false, ci, ci + indent_size});
			i++;
			continue;
		}

		// Closing paren
		if (tok.type == TokenType::OPERATOR && tok.text == ")") {
			after_clause = false;
			prev_was_keyword = false;
			bool had_clauses = !paren_stack.empty() && paren_stack.back().has_clauses;
			int32_t close_ind = paren_stack.empty() ? 0 : paren_stack.back().close_indent;
			if (!paren_stack.empty()) {
				paren_stack.pop_back();
			}
			if (had_clauses && !at_line_start) {
				write_newline();
				write_indent(close_ind);
			}
			result += ')';
			at_line_start = false;
			i++;
			continue;
		}

		// Comma
		if (tok.type == TokenType::OPERATOR && tok.text == ",") {
			after_clause = false;
			prev_was_keyword = false;
			result += ',';
			at_line_start = false;
			if (paren_stack.empty()) {
				write_newline();
				write_indent(content_indent());
			} else {
				result += ' ';
			}
			i++;
			continue;
		}

		// Dot — classified as NUMBER_LITERAL by the tokenizer
		if (tok.text == "." && (tok.type == TokenType::OPERATOR || tok.type == TokenType::NUMBER_LITERAL)) {
			after_clause = false;
			prev_was_keyword = false;
			if (!result.empty() && result.back() == ' ') {
				result.pop_back();
			}
			result += '.';
			at_line_start = false;
			i++;
			continue;
		}

		// Keyword tokens
		if (tok.type == TokenType::KEYWORD) {
			string compound_text;
			string original_text;
			idx_t extra = DetectCompoundClause(tokens, i, compound_text, original_text);

			if (extra != static_cast<idx_t>(-1)) {
				emit_clause(apply_case(compound_text, original_text, /*is_structural=*/true));
				i += 1 + extra;
				continue;
			}

			const string upper = StringUtil::Upper(tok.text);

			// AND / OR on their own indented line
			if ((upper == "AND" || upper == "OR") && !at_line_start) {
				write_newline();
				write_indent(content_indent());
				result += apply_case(upper, tok.text, /*is_structural=*/true);
				at_line_start = false;
				after_clause = false;
				prev_was_keyword = true;
				i++;
				continue;
			}

			// Generic keyword
			if (after_clause) {
				write_newline();
				write_indent(content_indent());
				after_clause = false;
			} else if (!result.empty() && result.back() != '.') {
				write_space();
			}
			result += apply_case(upper, tok.text);
			at_line_start = false;
			prev_was_keyword = true;
			i++;
			continue;
		}

		// All other tokens (identifiers, literals, misc operators)
		if (after_clause) {
			write_newline();
			write_indent(content_indent());
			after_clause = false;
		} else if (!result.empty() && result.back() != '.') {
			write_space();
		}
		result += tok.text;
		at_line_start = false;
		prev_was_keyword = false;
		i++;
	}

	return result;
}

// ─────────────────────────────────────────────────────────────────────────────
// Phase 2: inline short clauses
// ─────────────────────────────────────────────────────────────────────────────

//! Count the number of leading space characters in a line.
static idx_t LeadingSpaces(const string &line) {
	idx_t n = 0;
	while (n < line.size() && line[n] == ' ') {
		n++;
	}
	return n;
}

//! Return the substring of line after leading whitespace.
static string TrimLeft(const string &line) {
	idx_t n = LeadingSpaces(line);
	return line.substr(n);
}

//! Post-processing pass: walk through the already-formatted multiline output
//! and merge clause-keyword lines with their content when the combined line
//! fits within config.inline_threshold characters.
//!
//! A clause is only inlined when its content is "simple" — that is, none of
//! the content lines is itself a clause keyword (which would indicate a nested
//! subquery structure that shouldn't be flattened).
static string MergeShortClauses(const string &formatted, const FormatterConfig &config) {
	if (config.inline_threshold == 0) {
		return formatted;
	}

	// Split into lines (keep the strings; we'll rejoin at the end)
	vector<string> lines;
	{
		idx_t start = 0;
		for (idx_t k = 0; k <= formatted.size(); k++) {
			if (k == formatted.size() || formatted[k] == '\n') {
				lines.push_back(formatted.substr(start, k - start));
				start = k + 1;
			}
		}
	}

	vector<string> out;
	out.reserve(lines.size());

	for (idx_t i = 0; i < lines.size();) {
		const string &line = lines[i];
		const idx_t clause_ind = LeadingSpaces(line);
		const string trimmed = TrimLeft(line);

		if (!trimmed.empty() && IsClauseKeywordLine(trimmed)) {
			// Collect the content lines that belong to this clause.
			// Content lines are those indented strictly deeper than the clause keyword.
			// We stop as soon as:
			//   (a) indentation drops back to clause_ind or less, OR
			//   (b) a content line is itself a clause keyword (nested structure).
			const idx_t content_ind = clause_ind + config.indent_size;
			bool is_simple = true;
			idx_t j = i + 1;
			idx_t content_end = i + 1; // exclusive end of content range

			while (j < lines.size()) {
				const string &cline = lines[j];
				const string ctrimmed = TrimLeft(cline);

				if (ctrimmed.empty()) {
					// Empty line ends the clause
					break;
				}
				const idx_t cind = LeadingSpaces(cline);
				if (cind <= clause_ind) {
					// Back to outer level — stop before this line
					break;
				}
				// Content lines must be at exactly content_ind (direct children).
				// If they're indented deeper, this is a sub-structured block.
				if (cind > content_ind) {
					is_simple = false;
					break;
				}
				// A content line that is itself a clause keyword means there's a
				// nested clause structure (e.g. a subquery) — don't inline.
				if (IsClauseKeywordLine(ctrimmed)) {
					is_simple = false;
					break;
				}
				content_end = j + 1;
				j++;
			}

			if (is_simple && content_end > i + 1) {
				// Flatten content into a single space-separated string.
				string flat;
				for (idx_t k = i + 1; k < content_end; k++) {
					if (!flat.empty()) {
						flat += ' ';
					}
					flat += TrimLeft(lines[k]);
				}
				string merged = string(clause_ind, ' ') + trimmed + ' ' + flat;
				if (merged.size() <= config.inline_threshold) {
					out.push_back(std::move(merged));
					i = content_end;
					continue;
				}
			}
		}

		out.push_back(line);
		i++;
	}

	// Rejoin
	string result;
	result.reserve(formatted.size());
	for (idx_t k = 0; k < out.size(); k++) {
		if (k > 0) {
			result += '\n';
		}
		result += out[k];
	}
	return result;
}

// ─────────────────────────────────────────────────────────────────────────────
// Phase 3: lift first WHERE/HAVING predicate onto the keyword's line
// ─────────────────────────────────────────────────────────────────────────────

//! True when trimmed is exactly a WHERE or HAVING keyword (any case).
static bool IsConditionClauseLine(const string &trimmed) {
	const string upper = StringUtil::Upper(trimmed);
	return upper == "WHERE" || upper == "HAVING";
}

//! True when trimmed content starts with an AND/OR conjunction keyword (any case).
static bool StartsWithConjunction(const string &trimmed) {
	const string upper = StringUtil::Upper(trimmed);
	if (upper.size() >= 4 && upper.substr(0, 4) == "AND ") {
		return true;
	}
	if (upper.size() >= 3 && upper.substr(0, 3) == "OR ") {
		return true;
	}
	return false;
}

//! Phase 3: for WHERE/HAVING clauses left multi-line by Phase 2, lift the first
//! predicate onto the keyword line and align subsequent AND/OR conjunctions at
//! clause_indent+2 (so their content starts at the same column as the WHERE content).
//!
//! Only applies when config.inline_threshold > 0 and the lifted first line fits
//! within the threshold.  Clauses containing deeper-nested content (subqueries)
//! are left untouched.
static string CollapseFirstCondition(const string &formatted, const FormatterConfig &config) {
	if (config.inline_threshold == 0) {
		return formatted;
	}

	vector<string> lines;
	{
		idx_t start = 0;
		for (idx_t k = 0; k <= formatted.size(); k++) {
			if (k == formatted.size() || formatted[k] == '\n') {
				lines.push_back(formatted.substr(start, k - start));
				start = k + 1;
			}
		}
	}

	vector<string> out;
	out.reserve(lines.size());

	for (idx_t i = 0; i < lines.size();) {
		const string &line = lines[i];
		const idx_t clause_ind = LeadingSpaces(line);
		const string trimmed = TrimLeft(line);

		if (!IsConditionClauseLine(trimmed)) {
			out.push_back(line);
			i++;
			continue;
		}

		const idx_t content_ind = clause_ind + config.indent_size;

		// Collect content lines; bail if any are deeper-nested (subquery etc.)
		bool is_simple = true;
		idx_t j = i + 1;
		while (j < lines.size()) {
			const string ctrimmed = TrimLeft(lines[j]);
			if (ctrimmed.empty()) {
				break;
			}
			const idx_t cind = LeadingSpaces(lines[j]);
			if (cind <= clause_ind) {
				break;
			}
			if (cind > content_ind) {
				is_simple = false;
				break;
			}
			j++;
		}

		const idx_t content_count = j - (i + 1);

		if (!is_simple || content_count == 0) {
			out.push_back(line);
			i++;
			continue;
		}

		// Check that the first predicate fits on the WHERE line.
		const string first_content = TrimLeft(lines[i + 1]);
		const string merged_first = string(clause_ind, ' ') + trimmed + ' ' + first_content;
		if (merged_first.size() > config.inline_threshold) {
			out.push_back(line);
			i++;
			continue;
		}

		// Lift first predicate onto WHERE/HAVING line.
		out.push_back(merged_first);

		// Subsequent lines: AND/OR conjunctions get a 2-space indent relative to
		// the clause keyword so their content aligns with the WHERE content.
		for (idx_t k = i + 2; k < j; k++) {
			const string ctrimmed = TrimLeft(lines[k]);
			if (StartsWithConjunction(ctrimmed)) {
				out.push_back(string(clause_ind + 2, ' ') + ctrimmed);
			} else {
				out.push_back(lines[k]);
			}
		}

		i = j;
	}

	string result;
	result.reserve(formatted.size());
	for (idx_t k = 0; k < out.size(); k++) {
		if (k > 0) {
			result += '\n';
		}
		result += out[k];
	}
	return result;
}

// ─────────────────────────────────────────────────────────────────────────────
// Public entry point
// ─────────────────────────────────────────────────────────────────────────────

string FormatSQL(const string &sql, const FormatterConfig &config) {
	HighlightTokenizer tokenizer(sql);
	tokenizer.TokenizeInput();
	const auto &tokens = tokenizer.tokens;

	if (tokens.empty()) {
		return sql;
	}

	string multiline = FormatMultiline(tokens, config);
	string merged = MergeShortClauses(multiline, config);
	return CollapseFirstCondition(merged, config);
}

} // namespace duckdb
