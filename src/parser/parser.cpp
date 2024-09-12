#include "duckdb/parser/parser.hpp"

#include "duckdb/parser/group_by_node.hpp"
#include "duckdb/parser/parsed_data/create_table_info.hpp"
#include "duckdb/parser/parser_extension.hpp"
#include "duckdb/parser/query_error_context.hpp"
#include "duckdb/parser/query_node/select_node.hpp"
#include "duckdb/parser/statement/create_statement.hpp"
#include "duckdb/parser/statement/extension_statement.hpp"
#include "duckdb/parser/statement/select_statement.hpp"
#include "duckdb/parser/statement/update_statement.hpp"
#include "duckdb/parser/tableref/expressionlistref.hpp"
#include "duckdb/parser/transformer.hpp"
#include "parser/parser.hpp"
#include "postgres_parser.hpp"

namespace duckdb {

Parser::Parser(ParserOptions options_p) : options(options_p) {
}

struct UnicodeSpace {
	UnicodeSpace(idx_t pos, idx_t bytes) : pos(pos), bytes(bytes) {
	}

	idx_t pos;
	idx_t bytes;
};

static bool ReplaceUnicodeSpaces(const string &query, string &new_query, vector<UnicodeSpace> &unicode_spaces) {
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

static bool IsValidDollarQuotedStringTagFirstChar(const unsigned char &c) {
	// the first character can be between A-Z, a-z, or \200 - \377
	return (c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z') || c >= 0x80;
}

static bool IsValidDollarQuotedStringTagSubsequentChar(const unsigned char &c) {
	// subsequent characters can also be between 0-9
	return IsValidDollarQuotedStringTagFirstChar(c) || (c >= '0' && c <= '9');
}

// This function strips unicode space characters from the query and replaces them with regular spaces
// It returns true if any unicode space characters were found and stripped
// See here for a list of unicode space characters - https://jkorpela.fi/chars/spaces.html
bool Parser::StripUnicodeSpaces(const string &query_str, string &new_query) {
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

vector<string> SplitQueryStringIntoStatements(const string &query) {
	// Break sql string down into sql statements using the tokenizer
	vector<string> query_statements;
	auto tokens = Parser::Tokenize(query);
	idx_t next_statement_start = 0;
	for (idx_t i = 1; i < tokens.size(); ++i) {
		auto &t_prev = tokens[i - 1];
		auto &t = tokens[i];
		if (t_prev.type == SimplifiedTokenType::SIMPLIFIED_TOKEN_OPERATOR) {
			// LCOV_EXCL_START
			for (idx_t c = t_prev.start; c <= t.start; ++c) {
				if (query.c_str()[c] == ';') {
					query_statements.emplace_back(query.substr(next_statement_start, t.start - next_statement_start));
					next_statement_start = tokens[i].start;
				}
			}
			// LCOV_EXCL_STOP
		}
	}
	query_statements.emplace_back(query.substr(next_statement_start, query.size() - next_statement_start));
	return query_statements;
}

void Parser::ParseQuery(const string &query) {
	Transformer transformer(options);
	string parser_error;
	optional_idx parser_error_location;
	{
		// check if there are any unicode spaces in the string
		string new_query;
		if (StripUnicodeSpaces(query, new_query)) {
			// there are - strip the unicode spaces and re-run the query
			ParseQuery(new_query);
			return;
		}
	}
	{
		PostgresParser::SetPreserveIdentifierCase(options.preserve_identifier_case);
		bool parsing_succeed = false;
		// Creating a new scope to prevent multiple PostgresParser destructors being called
		// which led to some memory issues
		{
			PostgresParser parser;
			parser.Parse(query);
			if (parser.success) {
				if (!parser.parse_tree) {
					// empty statement
					return;
				}

				// if it succeeded, we transform the Postgres parse tree into a list of
				// SQLStatements
				transformer.TransformParseTree(parser.parse_tree, statements);
				parsing_succeed = true;
			} else {
				parser_error = parser.error_message;
				if (parser.error_location > 0) {
					parser_error_location = NumericCast<idx_t>(parser.error_location - 1);
				}
			}
		}
		// If DuckDB fails to parse the entire sql string, break the string down into individual statements
		// using ';' as the delimiter so that parser extensions can parse the statement
		if (parsing_succeed) {
			// no-op
			// return here would require refactoring into another function. o.w. will just no-op in order to run wrap up
			// code at the end of this function
		} else if (!options.extensions || options.extensions->empty()) {
			throw ParserException::SyntaxError(query, parser_error, parser_error_location);
		} else {
			// split sql string into statements and re-parse using extension
			auto query_statements = SplitQueryStringIntoStatements(query);
			idx_t stmt_loc = 0;
			for (auto const &query_statement : query_statements) {
				ErrorData another_parser_error;
				// Creating a new scope to allow extensions to use PostgresParser, which is not reentrant
				{
					PostgresParser another_parser;
					another_parser.Parse(query_statement);
					// LCOV_EXCL_START
					// first see if DuckDB can parse this individual query statement
					if (another_parser.success) {
						if (!another_parser.parse_tree) {
							// empty statement
							continue;
						}
						transformer.TransformParseTree(another_parser.parse_tree, statements);
						// important to set in the case of a mixture of DDB and parser ext statements
						statements.back()->stmt_length = query_statement.size() - 1;
						statements.back()->stmt_location = stmt_loc;
						stmt_loc += query_statement.size();
						continue;
					} else {
						another_parser_error = ErrorData(another_parser.error_message);
						if (another_parser.error_location > 0) {
							another_parser_error.AddQueryLocation(
							    NumericCast<idx_t>(another_parser.error_location - 1));
						}
					}
				} // LCOV_EXCL_STOP
				// LCOV_EXCL_START
				// let extensions parse the statement which DuckDB failed to parse
				bool parsed_single_statement = false;
				for (auto &ext : *options.extensions) {
					D_ASSERT(!parsed_single_statement);
					D_ASSERT(ext.parse_function);
					auto result = ext.parse_function(ext.parser_info.get(), query_statement);
					if (result.type == ParserExtensionResultType::PARSE_SUCCESSFUL) {
						auto statement = make_uniq<ExtensionStatement>(ext, std::move(result.parse_data));
						statement->stmt_length = query_statement.size() - 1;
						statement->stmt_location = stmt_loc;
						stmt_loc += query_statement.size();
						statements.push_back(std::move(statement));
						parsed_single_statement = true;
						break;
					} else if (result.type == ParserExtensionResultType::DISPLAY_EXTENSION_ERROR) {
						throw ParserException::SyntaxError(query, result.error, result.error_location);
					} else {
						// We move to the next one!
					}
				}
				if (!parsed_single_statement) {
					throw ParserException::SyntaxError(query, parser_error, parser_error_location);
				} // LCOV_EXCL_STOP
			}
		}
	}
	if (!statements.empty()) {
		auto &last_statement = statements.back();
		last_statement->stmt_length = query.size() - last_statement->stmt_location;
		for (auto &statement : statements) {
			statement->query = query;
			if (statement->type == StatementType::CREATE_STATEMENT) {
				auto &create = statement->Cast<CreateStatement>();
				create.info->sql = query.substr(statement->stmt_location, statement->stmt_length);
			}
		}
	}
}

vector<SimplifiedToken> Parser::Tokenize(const string &query) {
	auto pg_tokens = PostgresParser::Tokenize(query);
	vector<SimplifiedToken> result;
	result.reserve(pg_tokens.size());
	for (auto &pg_token : pg_tokens) {
		SimplifiedToken token;
		switch (pg_token.type) {
		case duckdb_libpgquery::PGSimplifiedTokenType::PG_SIMPLIFIED_TOKEN_IDENTIFIER:
			token.type = SimplifiedTokenType::SIMPLIFIED_TOKEN_IDENTIFIER;
			break;
		case duckdb_libpgquery::PGSimplifiedTokenType::PG_SIMPLIFIED_TOKEN_NUMERIC_CONSTANT:
			token.type = SimplifiedTokenType::SIMPLIFIED_TOKEN_NUMERIC_CONSTANT;
			break;
		case duckdb_libpgquery::PGSimplifiedTokenType::PG_SIMPLIFIED_TOKEN_STRING_CONSTANT:
			token.type = SimplifiedTokenType::SIMPLIFIED_TOKEN_STRING_CONSTANT;
			break;
		case duckdb_libpgquery::PGSimplifiedTokenType::PG_SIMPLIFIED_TOKEN_OPERATOR:
			token.type = SimplifiedTokenType::SIMPLIFIED_TOKEN_OPERATOR;
			break;
		case duckdb_libpgquery::PGSimplifiedTokenType::PG_SIMPLIFIED_TOKEN_KEYWORD:
			token.type = SimplifiedTokenType::SIMPLIFIED_TOKEN_KEYWORD;
			break;
		// comments are not supported by our tokenizer right now
		case duckdb_libpgquery::PGSimplifiedTokenType::PG_SIMPLIFIED_TOKEN_COMMENT: // LCOV_EXCL_START
			token.type = SimplifiedTokenType::SIMPLIFIED_TOKEN_COMMENT;
			break;
		default:
			throw InternalException("Unrecognized token category");
		} // LCOV_EXCL_STOP
		token.start = NumericCast<idx_t>(pg_token.start);
		result.push_back(token);
	}
	return result;
}

KeywordCategory ToKeywordCategory(duckdb_libpgquery::PGKeywordCategory type) {
	switch (type) {
	case duckdb_libpgquery::PGKeywordCategory::PG_KEYWORD_RESERVED:
		return KeywordCategory::KEYWORD_RESERVED;
	case duckdb_libpgquery::PGKeywordCategory::PG_KEYWORD_UNRESERVED:
		return KeywordCategory::KEYWORD_UNRESERVED;
	case duckdb_libpgquery::PGKeywordCategory::PG_KEYWORD_TYPE_FUNC:
		return KeywordCategory::KEYWORD_TYPE_FUNC;
	case duckdb_libpgquery::PGKeywordCategory::PG_KEYWORD_COL_NAME:
		return KeywordCategory::KEYWORD_COL_NAME;
	case duckdb_libpgquery::PGKeywordCategory::PG_KEYWORD_NONE:
		return KeywordCategory::KEYWORD_NONE;
	default:
		throw InternalException("Unrecognized keyword category");
	}
}

KeywordCategory Parser::IsKeyword(const string &text) {
	return ToKeywordCategory(PostgresParser::IsKeyword(text));
}

vector<ParserKeyword> Parser::KeywordList() {
	auto keywords = PostgresParser::KeywordList();
	vector<ParserKeyword> result;
	for (auto &kw : keywords) {
		ParserKeyword res;
		res.name = kw.text;
		res.category = ToKeywordCategory(kw.category);
		result.push_back(res);
	}
	return result;
}

vector<unique_ptr<ParsedExpression>> Parser::ParseExpressionList(const string &select_list, ParserOptions options) {
	// construct a mock query prefixed with SELECT
	string mock_query = "SELECT " + select_list;
	// parse the query
	Parser parser(options);
	parser.ParseQuery(mock_query);
	// check the statements
	if (parser.statements.size() != 1 || parser.statements[0]->type != StatementType::SELECT_STATEMENT) {
		throw ParserException("Expected a single SELECT statement");
	}
	auto &select = parser.statements[0]->Cast<SelectStatement>();
	if (select.node->type != QueryNodeType::SELECT_NODE) {
		throw ParserException("Expected a single SELECT node");
	}
	auto &select_node = select.node->Cast<SelectNode>();
	return std::move(select_node.select_list);
}

GroupByNode Parser::ParseGroupByList(const string &group_by, ParserOptions options) {
	// construct a mock SELECT query with our group_by expressions
	string mock_query = StringUtil::Format("SELECT 42 GROUP BY %s", group_by);
	// parse the query
	Parser parser(options);
	parser.ParseQuery(mock_query);
	// check the result
	if (parser.statements.size() != 1 || parser.statements[0]->type != StatementType::SELECT_STATEMENT) {
		throw ParserException("Expected a single SELECT statement");
	}
	auto &select = parser.statements[0]->Cast<SelectStatement>();
	D_ASSERT(select.node->type == QueryNodeType::SELECT_NODE);
	auto &select_node = select.node->Cast<SelectNode>();
	return std::move(select_node.groups);
}

vector<OrderByNode> Parser::ParseOrderList(const string &select_list, ParserOptions options) {
	// construct a mock query
	string mock_query = "SELECT * FROM tbl ORDER BY " + select_list;
	// parse the query
	Parser parser(options);
	parser.ParseQuery(mock_query);
	// check the statements
	if (parser.statements.size() != 1 || parser.statements[0]->type != StatementType::SELECT_STATEMENT) {
		throw ParserException("Expected a single SELECT statement");
	}
	auto &select = parser.statements[0]->Cast<SelectStatement>();
	D_ASSERT(select.node->type == QueryNodeType::SELECT_NODE);
	auto &select_node = select.node->Cast<SelectNode>();
	if (select_node.modifiers.empty() || select_node.modifiers[0]->type != ResultModifierType::ORDER_MODIFIER ||
	    select_node.modifiers.size() != 1) {
		throw ParserException("Expected a single ORDER clause");
	}
	auto &order = select_node.modifiers[0]->Cast<OrderModifier>();
	return std::move(order.orders);
}

void Parser::ParseUpdateList(const string &update_list, vector<string> &update_columns,
                             vector<unique_ptr<ParsedExpression>> &expressions, ParserOptions options) {
	// construct a mock query
	string mock_query = "UPDATE tbl SET " + update_list;
	// parse the query
	Parser parser(options);
	parser.ParseQuery(mock_query);
	// check the statements
	if (parser.statements.size() != 1 || parser.statements[0]->type != StatementType::UPDATE_STATEMENT) {
		throw ParserException("Expected a single UPDATE statement");
	}
	auto &update = parser.statements[0]->Cast<UpdateStatement>();
	update_columns = std::move(update.set_info->columns);
	expressions = std::move(update.set_info->expressions);
}

vector<vector<unique_ptr<ParsedExpression>>> Parser::ParseValuesList(const string &value_list, ParserOptions options) {
	// construct a mock query
	string mock_query = "VALUES " + value_list;
	// parse the query
	Parser parser(options);
	parser.ParseQuery(mock_query);
	// check the statements
	if (parser.statements.size() != 1 || parser.statements[0]->type != StatementType::SELECT_STATEMENT) {
		throw ParserException("Expected a single SELECT statement");
	}
	auto &select = parser.statements[0]->Cast<SelectStatement>();
	if (select.node->type != QueryNodeType::SELECT_NODE) {
		throw ParserException("Expected a single SELECT node");
	}
	auto &select_node = select.node->Cast<SelectNode>();
	if (!select_node.from_table || select_node.from_table->type != TableReferenceType::EXPRESSION_LIST) {
		throw ParserException("Expected a single VALUES statement");
	}
	auto &values_list = select_node.from_table->Cast<ExpressionListRef>();
	return std::move(values_list.values);
}

ColumnList Parser::ParseColumnList(const string &column_list, ParserOptions options) {
	string mock_query = "CREATE TABLE tbl (" + column_list + ")";
	Parser parser(options);
	parser.ParseQuery(mock_query);
	if (parser.statements.size() != 1 || parser.statements[0]->type != StatementType::CREATE_STATEMENT) {
		throw ParserException("Expected a single CREATE statement");
	}
	auto &create = parser.statements[0]->Cast<CreateStatement>();
	if (create.info->type != CatalogType::TABLE_ENTRY) {
		throw InternalException("Expected a single CREATE TABLE statement");
	}
	auto &info = create.info->Cast<CreateTableInfo>();
	return std::move(info.columns);
}

} // namespace duckdb
