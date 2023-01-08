#include "duckdb/parser/parser.hpp"

#include "duckdb/parser/transformer.hpp"
#include "duckdb/parser/parsed_data/create_table_info.hpp"
#include "duckdb/parser/statement/create_statement.hpp"
#include "duckdb/parser/statement/extension_statement.hpp"
#include "duckdb/parser/statement/select_statement.hpp"
#include "duckdb/parser/statement/update_statement.hpp"
#include "duckdb/parser/query_node/select_node.hpp"
#include "duckdb/parser/tableref/expressionlistref.hpp"
#include "postgres_parser.hpp"
#include "duckdb/parser/query_error_context.hpp"
#include "duckdb/parser/parser_extension.hpp"

#include "parser/parser.hpp"

namespace duckdb {

Parser::Parser(ParserOptions options_p) : options(options_p) {
}

static bool ReplaceUnicodeSpaces(const string &query, string &new_query, vector<idx_t> &unicode_spaces) {
	const idx_t UNICODE_SPACE_BYTES = 3;
	if (unicode_spaces.empty()) {
		// no unicode spaces found
		return false;
	}
	idx_t prev = 0;
	for (idx_t pos : unicode_spaces) {
		new_query += query.substr(prev, pos - prev);
		new_query += " ";
		prev = pos + UNICODE_SPACE_BYTES;
	}
	new_query += query.substr(prev, query.size() - prev);
	return true;
}

// This function strips unicode space characters from the query and replaces them with regular spaces
// It returns true if any unicode space characters were found and stripped
// See here for a list of unicode space characters - https://www.compart.com/en/unicode/category/Zs
static bool StripUnicodeSpaces(const string &query, string &new_query) {
	idx_t pos = 0;
	char quote;
	vector<idx_t> unicode_spaces;

regular:
	for (; pos + 2 < query.size(); pos++) {
		if (query[pos] == '\xe2') {
			if (query[pos + 1] == '\x80') {
				if (query[pos + 2] >= '\x80' && query[pos + 2] <= '\x8b') {
					// e28080 - e2808b
					unicode_spaces.push_back(pos);
				} else if (query[pos + 2] == '\xaf') {
					// e280af
					unicode_spaces.push_back(pos);
				}
			} else if (query[pos + 1] == '\x81') {
				if (query[pos + 2] == '\x9f') {
					// e2819f
					unicode_spaces.push_back(pos);
				}
			}
		} else if (query[pos] == '\xef') {
			if (query[pos + 1] == '\xbb' && query[pos + 2] == '\xbf') {
				// efbbbf
				unicode_spaces.push_back(pos);
			}
		} else if (query[pos] == '\xe3') {
			if (query[pos + 1] == '\x80' && query[pos + 2] == '\x80') {
				// e38080
				unicode_spaces.push_back(pos);
			}
		} else if (query[pos] == '"' || query[pos] == '\'') {
			quote = query[pos];
			pos++;
			goto in_quotes;
		} else if (query[pos] == '-' && query[pos + 1] == '-') {
			goto in_comment;
		}
	}
	goto end;
in_quotes:
	for (; pos + 1 < query.size(); pos++) {
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
in_comment:
	for (; pos < query.size(); pos++) {
		if (query[pos] == '\n' || query[pos] == '\r') {
			goto regular;
		}
	}
	goto end;
end:
	return ReplaceUnicodeSpaces(query, new_query, unicode_spaces);
}

void Parser::ParseQuery(const string &query) {
	Transformer transformer(options.max_expression_depth);
	string parser_error;
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
		} else {
			parser_error = QueryErrorContext::Format(query, parser.error_message, parser.error_location - 1);
		}
	}
	if (!parser_error.empty()) {
		if (options.extensions) {
			for (auto &ext : *options.extensions) {
				D_ASSERT(ext.parse_function);
				auto result = ext.parse_function(ext.parser_info.get(), query);
				if (result.type == ParserExtensionResultType::PARSE_SUCCESSFUL) {
					auto statement = make_unique<ExtensionStatement>(ext, move(result.parse_data));
					statement->stmt_length = query.size();
					statement->stmt_location = 0;
					statements.push_back(move(statement));
					return;
				}
				if (result.type == ParserExtensionResultType::DISPLAY_EXTENSION_ERROR) {
					throw ParserException(result.error);
				}
			}
		}
		throw ParserException(parser_error);
	}
	if (!statements.empty()) {
		auto &last_statement = statements.back();
		last_statement->stmt_length = query.size() - last_statement->stmt_location;
		for (auto &statement : statements) {
			statement->query = query;
			if (statement->type == StatementType::CREATE_STATEMENT) {
				auto &create = (CreateStatement &)*statement;
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
		token.start = pg_token.start;
		result.push_back(token);
	}
	return result;
}

bool Parser::IsKeyword(const string &text) {
	return PostgresParser::IsKeyword(text);
}

vector<ParserKeyword> Parser::KeywordList() {
	auto keywords = PostgresParser::KeywordList();
	vector<ParserKeyword> result;
	for (auto &kw : keywords) {
		ParserKeyword res;
		res.name = kw.text;
		switch (kw.category) {
		case duckdb_libpgquery::PGKeywordCategory::PG_KEYWORD_RESERVED:
			res.category = KeywordCategory::KEYWORD_RESERVED;
			break;
		case duckdb_libpgquery::PGKeywordCategory::PG_KEYWORD_UNRESERVED:
			res.category = KeywordCategory::KEYWORD_UNRESERVED;
			break;
		case duckdb_libpgquery::PGKeywordCategory::PG_KEYWORD_TYPE_FUNC:
			res.category = KeywordCategory::KEYWORD_TYPE_FUNC;
			break;
		case duckdb_libpgquery::PGKeywordCategory::PG_KEYWORD_COL_NAME:
			res.category = KeywordCategory::KEYWORD_COL_NAME;
			break;
		default:
			throw InternalException("Unrecognized keyword category");
		}
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
	auto &select = (SelectStatement &)*parser.statements[0];
	if (select.node->type != QueryNodeType::SELECT_NODE) {
		throw ParserException("Expected a single SELECT node");
	}
	auto &select_node = (SelectNode &)*select.node;
	return move(select_node.select_list);
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
	auto &select = (SelectStatement &)*parser.statements[0];
	if (select.node->type != QueryNodeType::SELECT_NODE) {
		throw ParserException("Expected a single SELECT node");
	}
	auto &select_node = (SelectNode &)*select.node;
	if (select_node.modifiers.empty() || select_node.modifiers[0]->type != ResultModifierType::ORDER_MODIFIER ||
	    select_node.modifiers.size() != 1) {
		throw ParserException("Expected a single ORDER clause");
	}
	auto &order = (OrderModifier &)*select_node.modifiers[0];
	return move(order.orders);
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
	auto &update = (UpdateStatement &)*parser.statements[0];
	update_columns = move(update.columns);
	expressions = move(update.expressions);
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
	auto &select = (SelectStatement &)*parser.statements[0];
	if (select.node->type != QueryNodeType::SELECT_NODE) {
		throw ParserException("Expected a single SELECT node");
	}
	auto &select_node = (SelectNode &)*select.node;
	if (!select_node.from_table || select_node.from_table->type != TableReferenceType::EXPRESSION_LIST) {
		throw ParserException("Expected a single VALUES statement");
	}
	auto &values_list = (ExpressionListRef &)*select_node.from_table;
	return move(values_list.values);
}

ColumnList Parser::ParseColumnList(const string &column_list, ParserOptions options) {
	string mock_query = "CREATE TABLE blabla (" + column_list + ")";
	Parser parser(options);
	parser.ParseQuery(mock_query);
	if (parser.statements.size() != 1 || parser.statements[0]->type != StatementType::CREATE_STATEMENT) {
		throw ParserException("Expected a single CREATE statement");
	}
	auto &create = (CreateStatement &)*parser.statements[0];
	if (create.info->type != CatalogType::TABLE_ENTRY) {
		throw InternalException("Expected a single CREATE TABLE statement");
	}
	auto &info = ((CreateTableInfo &)*create.info);
	return move(info.columns);
}

} // namespace duckdb
