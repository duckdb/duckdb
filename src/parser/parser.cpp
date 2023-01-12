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

// This function strips unicode space characters from the query and replaces them with regular spaces
// It returns true if any unicode space characters were found and stripped
// See here for a list of unicode space characters - https://jkorpela.fi/chars/spaces.html
static bool StripUnicodeSpaces(const string &query_str, string &new_query) {
	const idx_t NBSP_LEN = 2;
	const idx_t USP_LEN = 3;
	idx_t pos = 0;
	unsigned char quote;
	vector<UnicodeSpace> unicode_spaces;
	auto query = (unsigned char *)query_str.c_str();
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
					auto statement = make_unique<ExtensionStatement>(ext, std::move(result.parse_data));
					statement->stmt_length = query.size();
					statement->stmt_location = 0;
					statements.push_back(std::move(statement));
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
	return std::move(select_node.select_list);
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
	auto &update = (UpdateStatement &)*parser.statements[0];
	update_columns = std::move(update.columns);
	expressions = std::move(update.expressions);
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
	return std::move(values_list.values);
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
	return std::move(info.columns);
}

} // namespace duckdb
