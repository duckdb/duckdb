#include "duckdb/parser/parser_override.hpp"


namespace duckdb {

void ParserOverride::LogQuery(const string &query, const std::exception &e) {
	auto &logger = context.GetLogger();
	logger.WriteLog("PARSER", LogLevel::LOG_ERROR, query.c_str());
	logger.Flush();
}

}