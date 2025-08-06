#include "duckdb/parser/parser_override.hpp"


namespace duckdb {

void ParserOverride::LogQuery(const string &message) {
	auto &logger = context.GetLogger();
	logger.WriteLog("PARSER", LogLevel::LOG_INFO, message.c_str());
	logger.Flush();
}

void ParserOverride::LogError(const string &message, const std::exception &e) {
	auto &logger = context.GetLogger();
	string error_message = message + " [Exception]: " + e.what();
	logger.WriteLog("PARSER", LogLevel::LOG_ERROR, error_message.c_str());
	logger.Flush();
}

}