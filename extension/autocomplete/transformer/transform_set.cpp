#include "transformer/peg_transformer.hpp"

namespace duckdb {


// ResetStatement <- 'RESET' (SetVariable / SetSetting)
unique_ptr<SQLStatement> PEGTransformerFactory::TransformResetStatement(PEGTransformer &transformer, optional_ptr<ParseResult> parse_result) {
    auto &list_pr = parse_result->Cast<ListParseResult>();
    throw NotImplementedException("Rule 'ResetStatement' has not been implemented yet");
}

// SetAssignment <- VariableAssign VariableList
unique_ptr<SQLStatement> PEGTransformerFactory::TransformSetAssignment(PEGTransformer &transformer, optional_ptr<ParseResult> parse_result) {
    auto &list_pr = parse_result->Cast<ListParseResult>();
    throw NotImplementedException("Rule 'SetAssignment' has not been implemented yet");
}

// SetSetting <- SettingScope? SettingName
unique_ptr<SQLStatement> PEGTransformerFactory::TransformSetSetting(PEGTransformer &transformer, optional_ptr<ParseResult> parse_result) {
    auto &list_pr = parse_result->Cast<ListParseResult>();
    throw NotImplementedException("Rule 'SetSetting' has not been implemented yet");
}

// SetStatement <- 'SET' (StandardAssignment / SetTimeZone)
unique_ptr<SQLStatement> PEGTransformerFactory::TransformSetStatement(PEGTransformer &transformer, optional_ptr<ParseResult> parse_result) {
    auto &list_pr = parse_result->Cast<ListParseResult>();
    throw NotImplementedException("Rule 'SetStatement' has not been implemented yet");
}

// SetTimeZone <- 'TIME' 'ZONE' Expression
unique_ptr<SQLStatement> PEGTransformerFactory::TransformSetTimeZone(PEGTransformer &transformer, optional_ptr<ParseResult> parse_result) {
    auto &list_pr = parse_result->Cast<ListParseResult>();
    throw NotImplementedException("Rule 'SetTimeZone' has not been implemented yet");
}

// SetVariable <- 'VARIABLE' Identifier
unique_ptr<SQLStatement> PEGTransformerFactory::TransformSetVariable(PEGTransformer &transformer, optional_ptr<ParseResult> parse_result) {
    auto &list_pr = parse_result->Cast<ListParseResult>();
    throw NotImplementedException("Rule 'SetVariable' has not been implemented yet");
}

// SettingScope <- 'LOCAL' / 'SESSION' / 'GLOBAL'
unique_ptr<SQLStatement> PEGTransformerFactory::TransformSettingScope(PEGTransformer &transformer, optional_ptr<ParseResult> parse_result) {
    auto &list_pr = parse_result->Cast<ListParseResult>();
    throw NotImplementedException("Rule 'SettingScope' has not been implemented yet");
}

// StandardAssignment <- (SetVariable / SetSetting) SetAssignment
unique_ptr<SQLStatement> PEGTransformerFactory::TransformStandardAssignment(PEGTransformer &transformer, optional_ptr<ParseResult> parse_result) {
    auto &list_pr = parse_result->Cast<ListParseResult>();
    throw NotImplementedException("Rule 'StandardAssignment' has not been implemented yet");
}

// VariableAssign <- '=' / 'TO'
unique_ptr<SQLStatement> PEGTransformerFactory::TransformVariableAssign(PEGTransformer &transformer, optional_ptr<ParseResult> parse_result) {
    auto &list_pr = parse_result->Cast<ListParseResult>();
    throw NotImplementedException("Rule 'VariableAssign' has not been implemented yet");
}

// VariableList <- List(Expression)
unique_ptr<SQLStatement> PEGTransformerFactory::TransformVariableList(PEGTransformer &transformer, optional_ptr<ParseResult> parse_result) {
    auto &list_pr = parse_result->Cast<ListParseResult>();
    throw NotImplementedException("Rule 'VariableList' has not been implemented yet");
}
} // namespace duckdb
