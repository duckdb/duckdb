#include "duckdb/common/exception/binder_exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/function/function.hpp"

namespace duckdb {

BinderException::BinderException(const string &msg) : Exception(ExceptionType::BINDER, msg) {
}

BinderException::BinderException(const string &msg, const unordered_map<string, string> &extra_info)
    : Exception(ExceptionType::BINDER, msg, extra_info) {
}

BinderException BinderException::ColumnNotFound(const string &name, const vector<string> &similar_bindings,
                                                QueryErrorContext context) {
	auto extra_info = Exception::InitializeExtraInfo("COLUMN_NOT_FOUND", context.query_location);
	string candidate_str = StringUtil::CandidatesMessage(similar_bindings, "Candidate bindings");
	extra_info["name"] = name;
	if (!similar_bindings.empty()) {
		extra_info["candidates"] = StringUtil::Join(similar_bindings, ",");
	}
	return BinderException(
	    StringUtil::Format("Referenced column \"%s\" not found in FROM clause!%s", name, candidate_str), extra_info);
}

BinderException BinderException::NoMatchingFunction(const string &name, const vector<LogicalType> &arguments,
                                                    const vector<string> &candidates) {
	auto extra_info = Exception::InitializeExtraInfo("NO_MATCHING_FUNCTION", optional_idx());
	// no matching function was found, throw an error
	string call_str = Function::CallToString(name, arguments);
	string candidate_str;
	for (auto &candidate : candidates) {
		candidate_str += "\t" + candidate + "\n";
	}
	extra_info["name"] = name;
	extra_info["call"] = call_str;
	if (!candidates.empty()) {
		extra_info["candidates"] = StringUtil::Join(candidates, ",");
	}
	return BinderException(
	    StringUtil::Format("No function matches the given name and argument types '%s'. You might need to add "
	                       "explicit type casts.\n\tCandidate functions:\n%s",
	                       call_str, candidate_str),
	    extra_info);
}

BinderException BinderException::Unsupported(ParsedExpression &expr, const string &message) {
	auto extra_info = Exception::InitializeExtraInfo("UNSUPPORTED", expr.GetQueryLocation());
	return BinderException(message, extra_info);
}

} // namespace duckdb
