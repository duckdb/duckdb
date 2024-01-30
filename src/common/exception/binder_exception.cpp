#include "duckdb/common/exception/binder_exception.hpp"
#include "duckdb/common/string_util.hpp"

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
	    context.FormatError("Referenced column \"%s\" not found in FROM clause!%s", name, candidate_str), extra_info);
}

} // namespace duckdb
