#include "duckdb/function/create_secret_function.hpp"
#include "duckdb/common/string_util.hpp"

namespace duckdb {

CreateSecretFunction::CreateSecretFunction(const string& type, const string& mode, create_secret_function_t function)
    : SimpleNamedParameterFunction(type, {}), function(function) {
	extra_info = mode;
}

string CreateSecretFunction::ToString() const {
	return StringUtil::Format("CREATE SECRET %s", SimpleNamedParameterFunction::ToString());
}

} // namespace duckdb
