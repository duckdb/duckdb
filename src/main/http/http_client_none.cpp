#include "duckdb/main/http/http_util.hpp"

namespace duckdb {

string HTTPUtil::GetName() const {
	return "none";
}

unique_ptr<HTTPClient> HTTPUtil::InitializeClient(HTTPParams &http_params, const string &proto_host_port) {
	return nullptr;
}

} // namespace duckdb
