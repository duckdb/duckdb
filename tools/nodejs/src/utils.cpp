#include "duckdb_node.hpp"

namespace node_duckdb {

bool Utils::OtherIsInt(Napi::Number source) {
	double orig_val = source.DoubleValue();
	double int_val = (double)source.Int32Value();
	if (orig_val == int_val) {
		return true;
	} else {
		return false;
	}
}

Napi::Value Utils::CreateError(Napi::Env env, std::string msg) {
	auto err = Napi::Error::New(env, Napi::String::New(env, msg).Utf8Value()).Value();
	Napi::Object obj = err.As<Napi::Object>();
	obj.Set(Napi::String::New(env, "errno"), Napi::Number::New(env, Database::DUCKDB_NODEJS_ERROR));
	obj.Set(Napi::String::New(env, "code"), Napi::String::New(env, "DUCKDB_NODEJS_ERROR"));

	return obj;
}

} // namespace node_duckdb
