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

static void SetString(Napi::Object &obj, const std::string &key, const std::string &value) {
	obj.Set(Napi::String::New(obj.Env(), key), Napi::String::New(obj.Env(), value));
}

Napi::Object Utils::CreateError(Napi::Env env, duckdb::PreservedError &error) {
	auto obj = Utils::CreateError(env, error.Message());
	if (error.Type() == duckdb::ExceptionType::HTTP) {
		const auto &e = error.GetError()->AsHTTPException();
		obj.Set(Napi::String::New(env, "statusCode"), Napi::Number::New(env, e.GetStatusCode()));
		SetString(obj, "response", e.GetResponseBody());
		SetString(obj, "reason", e.GetReason());

		auto headers = Napi::Object::New(env);
		for (const auto &item : e.GetHeaders()) {
			SetString(headers, item.first, item.second);
		}
		obj.Set(Napi::String::New(env, "headers"), headers);
	}

	SetString(obj, "errorType", duckdb::Exception::ExceptionTypeToString(error.Type()));

	return obj;
}

Napi::Object Utils::CreateError(Napi::Env env, std::string msg) {
	auto err = Napi::Error::New(env, Napi::String::New(env, msg).Utf8Value()).Value();
	Napi::Object obj = err.As<Napi::Object>();
	obj.Set(Napi::String::New(env, "errno"), Napi::Number::New(env, Database::DUCKDB_NODEJS_ERROR));
	SetString(obj, "code", "DUCKDB_NODEJS_ERROR");
	SetString(obj, "errorType", "Invalid");

	return obj;
}
// A Napi InstanceOf for Javascript Objects "Date" and "RegExp"
static bool OtherInstanceOf(Napi::Object source, const char *object_type) {
	if (strcmp(object_type, "Date") == 0) {
		return source.InstanceOf(source.Env().Global().Get(object_type).As<Napi::Function>());
	} else if (strcmp(object_type, "RegExp") == 0) {
		return source.InstanceOf(source.Env().Global().Get(object_type).As<Napi::Function>());
	}

	return false;
}
duckdb::Value Utils::BindParameter(const Napi::Value source) {
	if (source.IsString()) {
		return duckdb::Value(source.As<Napi::String>().Utf8Value());
	} else if (OtherInstanceOf(source.As<Napi::Object>(), "RegExp")) {
		return duckdb::Value(source.ToString().Utf8Value());
	} else if (source.IsNumber()) {
		if (Utils::OtherIsInt(source.As<Napi::Number>())) {
			return duckdb::Value::INTEGER(source.As<Napi::Number>().Int32Value());
		} else {
			return duckdb::Value::DOUBLE(source.As<Napi::Number>().DoubleValue());
		}
	} else if (source.IsBoolean()) {
		return duckdb::Value::BOOLEAN(source.As<Napi::Boolean>().Value());
	} else if (source.IsNull()) {
		return duckdb::Value();
	} else if (source.IsBuffer()) {
		Napi::Buffer<char> buffer = source.As<Napi::Buffer<char>>();
		return duckdb::Value::BLOB(std::string(buffer.Data(), buffer.Length()));
#if (NAPI_VERSION > 4)
	} else if (source.IsDate()) {
		const auto micros = int64_t(source.As<Napi::Date>().ValueOf()) * duckdb::Interval::MICROS_PER_MSEC;
		if (micros % duckdb::Interval::MICROS_PER_DAY) {
			return duckdb::Value::TIMESTAMP(duckdb::timestamp_t(micros));
		} else {
			const auto days = int32_t(micros / duckdb::Interval::MICROS_PER_DAY);
			return duckdb::Value::DATE(duckdb::date_t(days));
		}
#endif
	} else if (source.IsObject()) {
		return duckdb::Value(source.ToString().Utf8Value());
	}
	return duckdb::Value();
}
} // namespace node_duckdb
