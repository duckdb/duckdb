#include "duckdb_node.hpp"

namespace node_duckdb {

Napi::FunctionReference Connection::constructor;

Napi::Object Connection::Init(Napi::Env env, Napi::Object exports) {
	Napi::HandleScope scope(env);

	Napi::Function t =
	    DefineClass(env, "Connection",
	                {InstanceMethod("run", &Connection::Run), InstanceMethod("each", &Connection::Each),
	                 InstanceMethod("all", &Connection::All), InstanceMethod("exec", &Connection::Exec)});

	constructor = Napi::Persistent(t);
	constructor.SuppressDestruct();

	exports.Set("Connection", t);
	return exports;
}

Connection::Connection(const Napi::CallbackInfo &info) : Napi::ObjectWrap<Connection>(info) {
	if (info.Length() <= 0) {
		// TODO check that what is passed is a Database instance. compare constructors (gah)
		Napi::TypeError::New(info.Env(), "Database object expected").ThrowAsJavaScriptException();
		return;
	}
	database_ref = Napi::ObjectWrap<Database>::Unwrap(info[0].As<Napi::Object>());
	database_ref->Ref();

	connection = duckdb::make_unique<duckdb::Connection>(*database_ref->database);
}

Connection::~Connection() {
	database_ref->Unref();
}

Napi::Value Connection::Run(const Napi::CallbackInfo &info) {
	Utils::NewUnwrap<Statement>({Value(), info[0].As<Napi::String>()})->Run(info);
	return info.This();
}

Napi::Value Connection::Exec(const Napi::CallbackInfo &info) {
	// FIXME Utils::NewUnwrap<Statement>( {Value(), info[0].As<Napi::String>()})->Exec(info);
	return info.This();
}

// TODO make this a template?
Napi::Value Connection::All(const Napi::CallbackInfo &info) {
	Utils::NewUnwrap<Statement>({Value(), info[0].As<Napi::String>()})->All(info);
	return info.This();
}

// TODO make this a template?
Napi::Value Connection::Each(const Napi::CallbackInfo &info) {
	Utils::NewUnwrap<Statement>({Value(), info[0].As<Napi::String>()})->Each(info);
	return info.This();
}

Napi::Value Connection::Prepare(const Napi::CallbackInfo &info) {
	// TODO check those params
	auto sql = info[0].As<Napi::String>();

	std::vector<napi_value> args;
	// push the connection as first argument
	args.push_back(Value());
	// push the query as second argument
	args.push_back(sql);

	// we need to pass all the arguments onward to statement
	for (size_t i = 0; i < info.Length(); i++) {
		args.push_back(info[i]);
	}
	auto res = Utils::NewUnwrap<Statement>(args);
	res->SetProcessFirstParam();
	return res->Value();
}

} // namespace node_duckdb
