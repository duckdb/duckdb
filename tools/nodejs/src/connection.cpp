#include "duckdb_node.hpp"

namespace node_duckdb {

Napi::FunctionReference Connection::constructor;

Napi::Object Connection::Init(Napi::Env env, Napi::Object exports) {
    Napi::HandleScope scope(env);

    Napi::Function t = DefineClass(
        env, "Connection", {InstanceMethod("run", &Connection::Run), InstanceMethod("exec", &Connection::Exec)});

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

struct RunTask : public Task {
	RunTask(Connection &connection_, std::string sql_, Napi::Function callback_)
	    : Task(callback_), connection(connection_), sql(sql_) {
		connection.Ref();
	}

	void DoWork() override {
		result = connection.connection->Query(sql);
	}

	void Callback() override {
		Napi::Env env = connection.Env();

		std::vector<napi_value> args;
		if (!result->success) {
			args.push_back(Utils::CreateError(env, result->error));
		} else {
			args.push_back(env.Null());
		}

		Napi::HandleScope scope(env);
		callback.Value().MakeCallback(connection.Value(), args);
	}

	~RunTask() {
		connection.Unref();
	}

	unique_ptr<duckdb::QueryResult> result;
	Connection &connection;
	std::string sql;
};

Napi::Value Connection::Run(const Napi::CallbackInfo &info) {
	// TODO check those params, could also have params here
	std::string sql = info[0].As<Napi::String>();
	auto callback = info[1].As<Napi::Function>();
	database_ref->Schedule(info.Env(), duckdb::make_unique<RunTask>(*this, sql, callback));
	return info.This();
}

Napi::Value Connection::Exec(const Napi::CallbackInfo &info) {
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
	return Statement::constructor.New(args);
}

}
