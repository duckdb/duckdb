#include "duckdb_node.hpp"

namespace node_duckdb {

Napi::FunctionReference Connection::constructor;

Napi::Object Connection::Init(Napi::Env env, Napi::Object exports) {
	Napi::HandleScope scope(env);

	Napi::Function t = DefineClass(env, "Connection", {InstanceMethod("prepare", &Connection::Prepare)});

	constructor = Napi::Persistent(t);
	constructor.SuppressDestruct();

	exports.Set("Connection", t);
	return exports;
}


struct ConnectTask : public Task {
    ConnectTask(Connection &connection_, Napi::Function callback_) : Task(connection_, callback_) {
    }

    void DoWork() override {
        auto &connection = Get<Connection>();
        connection.connection = duckdb::make_unique<duckdb::Connection>(*connection.database_ref->database);
    }

    void Callback() override {
        auto &connection = Get<Connection>();

        // somehow the function can disappear mid-flight (?)
        Napi::HandleScope scope(connection.Env());
        auto cb = callback.Value();
        if (!cb.IsFunction()) {
            return;
        }
        cb.MakeCallback(connection.Value(), {connection.Env().Null(), connection.Value()});
    }
};


Connection::Connection(const Napi::CallbackInfo &info) : Napi::ObjectWrap<Connection>(info) {
    Napi::Env env = info.Env();
    int length = info.Length();

    if (length <= 0 || !Database::HasInstance(info[0])) {
        Napi::TypeError::New(env, "Database object expected").ThrowAsJavaScriptException();
        return;
    }

	database_ref = Napi::ObjectWrap<Database>::Unwrap(info[0].As<Napi::Object>());
	database_ref->Ref();

	database_ref->Schedule(env, duckdb::make_unique<ConnectTask>(*this, env.Null().As<Napi::Function>()));
}

Connection::~Connection() {
	database_ref->Unref();
    database_ref = nullptr;

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
