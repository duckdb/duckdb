#include "duckdb_node.hpp"

namespace node_duckdb {

Napi::FunctionReference Connection::constructor;

Napi::Object Connection::Init(Napi::Env env, Napi::Object exports) {
	Napi::HandleScope scope(env);

	Napi::Function t =
	    DefineClass(env, "Connection",
	                {InstanceMethod("prepare", &Connection::Prepare), InstanceMethod("exec", &Connection::Exec), InstanceMethod("register", &Connection::Register)});

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

	Napi::Function callback;
	if (info.Length() > 0 && info[1].IsFunction()) {
		callback = info[1].As<Napi::Function>();
	}

	database_ref->Schedule(env, duckdb::make_unique<ConnectTask>(*this, callback));
}

Connection::~Connection() {
	database_ref->Unref();
	database_ref = nullptr;
}

Napi::Value Connection::Prepare(const Napi::CallbackInfo &info) {
	std::vector<napi_value> args;
	// push the connection as first argument
	args.push_back(Value());
	// we need to pass all the arguments onward to statement
	for (size_t i = 0; i < info.Length(); i++) {
		args.push_back(info[i]);
	}
	auto res = Utils::NewUnwrap<Statement>(args);
	res->SetProcessFirstParam();
	return res->Value();
}

Napi::Value Connection::Register(const Napi::CallbackInfo &info) {
	if (info.Length() != 2 || !info[0].IsString() || !info[1].IsFunction()) {
		Napi::TypeError::New(info.Env(), "Holding it wrong").ThrowAsJavaScriptException();
		return Value();
	}

	std::string name = info[0].As<Napi::String>();
	Napi::Function udf = info[1].As<Napi::Function>();
	Napi::FunctionReference udf_ref = Persistent(udf);

	//cb.MakeCallback(statement.Value(), {Utils::CreateError(env, statement.statement->error)});

//	this->connection->CreateScalarFunction();

	// create new udf that calls the udf
	//Napi::FunctionReference callback;


	return Value();
}

struct ExecTask : public Task {
	ExecTask(Connection &connection_, std::string sql_, Napi::Function callback_)
	    : Task(connection_, callback_), sql(sql_) {
	}

	void DoWork() override {
		auto &connection = Get<Connection>();

		success = true;
		auto statements = connection.connection->ExtractStatements(sql);
		if (statements.size() == 0) {
			return;
		}

		// thanks Mark
		for (duckdb::idx_t i = 0; i < statements.size(); i++) {
			auto res = connection.connection->Query(move(statements[i]));
			if (!res->success) {
				success = false;
				error = res->error;
				break;
			}
		}
	}
	std::string sql;
	bool success;
	std::string error;
};

Napi::Value Connection::Exec(const Napi::CallbackInfo &info) {
	auto env = info.Env();

	if (info.Length() < 1 || !info[0].IsString()) {
		Napi::TypeError::New(env, "SQL query expected").ThrowAsJavaScriptException();
		return env.Null();
	}

	std::string sql = info[0].As<Napi::String>();

	Napi::Function callback;
	if (info.Length() > 0 && info[1].IsFunction()) {
		callback = info[1].As<Napi::Function>();
	}

	database_ref->Schedule(info.Env(), duckdb::make_unique<ExecTask>(*this, sql, callback));
	return Value();
}

} // namespace node_duckdb
