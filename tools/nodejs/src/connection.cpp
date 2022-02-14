#include "duckdb_node.hpp"
#include <thread>

namespace node_duckdb {

Napi::FunctionReference Connection::constructor;

Napi::Object Connection::Init(Napi::Env env, Napi::Object exports) {
	Napi::HandleScope scope(env);

	Napi::Function t =
	    DefineClass(env, "Connection",
	                {InstanceMethod("prepare", &Connection::Prepare), InstanceMethod("exec", &Connection::Exec), InstanceMethod("register", &Connection::Register), InstanceMethod("unregister", &Connection::Unregister)});

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


struct JSArgs {
	duckdb::idx_t rows;
	duckdb::Vector* first_arg_vec;
	duckdb::Vector* result;
	bool done;
};

void DuckDBNodeUDFLauncher(Napi::Env env, Napi::Function jsudf, nullptr_t *, JSArgs *data) {
	auto first_arg_buf = Napi::Buffer<uint8_t>::New(env, duckdb::FlatVector::GetData<uint8_t>(*data->first_arg_vec), data->rows * sizeof(int32_t));
	auto ret_buf = Napi::Buffer<uint8_t>::New(env, duckdb::FlatVector::GetData<uint8_t>(*data->result), data->rows* sizeof(int32_t));

	jsudf.Call({first_arg_buf, ret_buf});
	data->done = true;
}



struct RegisterTask : public Task {
	RegisterTask(Connection &connection_, std::string name_, Napi::Function callback_)
	    : Task(connection_, callback_), name(name_){

	}

	void DoWork() override {
		auto &connection = Get<Connection>();
		auto& udf_ptr = connection.udfs[name];
		duckdb::scalar_function_t udf_function = [&udf_ptr](duckdb::DataChunk &args, duckdb::ExpressionState &state, duckdb::Vector &result) -> void {
			// here we can do only DuckDB stuff because we do not have a functioning env
			auto& first_arg_vec = args.data[0];
			first_arg_vec.Normalify(args.size());

			JSArgs jsargs;
			jsargs.rows = args.size();
			jsargs.first_arg_vec = &first_arg_vec;
			jsargs.result = &result;
			jsargs.done = false;

			udf_ptr.BlockingCall(&jsargs);
			while (!jsargs.done) {
				std::this_thread::yield();
			}
		};

		duckdb::ScalarFunction function({duckdb::LogicalType::INTEGER}, duckdb::LogicalType::INTEGER, udf_function);
		duckdb::CreateScalarFunctionInfo info(function);
		info.name = name;

		auto &con = *connection.connection;
		con.BeginTransaction();
		auto &context = *con.context;
		auto &catalog = duckdb::Catalog::GetCatalog(context);
		catalog.CreateFunction(context, &info);
		con.Commit();

	}
	std::string name;
};



Napi::Value Connection::Register(const Napi::CallbackInfo &info) {
	auto env = info.Env();
	if (info.Length() != 2 || !info[0].IsString() || !info[1].IsFunction()) {
		Napi::TypeError::New(env, "Holding it wrong").ThrowAsJavaScriptException();
		return env.Null();
	}

	std::string name = info[0].As<Napi::String>();
	Napi::Function udf_callback = info[1].As<Napi::Function>();
	Napi::Function dummy;

	auto udf2 = DuckDBNodeUDFFUnction::New(
	    env,
	    udf_callback, // JavaScript function called asynchronously
	    "duckdb_node_udf" + name,        // Name
	    0,                      // Unlimited queue
	    1,                      // Only one thread will use this initially
	    nullptr,

	    [](Napi::Env, void *,
	       nullptr_t *ctx) { // Finalizer used to clean threads up
	    });

	udfs[name] = udf2;
	// TODO check if the entry is already there?

	database_ref->Schedule(info.Env(), duckdb::make_unique<RegisterTask>(*this, name, dummy));

	return Value();
}


struct UnregisterTask : public Task {
	UnregisterTask(Connection &connection_, std::string name_, Napi::Function callback_)
	    : Task(connection_, callback_), name(name_) {

	}

	void DoWork() override {
		auto &connection = Get<Connection>();
		// TODO check if the entry is there to begin with?
		connection.udfs[name].Release();
		connection.udfs.erase(name);
		auto &con = *connection.connection;
		con.BeginTransaction();
		auto &context = *con.context;
		auto &catalog = duckdb::Catalog::GetCatalog(context);
		duckdb::DropInfo info;
		info.type = duckdb::CatalogType::SCALAR_FUNCTION_ENTRY;
		info.name = name;
		catalog.DropEntry(context, &info);
	}
	std::string name;
};



Napi::Value Connection::Unregister(const Napi::CallbackInfo &info) {
	auto env = info.Env();
	if (info.Length() != 1 || !info[0].IsString()) {
		Napi::TypeError::New(env, "Holding it wrong").ThrowAsJavaScriptException();
		return env.Null();
	}
	std::string name = info[0].As<Napi::String>();
	Napi::Function dummy;


	database_ref->Schedule(info.Env(), duckdb::make_unique<UnregisterTask>(*this, name, dummy));


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
