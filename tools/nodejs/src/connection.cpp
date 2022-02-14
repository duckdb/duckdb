#include "duckdb_node.hpp"
#include <thread>

namespace node_duckdb {

Napi::FunctionReference Connection::constructor;

Napi::Object Connection::Init(Napi::Env env, Napi::Object exports) {
	Napi::HandleScope scope(env);

	Napi::Function t =
	    DefineClass(env, "Connection",
	                {InstanceMethod("prepare", &Connection::Prepare), InstanceMethod("exec", &Connection::Exec),
	                 InstanceMethod("register_bulk", &Connection::Register)});

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
	duckdb::DataChunk *args;
	duckdb::Vector *result;
	bool done;
};

typedef std::vector<duckdb::unique_ptr<duckdb::data_t[]>> additional_buffers_t;

static duckdb::data_ptr_t create_additional_buffer(std::vector<double> &data_ptrs,
                                                   additional_buffers_t &additional_buffers, idx_t size,
                                                   int64_t &buffer_idx) {
	additional_buffers.emplace_back(duckdb::unique_ptr<duckdb::data_t[]>(new duckdb::data_t[size]));
	auto res_ptr = additional_buffers.back().get();
	buffer_idx = data_ptrs.size() - 1;
	return res_ptr;
}

void DuckDBNodeUDFLauncher(Napi::Env env, Napi::Function jsudf, nullptr_t *, JSArgs *jsargs) {
	Napi::Array data_buffers(Napi::Array::New(env, 0));
	Napi::Array args_descr(Napi::Array::New(env, jsargs->args->ColumnCount()));

	additional_buffers_t additional_buffers;

	for (idx_t col_idx = 0; col_idx < jsargs->args->ColumnCount(); col_idx++) {
		auto &vec = jsargs->args->data[col_idx];

		data_buffers.Set(
		    data_buffers.Length(),
		    Napi::Buffer<uint8_t>::New(env, duckdb::FlatVector::GetData<uint8_t>(vec),
		                               jsargs->rows * duckdb::GetTypeIdSize(vec.GetType().InternalType())));
		auto data_buffer_index = data_buffers.Length() - 1;

		additional_buffers.emplace_back(duckdb::unique_ptr<duckdb::data_t[]>(new duckdb::data_t[jsargs->rows]));
		auto validity = duckdb::FlatVector::Validity(vec);
		for (idx_t row_idx = 0; row_idx < jsargs->rows; row_idx++) {
			additional_buffers.back().get()[row_idx] = validity.RowIsValid(row_idx);
		}
		data_buffers.Set(data_buffers.Length(),
		                 Napi::Buffer<uint8_t>::New(env, additional_buffers.back().get(), jsargs->rows));
		auto validity_buffer_index = data_buffers.Length() - 1;

		Napi::Object arg_descr(Napi::Object::New(env));
		arg_descr.Set("logical_type", vec.GetType().ToString());
		arg_descr.Set("physical_type", TypeIdToString(vec.GetType().InternalType()));
		arg_descr.Set("validity_buffer", validity_buffer_index);
		arg_descr.Set("data_buffer", data_buffer_index);
		arg_descr.Set("length_buffer", -1);

		args_descr.Set(col_idx, arg_descr);
	}

	auto ret_buf =
	    Napi::Buffer<uint8_t>::New(env, duckdb::FlatVector::GetData<uint8_t>(*jsargs->result),
	                               jsargs->rows * duckdb::GetTypeIdSize(jsargs->result->GetType().InternalType()));

	data_buffers.Set(data_buffers.Length(), ret_buf);
	auto return_data_buffer_index = data_buffers.Length() - 1;

	additional_buffers.emplace_back(duckdb::unique_ptr<duckdb::data_t[]>(new duckdb::data_t[jsargs->rows]));
	data_buffers.Set(data_buffers.Length(),
	                 Napi::Buffer<uint8_t>::New(env, additional_buffers.back().get(), jsargs->rows));
	auto return_validity_buffer_ptr = additional_buffers.back().get();

	auto return_validity_buffer_index = data_buffers.Length() - 1;

	Napi::Object descr(Napi::Object::New(env));
	descr.Set("rows", jsargs->rows);
	descr.Set("args", args_descr);

	Napi::Object ret_descr(Napi::Object::New(env));

	ret_descr.Set("logical_type", jsargs->result->GetType().ToString());
	ret_descr.Set("physical_type", TypeIdToString(jsargs->result->GetType().InternalType()));
	ret_descr.Set("data_buffer", return_data_buffer_index);
	ret_descr.Set("validity_buffer", return_validity_buffer_index);
	ret_descr.Set("length_buffer", -1);

	descr.Set("ret", ret_descr);

	jsudf.Call({descr, data_buffers});

	for (idx_t row_idx = 0; row_idx < jsargs->rows; row_idx++) {
		duckdb::FlatVector::SetNull(*jsargs->result, row_idx, !return_validity_buffer_ptr[row_idx]);
	}

	jsargs->done = true;
}

struct RegisterTask : public Task {
	RegisterTask(Connection &connection_, std::string name_, Napi::Function callback_)
	    : Task(connection_, callback_), name(name_) {
	}

	void DoWork() override {
		auto &connection = Get<Connection>();
		auto &udf_ptr = connection.udfs[name];
		duckdb::scalar_function_t udf_function = [&udf_ptr](duckdb::DataChunk &args, duckdb::ExpressionState &state,
		                                                    duckdb::Vector &result) -> void {
			// here we can do only DuckDB stuff because we do not have a functioning env

			// Flatten all args to simplify udfs
			args.Normalify();

			JSArgs jsargs;
			jsargs.rows = args.size();
			jsargs.args = &args;
			jsargs.result = &result;
			jsargs.done = false;

			udf_ptr.BlockingCall(&jsargs);
			while (!jsargs.done) {
				std::this_thread::yield();
			}
		};

		// TODO allow for different return types, need additional parameter
		connection.connection->CreateVectorizedFunction(name, std::vector<duckdb::LogicalType> {},
		                                                duckdb::LogicalType::INTEGER, udf_function,
		                                                duckdb::LogicalType::ANY);
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

	auto udf2 = DuckDBNodeUDFFUnction::New(env,
	                                       udf_callback,             // JavaScript function called asynchronously
	                                       "duckdb_node_udf" + name, // Name
	                                       0,                        // Unlimited queue
	                                       1,                        // Only one thread will use this initially
	                                       nullptr,

	                                       [](Napi::Env, void *,
	                                          nullptr_t *ctx) { // Finalizer used to clean threads up
	                                       });

	// we have to unref the udf because otherwise there is a circular ref with the connection somehow(?)
	udf2.Unref(env);
	udfs[name] = udf2;
	// TODO check if the entry is already there?

	database_ref->Schedule(info.Env(), duckdb::make_unique<RegisterTask>(*this, name, dummy));

	return Value();
}

/*
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
 */

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
