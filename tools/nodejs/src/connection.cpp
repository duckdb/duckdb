#include "duckdb_node.hpp"

#include <thread>

namespace node_duckdb {

Napi::FunctionReference Connection::constructor;

Napi::Object Connection::Init(Napi::Env env, Napi::Object exports) {
	Napi::HandleScope scope(env);

	Napi::Function t =
	    DefineClass(env, "Connection",
	                {InstanceMethod("prepare", &Connection::Prepare), InstanceMethod("exec", &Connection::Exec),
	                 InstanceMethod("register_bulk", &Connection::Register),
	                 InstanceMethod("unregister", &Connection::Unregister)});

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
	std::string error;
};

static Napi::Value transform_vector(Napi::Env env, duckdb::Vector &vec, duckdb::idx_t rows, bool copy) {
	Napi::EscapableHandleScope scope(env);

	Napi::Array data_buffers(Napi::Array::New(env));

	auto data_buffer_index = -1;
	auto length_buffer_index = -1;
	auto validity_buffer_index = -1;

	auto validity_buffer = Napi::Buffer<uint8_t>::New(env, rows);
	auto validity = duckdb::FlatVector::Validity(vec);
	auto validity_ptr = validity_buffer.Data();
	for (duckdb::idx_t row_idx = 0; row_idx < rows; row_idx++) {
		validity_ptr[row_idx] = validity.RowIsValid(row_idx);
	}
	data_buffers.Set(data_buffers.Length(), validity_buffer);
	validity_buffer_index = data_buffers.Length() - 1;

	auto &vec_type = vec.GetType();

	switch (vec_type.id()) {
	case duckdb::LogicalTypeId::BOOLEAN:
	case duckdb::LogicalTypeId::UTINYINT:
	case duckdb::LogicalTypeId::TINYINT:
	case duckdb::LogicalTypeId::USMALLINT:
	case duckdb::LogicalTypeId::SMALLINT:
	case duckdb::LogicalTypeId::INTEGER:
	case duckdb::LogicalTypeId::UINTEGER:
	case duckdb::LogicalTypeId::FLOAT:
	case duckdb::LogicalTypeId::DOUBLE: {
		auto data_buffer = Napi::Buffer<uint8_t>::New(env, rows * duckdb::GetTypeIdSize(vec_type.InternalType()));
		// TODO this is technically not neccessary but it fixes a crash in Node 14. Some weird data ownership issue.
		memcpy(data_buffer.Data(), duckdb::FlatVector::GetData<uint8_t>(vec),
		       rows * duckdb::GetTypeIdSize(vec_type.InternalType()));

		data_buffers.Set(data_buffers.Length(), data_buffer);
		data_buffer_index = data_buffers.Length() - 1;
		break;
	}
	case duckdb::LogicalTypeId::VARCHAR: {
		Napi::Array string_buffers(Napi::Array::New(env, rows));
		if (copy) {
			auto string_vec_ptr = duckdb::FlatVector::GetData<duckdb::string_t>(vec);
			for (duckdb::idx_t row_idx = 0; row_idx < rows; row_idx++) {
				string_buffers.Set(row_idx, validity_ptr[row_idx]
				                                ? Napi::String::New(env, string_vec_ptr[row_idx].GetDataUnsafe(),
				                                                    string_vec_ptr[row_idx].GetSize())
				                                : Napi::Value());
			}
		}
		data_buffers.Set(data_buffers.Length(), string_buffers);
		data_buffer_index = data_buffers.Length() - 1;
		break;
	}
	default:
		throw duckdb::NotImplementedException(vec_type.ToString());
	}

	Napi::Object desc(Napi::Object::New(env));
	desc.Set("logical_type", vec_type.ToString());
	desc.Set("physical_type", TypeIdToString(vec_type.InternalType()));
	desc.Set("validity_buffer", validity_buffer_index);
	desc.Set("data_buffer", data_buffer_index);
	desc.Set("length_buffer", length_buffer_index);
	desc.Set("data_buffers", data_buffers);

	return scope.Escape(desc);
}

void DuckDBNodeUDFLauncher(Napi::Env env, Napi::Function jsudf, nullptr_t *, JSArgs *jsargs) {
	try { // if we dont catch exceptions here we terminate node if one happens ^^

		// set up descriptor and data arrays
		Napi::Array args_descr(Napi::Array::New(env, jsargs->args->ColumnCount()));
		for (duckdb::idx_t col_idx = 0; col_idx < jsargs->args->ColumnCount(); col_idx++) {
			auto &vec = jsargs->args->data[col_idx];
			auto arg_descr = transform_vector(env, vec, jsargs->rows, true);
			args_descr.Set(col_idx, arg_descr);
		}
		auto ret_descr = transform_vector(env, *jsargs->result, jsargs->rows, false);

		Napi::Object descr(Napi::Object::New(env));
		descr.Set("rows", jsargs->rows);
		descr.Set("args", args_descr);
		descr.Set("ret", ret_descr);

		// actually call the UDF, or rather its vectorized wrapper from duckdb.js/Connection.prototype.register wrapper
		jsudf({descr});

		if (env.IsExceptionPending()) {
			// bit of a dance to get a nice error message if possible
			auto exception = env.GetAndClearPendingException();
			std::string msg = exception.Message();
			if (msg.empty()) {
				auto exception_value = exception.Value();
				if (exception_value.IsObject() && exception_value.Has("message")) {
					msg = exception_value.Get("message").ToString().Utf8Value();
				}
			}
			throw duckdb::IOException("UDF Execution Error: " + msg);
		}

		// transform the result back to a vector
		auto return_validity = ret_descr.ToObject()
		                           .Get("data_buffers")
		                           .ToObject()
		                           .Get(ret_descr.ToObject().Get("validity_buffer"))
		                           .As<Napi::Buffer<uint8_t>>();
		for (duckdb::idx_t row_idx = 0; row_idx < jsargs->rows; row_idx++) {
			duckdb::FlatVector::SetNull(*jsargs->result, row_idx, !return_validity[row_idx]);
		}

		if (jsargs->result->GetType().id() == duckdb::LogicalTypeId::VARCHAR) {
			auto return_string_array = ret_descr.ToObject()
			                               .Get("data_buffers")
			                               .ToObject()
			                               .Get(ret_descr.ToObject().Get("data_buffer"))
			                               .ToObject();
			auto return_string_vec_ptr = duckdb::FlatVector::GetData<duckdb::string_t>(*jsargs->result);

			for (duckdb::idx_t row_idx = 0; row_idx < jsargs->rows; row_idx++) {
				if (!return_validity[row_idx]) {
					duckdb::FlatVector::SetNull(*jsargs->result, row_idx, true);
				} else {
					auto str = return_string_array.Get(row_idx).As<Napi::String>();
					return_string_vec_ptr[row_idx] = duckdb::StringVector::AddString(*jsargs->result, str);
				}
			}
		} else {
			auto return_data = ret_descr.ToObject()
			                       .Get("data_buffers")
			                       .ToObject()
			                       .Get(ret_descr.ToObject().Get("data_buffer"))
			                       .As<Napi::Buffer<uint8_t>>();
			memcpy(duckdb::FlatVector::GetData<uint8_t>(*jsargs->result), return_data.Data(),
			       jsargs->rows * duckdb::GetTypeIdSize(jsargs->result->GetType().InternalType()));
		}
	} catch (const std::exception &e) {
		jsargs->error = e.what();
	}
	jsargs->done = true;
}

struct RegisterTask : public Task {
	RegisterTask(Connection &connection_, std::string name_, std::string return_type_name_, Napi::Function callback_)
	    : Task(connection_, callback_), name(name_), return_type_name(return_type_name_) {
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
			if (!jsargs.error.empty()) {
				throw duckdb::IOException(jsargs.error);
			}
		};

		auto expr = duckdb::Parser::ParseExpressionList(duckdb::StringUtil::Format("asdf::%s", return_type_name));
		auto &cast = (duckdb::CastExpression &)*expr[0];
		auto return_type = cast.cast_type;

		connection.connection->CreateVectorizedFunction(name, std::vector<duckdb::LogicalType> {}, return_type,
		                                                udf_function, duckdb::LogicalType::ANY);
	}
	std::string name;
	std::string return_type_name;
};

Napi::Value Connection::Register(const Napi::CallbackInfo &info) {
	auto env = info.Env();
	if (info.Length() < 3 || !info[0].IsString() || !info[1].IsString() || !info[2].IsFunction()) {
		Napi::TypeError::New(env, "Holding it wrong").ThrowAsJavaScriptException();
		return env.Null();
	}

	std::string name = info[0].As<Napi::String>();
	std::string return_type_name = info[1].As<Napi::String>();
	Napi::Function udf_callback = info[2].As<Napi::Function>();
	Napi::Function completion_callback;
	if (info.Length() > 3 && info[3].IsFunction()) {
		completion_callback = info[3].As<Napi::Function>();
	}

	if (udfs.find(name) != udfs.end()) {
		Napi::TypeError::New(env, "UDF with this name already exists").ThrowAsJavaScriptException();
		return env.Null();
	}

	auto udf = DuckDBNodeUDFFUnction::New(env, udf_callback, "duckdb_node_udf" + name, 0, 1, nullptr,
	                                      [](Napi::Env, void *, nullptr_t *ctx) {});

	// we have to unref the udf because otherwise there is a circular ref with the connection somehow(?)
	// this took far too long to figure out
	udf.Unref(env);
	udfs[name] = udf;

	database_ref->Schedule(info.Env(),
	                       duckdb::make_unique<RegisterTask>(*this, name, return_type_name, completion_callback));

	return Value();
}

struct UnregisterTask : public Task {
	UnregisterTask(Connection &connection_, std::string name_, Napi::Function callback_)
	    : Task(connection_, callback_), name(name_) {
	}

	void DoWork() override {
		auto &connection = Get<Connection>();
		if (connection.udfs.find(name) == connection.udfs.end()) { // silently ignore
			return;
		}

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
		con.Commit();
	}
	std::string name;
};

Napi::Value Connection::Unregister(const Napi::CallbackInfo &info) {
	auto env = info.Env();
	if (info.Length() < 1 || !info[0].IsString()) {
		Napi::TypeError::New(env, "Holding it wrong").ThrowAsJavaScriptException();
		return env.Null();
	}
	std::string name = info[0].As<Napi::String>();

	Napi::Function callback;
	if (info.Length() > 1 && info[1].IsFunction()) {
		callback = info[1].As<Napi::Function>();
	}

	database_ref->Schedule(info.Env(), duckdb::make_unique<UnregisterTask>(*this, name, callback));
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

	void Callback() override {
		auto env = object.Env();
		Napi::HandleScope scope(env);
		callback.Value().MakeCallback(object.Value(), {success ? env.Null() : Napi::String::New(env, error)});
	};

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
