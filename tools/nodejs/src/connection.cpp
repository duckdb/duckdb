#include "duckdb.hpp"
#include "duckdb_node.hpp"
#include "napi.h"

#include <iostream>
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
	ConnectTask(Connection &connection, Napi::Function callback) : Task(connection, callback) {
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

void DuckDBNodeUDFLauncher(Napi::Env env, Napi::Function jsudf, std::nullptr_t *, JSArgs *jsargs) {
	try { // if we dont catch exceptions here we terminate node if one happens ^^
		Napi::EscapableHandleScope scope(env);

		// Set up descriptor and data arrays
		auto descr = Napi::Object::New(env);
		auto chunk = EncodeDataChunk(env, *jsargs->args, true, true);
		descr.Set("args", scope.Escape(chunk));
		descr.Set("rows", jsargs->rows);
		auto ret = Napi::Object::New(env);
		ret.Set("sqlType", jsargs->result->GetType().ToString());
		auto ret_type = jsargs->result->GetType().InternalType();
#if NAPI_VERSION <= 5
		if (ret_type == duckdb::PhysicalType::INT64 || ret_type == duckdb::PhysicalType::UINT64) {
			ret_type = duckdb::PhysicalType::DOUBLE;
		}
#endif
		ret.Set("physicalType", TypeIdToString(ret_type));
		descr.Set("ret", ret);

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
		auto return_validity = ret.ToObject().Get("validity").As<Napi::Uint8Array>();
		for (duckdb::idx_t row_idx = 0; row_idx < jsargs->rows; row_idx++) {
			duckdb::FlatVector::SetNull(*jsargs->result, row_idx, !return_validity[row_idx]);
		}

		switch (jsargs->result->GetType().id()) {
		case duckdb::LogicalTypeId::TINYINT: {
			auto data = ret.Get("data").As<Napi::Int8Array>();
			auto out = duckdb::FlatVector::GetData<int8_t>(*jsargs->result);
			memcpy(out, data.Data(), jsargs->rows * duckdb::GetTypeIdSize(ret_type));
			break;
		}
		case duckdb::LogicalTypeId::SMALLINT: {
			auto data = ret.Get("data").As<Napi::Int16Array>();
			auto out = duckdb::FlatVector::GetData<int16_t>(*jsargs->result);
			memcpy(out, data.Data(), jsargs->rows * duckdb::GetTypeIdSize(ret_type));
			break;
		}
		case duckdb::LogicalTypeId::INTEGER: {
			auto data = ret.Get("data").As<Napi::Int32Array>();
			auto out = duckdb::FlatVector::GetData<int32_t>(*jsargs->result);
			memcpy(out, data.Data(), jsargs->rows * duckdb::GetTypeIdSize(ret_type));
			break;
		}
		case duckdb::LogicalTypeId::DOUBLE: {
			auto data = ret.Get("data").As<Napi::Float64Array>();
			auto out = duckdb::FlatVector::GetData<double>(*jsargs->result);
			memcpy(out, data.Data(), jsargs->rows * duckdb::GetTypeIdSize(ret_type));
			break;
		}
		case duckdb::LogicalTypeId::TIME:
		case duckdb::LogicalTypeId::TIMESTAMP:
		case duckdb::LogicalTypeId::TIMESTAMP_MS:
		case duckdb::LogicalTypeId::TIMESTAMP_SEC:
		case duckdb::LogicalTypeId::TIMESTAMP_NS:
		case duckdb::LogicalTypeId::BIGINT: {
#if NAPI_VERSION > 5
			auto data = ret.Get("data").As<Napi::BigInt64Array>();
#else
			auto data = ret.Get("data").As<Napi::Float64Array>();
#endif
			auto out = duckdb::FlatVector::GetData<int64_t>(*jsargs->result);
			memcpy(out, data.Data(), jsargs->rows * duckdb::GetTypeIdSize(ret_type));
			break;
		}
		case duckdb::LogicalTypeId::UBIGINT: {
#if NAPI_VERSION > 5
			auto data = ret.Get("data").As<Napi::BigUint64Array>();
#else
			auto data = ret.Get("data").As<Napi::Float64Array>();
#endif
			auto out = duckdb::FlatVector::GetData<uint64_t>(*jsargs->result);
			memcpy(out, data.Data(), jsargs->rows * duckdb::GetTypeIdSize(ret_type));
			break;
		}
		case duckdb::LogicalTypeId::BLOB:
		case duckdb::LogicalTypeId::VARCHAR: {
			auto data = ret.Get("data").As<Napi::Array>();
			auto out = duckdb::FlatVector::GetData<duckdb::string_t>(*jsargs->result);
			for (size_t i = 0; i < data.Length(); ++i) {
				out[i] = duckdb::string_t(data.Get(i).ToString());
			}
			break;
		}
		default: {
			for (duckdb::idx_t row_idx = 0; row_idx < jsargs->rows; row_idx++) {
				duckdb::FlatVector::SetNull(*jsargs->result, row_idx, false);
			}
		}
		}
	} catch (const std::exception &e) {
		jsargs->error = e.what();
	}
	jsargs->done = true;
}

struct RegisterTask : public Task {
	RegisterTask(Connection &connection, std::string name, std::string return_type_name, Napi::Function callback)
	    : Task(connection, callback), name(std::move(name)), return_type_name(std::move(return_type_name)) {
	}

	void DoWork() override {
		auto &connection = Get<Connection>();
		auto &udf_ptr = connection.udfs[name];
		duckdb::scalar_function_t udf_function = [&udf_ptr](duckdb::DataChunk &args, duckdb::ExpressionState &state,
		                                                    duckdb::Vector &result) -> void {
			// here we can do only DuckDB stuff because we do not have a functioning env

			// Flatten all args to simplify udfs
			args.Flatten();

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

	auto udf = duckdb_node_udf_function_t::New(env, udf_callback, "duckdb_node_udf" + name, 0, 1, nullptr,
	                                           [](Napi::Env, void *, std::nullptr_t *ctx) {});

	// we have to unref the udf because otherwise there is a circular ref with the connection somehow(?)
	// this took far too long to figure out
	udf.Unref(env);
	udfs[name] = udf;

	database_ref->Schedule(info.Env(),
	                       duckdb::make_unique<RegisterTask>(*this, name, return_type_name, completion_callback));

	return Value();
}

struct UnregisterTask : public Task {
	UnregisterTask(Connection &connection, std::string name, Napi::Function callback)
	    : Task(connection, callback), name(std::move(name)) {
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
	ExecTask(Connection &connection, std::string sql, Napi::Function callback)
	    : Task(connection, callback), sql(std::move(sql)) {
	}

	void DoWork() override {
		auto &connection = Get<Connection>();

		success = true;
		try {
			auto statements = connection.connection->ExtractStatements(sql);
			if (statements.empty()) {
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
		} catch (duckdb::ParserException &e) {
			success = false;
			error = e.what();
			return;
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
