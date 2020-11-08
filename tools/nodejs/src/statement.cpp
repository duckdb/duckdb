#include "duckdb_node.hpp"

namespace node_duckdb {

Napi::FunctionReference Statement::constructor;

Napi::Object Statement::Init(Napi::Env env, Napi::Object exports) {
	Napi::HandleScope scope(env);

	Napi::Function t = DefineClass(env, "Result",
	                               {InstanceMethod("run", &Statement::Run), InstanceMethod("all", &Statement::All),

	                                InstanceMethod("finalize", &Statement::Finalize_)});

	constructor = Napi::Persistent(t);
	constructor.SuppressDestruct();

	exports.Set("Statement", t);
	return exports;
}

Statement::Statement(const Napi::CallbackInfo &info) : Napi::ObjectWrap<Statement>(info) {
	if (info.Length() <= 0) {
		// TODO check that what is passed is a Database instance. compare constructors (gah)
		Napi::TypeError::New(info.Env(), "Connection object expected").ThrowAsJavaScriptException();
		return;
	}
	connection_ref = Napi::ObjectWrap<Connection>::Unwrap(info[0].As<Napi::Object>());
	connection_ref->Ref();

	// TODO check those params
	std::string sql = info[1].As<Napi::String>();
	statement = connection_ref->connection->Prepare(sql);
}

Statement::~Statement() {
	connection_ref->Unref();
}

// A Napi InstanceOf for Javascript Objects "Date" and "RegExp"
static bool other_instance_of(Napi::Object source, const char *object_type) {
	if (strncmp(object_type, "Date", 4) == 0) {
		return source.InstanceOf(source.Env().Global().Get("Date").As<Napi::Function>());
	} else if (strncmp(object_type, "RegExp", 6) == 0) {
		return source.InstanceOf(source.Env().Global().Get("RegExp").As<Napi::Function>());
	}

	return false;
}

static duckdb::Value bind_parameter(const Napi::Value source) {
	if (source.IsString()) {
		return duckdb::Value(source.As<Napi::String>().Utf8Value());
	} else if (other_instance_of(source.As<Napi::Object>(), "RegExp")) {
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
		return duckdb::Value::BLOB(string(buffer.Data(), buffer.Length()));
	} else if (other_instance_of(source.As<Napi::Object>(), "Date")) {
		// FIXME
		// return new Values::Float(pos, source.ToNumber().DoubleValue());
	} else if (source.IsObject()) {
		return duckdb::Value(source.ToString().Utf8Value());
	}
	return duckdb::Value();
}

struct RunPreparedTask : public Task {
	RunPreparedTask(Statement &statement_, std::vector<duckdb::Value> params_, Napi::Function callback_)
	    : Task(callback_), statement(statement_), params(params_) {
		statement.Ref();
	}

	void DoWork() override {
		result = statement.statement->Execute(params, true);
	}

	void Callback() override {
		Napi::Env env = statement.Env();

		std::vector<napi_value> args;
		if (!result->success) {
			args.push_back(Utils::CreateError(env, result->error));
		} else {
			args.push_back(env.Null());
		}
		// somehow the function can disappear mid-flight (?)
		if (!callback.Value().IsFunction()) {
			return;
		}
		Napi::HandleScope scope(env);
		callback.Value().MakeCallback(statement.Value(), args);
	}

	~RunPreparedTask() {
		statement.Unref();
	}

	unique_ptr<duckdb::QueryResult> result;
	Statement &statement;
	std::vector<duckdb::Value> params;
};

static Napi::Value convert_chunk(Napi::Env &env, vector<string> names, duckdb::DataChunk &chunk) {
	Napi::EscapableHandleScope scope(env);
	vector<Napi::String> node_names;
	assert(names.size() == chunk.column_count());
	for (auto &name : names) {
		node_names.push_back(Napi::String::New(env, name));
	}
	Napi::Array result(Napi::Array::New(env, chunk.size()));

	for (duckdb::idx_t row_idx = 0; row_idx < chunk.size(); row_idx++) {
		Napi::Object row_result = Napi::Object::New(env);

		for (duckdb::idx_t col_idx = 0; col_idx < chunk.column_count(); col_idx++) {
			Napi::Value value;

			auto dval = chunk.GetValue(col_idx, row_idx);
			if (dval.is_null) {
				row_result.Set(node_names[col_idx], env.Null());
				continue;
			}

			// TODO templateroo here
			switch (chunk.data[col_idx].type.id()) {
			case duckdb::LogicalTypeId::INTEGER: {
				value = Napi::Number::New(env, dval.value_.integer);
			} break;
			case duckdb::LogicalTypeId::FLOAT: {
				value = Napi::Number::New(env, dval.value_.float_);
			} break;
			case duckdb::LogicalTypeId::BIGINT: {
				value = Napi::Number::New(env, dval.value_.bigint);
			} break;
			case duckdb::LogicalTypeId::VARCHAR: {
				value = Napi::String::New(env, dval.str_value);
			} break;
			case duckdb::LogicalTypeId::BLOB: {
				value = Napi::Buffer<char>::Copy(env, dval.str_value.c_str(), dval.str_value.length());
			} break;
			case duckdb::LogicalTypeId::SQLNULL: {
				value = env.Null();
			} break;
			default:
				Napi::Error::New(env, "Data type is not supported " + dval.type().ToString())
				    .ThrowAsJavaScriptException();
				return env.Null();
			}
			row_result.Set(node_names[col_idx], value);
		}
		result.Set(row_idx, row_result);
	}

	return scope.Escape(result);
}

struct RunAllTask : public Task {
	RunAllTask(Statement &statement_, std::vector<duckdb::Value> params_, Napi::Function callback_)
	    : Task(callback_), statement(statement_), params(params_) {
		statement.Ref();
	}

	void DoWork() override {
		result = statement.statement->Execute(params, true);
	}

	void Callback() override {

		Napi::Env env = statement.Env();

		// somehow the function can disappear mid-flight (?)
		Napi::HandleScope scope(env);

		if (!callback.Value().IsFunction()) {
			return;
		}

		if (!result->success) {
			callback.Value().MakeCallback(statement.Value(), {Utils::CreateError(env, result->error)});
			return;
		}

		// regrettably this needs to know the result size
		// ensure result is materialized
		if (result->type == duckdb::QueryResultType::STREAM_RESULT) {
			auto streaming_result = (duckdb::StreamQueryResult *)result.get();
			auto materialized_result = streaming_result->Materialize();
			result = move(materialized_result);
		}
		auto materialized_result = (duckdb::MaterializedQueryResult *)result.get();

		Napi::Array result(Napi::Array::New(env, materialized_result->collection.count));

		duckdb::idx_t out_idx = 0;
		while (true) {
			auto chunk = materialized_result->Fetch();
			if (chunk->size() == 0) {
				break;
			}
			auto chunk_converted = convert_chunk(env, materialized_result->names, *chunk);
			if (!chunk_converted.IsArray()) {
				// error was set before
				return;
			}
			for (duckdb::idx_t row_idx = 0; row_idx < chunk->size(); row_idx++) {
				result.Set(out_idx++, chunk_converted.ToObject().Get(row_idx));
			}
		}

		// TODO if no results, call with empty result arr
		callback.Value().MakeCallback(statement.Value(), {env.Null(), result});
	}

	~RunAllTask() {
		statement.Unref();
	}

	unique_ptr<duckdb::QueryResult> result;
	Statement &statement;
	std::vector<duckdb::Value> params;
};

// TODO loads of overlap, abstract this
// TODO better param parsing?
Napi::Value Statement::All(const Napi::CallbackInfo &info) {
	Napi::Function callback;

	std::vector<duckdb::Value> params;
	for (size_t i = 1; i < info.Length(); i++) {
		auto &p = info[i];
		if (p.IsFunction()) {
			callback = p.As<Napi::Function>();
			break;
		}

		params.push_back(bind_parameter(p));
	}

	connection_ref->database_ref->Schedule(info.Env(), duckdb::make_unique<RunAllTask>(*this, params, callback));
	return info.This();
}

Napi::Value Statement::Run(const Napi::CallbackInfo &info) {
	Napi::Function callback;

	std::vector<duckdb::Value> params;
	for (size_t i = 0; i < info.Length(); i++) {
		auto &p = info[i];
		if (p.IsFunction()) {
			callback = p.As<Napi::Function>();
			break;
		}

		params.push_back(bind_parameter(p));
	}
	connection_ref->database_ref->Schedule(info.Env(), duckdb::make_unique<RunPreparedTask>(*this, params, callback));
	return info.This();
}

struct FinalizeTask : public Task {
	FinalizeTask(Statement &statement_, Napi::Function callback_) : Task(callback_), statement(statement_) {
		statement.Ref();
	}

	void DoWork() override {
		// nothing
	}

	void Callback() override {
		// somehow the function can disappear mid-flight (?)
		Napi::HandleScope scope(statement.Env());
		if (!callback.Value().IsFunction()) {
			return;
		}
		// TODO if no results, call with empty result arr
		callback.Value().MakeCallback(statement.Value(), {statement.Env().Null()});
	}

	~FinalizeTask() {
		statement.Unref();
	}

	unique_ptr<duckdb::QueryResult> result;
	Statement &statement;
	std::vector<duckdb::Value> params;
};

Napi::Value Statement::Finalize_(const Napi::CallbackInfo &info) {
	// statement.reset();

	// TODO check args
	// TODO do we need to wait here? yes!
	Napi::HandleScope scope(Env());

	auto callback = info[0].As<Napi::Function>();
	connection_ref->database_ref->Schedule(info.Env(), duckdb::make_unique<FinalizeTask>(*this, callback));
	return Env().Null();
}

} // namespace node_duckdb
