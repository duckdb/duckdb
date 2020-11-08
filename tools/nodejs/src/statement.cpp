#include "duckdb_node.hpp"

namespace node_duckdb {

Napi::FunctionReference Statement::constructor;

Napi::Object Statement::Init(Napi::Env env, Napi::Object exports) {
	Napi::HandleScope scope(env);

	Napi::Function t = DefineClass(env, "Result",
	                               {InstanceMethod("run", &Statement::Run), InstanceMethod("all", &Statement::All),
	                                InstanceMethod("each", &Statement::Each),

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
	// FIXME communicate back
	if (!statement->success) {
		printf("XXX: %s\n", statement->error.c_str());
	}
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
	    : Task(statement_, callback_), params(params_) {
	}

	void DoWork() override {
		auto &statement = Get<Statement>();
		statement.result = statement.statement->Execute(params, true);
	}

	void Callback() override {
		auto &statement = Get<Statement>();
		Napi::Env env = statement.Env();

		std::vector<napi_value> args;
		if (!statement.result->success) {
			args.push_back(Utils::CreateError(env, statement.result->error));
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
	    : Task(statement_, callback_), params(params_) {
	}

	void DoWork() override {
		auto &statement = Get<Statement>();
		statement.result = statement.statement->Execute(params, true);
	}

	void Callback() override {
		auto &statement = Get<Statement>();

		Napi::Env env = statement.Env();

		// somehow the function can disappear mid-flight (?)
		Napi::HandleScope scope(env);

		if (!callback.Value().IsFunction()) {
			return;
		}

		if (!statement.result->success) {
			callback.Value().MakeCallback(statement.Value(), {Utils::CreateError(env, statement.result->error)});
			return;
		}

		// regrettably this needs to know the result size
		// ensure result is materialized
		if (statement.result->type == duckdb::QueryResultType::STREAM_RESULT) {
			auto streaming_result = (duckdb::StreamQueryResult *)statement.result.get();
			auto materialized_result = streaming_result->Materialize();
			statement.result = move(materialized_result);
		}
		auto materialized_result = (duckdb::MaterializedQueryResult *)statement.result.get();

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

	std::vector<duckdb::Value> params;
};

struct StatementParam {
	std::vector<duckdb::Value> params;
	Napi::Function callback;
};

void Statement::HandleArgs(const Napi::CallbackInfo &info, StatementParam &params_out) {
	size_t start_idx = ignore_first_param ? 1 : 0;

	for (auto i = start_idx; i < info.Length(); i++) {
		auto &p = info[i];
		if (p.IsFunction()) {
			params_out.callback = p.As<Napi::Function>();
			continue;
		}
		if (p.IsUndefined()) {
			continue;
		}
		params_out.params.push_back(bind_parameter(p));
	}
}

// TODO loads of overlap, abstract this
// TODO better param parsing?
Napi::Value Statement::All(const Napi::CallbackInfo &info) {
	StatementParam params;
	HandleArgs(info, params);
	connection_ref->database_ref->Schedule(info.Env(),
	                                       duckdb::make_unique<RunAllTask>(*this, params.params, params.callback));
	return info.This();
}

Napi::Value Statement::Run(const Napi::CallbackInfo &info) {
	StatementParam params;
	HandleArgs(info, params);
	connection_ref->database_ref->Schedule(info.Env(),
	                                       duckdb::make_unique<RunPreparedTask>(*this, params.params, params.callback));
	return info.This();
}

struct RunEachTask : public Task {
	RunEachTask(Statement &statement_, std::vector<duckdb::Value> params_, Napi::Function callback_)
	    : Task(statement_, callback_), params(params_) {
	}

	void DoWork() override {
		auto &statement = Get<Statement>();
		statement.result = statement.statement->Execute(params, true);
	}

	void Callback() override {
		auto &statement = Get<Statement>();

		Napi::Env env = statement.Env();

		// somehow the function can disappear mid-flight (?)
		Napi::HandleScope scope(env);

		if (!callback.Value().IsFunction()) {
			return;
		}

		if (!statement.result->success) {
			callback.Value().MakeCallback(statement.Value(), {Utils::CreateError(env, statement.result->error)});
			return;
		}

		duckdb::idx_t out_idx = 0;
		while (true) {
			auto chunk = statement.result->Fetch();
			if (chunk->size() == 0) {
				break;
			}
			auto chunk_converted = convert_chunk(env, statement.result->names, *chunk);
			if (!chunk_converted.IsArray()) {
				// error was set before
				return;
			}
			for (duckdb::idx_t row_idx = 0; row_idx < chunk->size(); row_idx++) {
				// fire ze missiles
				callback.Value().MakeCallback(statement.Value(), {env.Null(), chunk_converted.ToObject().Get(row_idx)});
			}
		}
	}

	std::vector<duckdb::Value> params;
};

Napi::Value Statement::Each(const Napi::CallbackInfo &info) {
	StatementParam params;
	HandleArgs(info, params);
	connection_ref->database_ref->Schedule(info.Env(),
	                                       duckdb::make_unique<RunEachTask>(*this, params.params, params.callback));
	return info.This();
}

struct FinalizeTask : public Task {
	FinalizeTask(Statement &statement_, Napi::Function callback_) : Task(statement_, callback_) {
	}

	void DoWork() override {
		// nothing
	}

	void Callback() override {
		auto &statement = Get<Statement>();

		// somehow the function can disappear mid-flight (?)
		Napi::HandleScope scope(statement.Env());
		if (!callback.Value().IsFunction()) {
			return;
		}
		// TODO if no results, call with empty result arr
		callback.Value().MakeCallback(statement.Value(), {statement.Env().Null()});
	}
};

Napi::Value Statement::Finalize_(const Napi::CallbackInfo &info) {
	// TODO check args
	Napi::HandleScope scope(Env());

	auto callback = info[0].As<Napi::Function>();
	connection_ref->database_ref->Schedule(info.Env(), duckdb::make_unique<FinalizeTask>(*this, callback));
	return Env().Null();
}

} // namespace node_duckdb
