#include "duckdb_node.hpp"

namespace node_duckdb {

Napi::FunctionReference Statement::constructor;

Napi::Object Statement::Init(Napi::Env env, Napi::Object exports) {
	Napi::HandleScope scope(env);

	Napi::Function t =
	    DefineClass(env, "Result",
	                {InstanceMethod("run", &Statement::Run), InstanceMethod("all", &Statement::All),
	                 InstanceMethod("each", &Statement::Each), InstanceMethod("finalize", &Statement::Finalize_)});

	constructor = Napi::Persistent(t);
	constructor.SuppressDestruct();

	exports.Set("Statement", t);
	return exports;
}


struct PrepareTask : public Task {
    PrepareTask(Statement &statement_, Napi::Function callback_) : Task(statement_, callback_) {
    }

    void DoWork() override {
        auto &statement = Get<Statement>();
        statement.statement = statement.connection_ref->connection->Prepare(statement.sql);
    }

    void Callback() override {
        auto &statement = Get<Statement>();

        // somehow the function can disappear mid-flight (?)
        Napi::HandleScope scope(statement.Env());
        auto cb = callback.Value();
        if (!cb.IsFunction()) {
            return;
        }
		// TODO error callback here and invalidate statement if borked
        cb.MakeCallback(statement.Value(), {statement.Env().Null(), statement.Value()});
    }
};


Statement::Statement(const Napi::CallbackInfo &info) : Napi::ObjectWrap<Statement>(info) {

    Napi::Env env = info.Env();
    int length = info.Length();

    if (length <= 0 || !Connection::HasInstance(info[0])) {
        Napi::TypeError::New(env, "Connection object expected").ThrowAsJavaScriptException();
        return;
    }
    else if (length <= 1 || !info[1].IsString()) {
        Napi::TypeError::New(env, "SQL query expected").ThrowAsJavaScriptException();
        return;
    }

	connection_ref = Napi::ObjectWrap<Connection>::Unwrap(info[0].As<Napi::Object>());
	connection_ref->Ref();

	sql = info[1].As<Napi::String>();

    connection_ref->database_ref->Schedule(env, duckdb::make_unique<PrepareTask>(*this, env.Null().As<Napi::Function>()));
}

Statement::~Statement() {
	connection_ref->Unref();
	connection_ref = nullptr;
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

enum ResultHandlingType { RUN, EACH, ALL };


struct StatementParam {
    std::vector<duckdb::Value> params;
    Napi::Function callback;
};

struct RunPreparedTask : public Task {
	RunPreparedTask(Statement &statement_, unique_ptr<StatementParam> params_, ResultHandlingType result_handling_type_)
	    : Task(statement_, params_->callback), params(params_->params), result_handling_type(result_handling_type_) {
	}

	void DoWork() override {
		result = Get<Statement>().statement->Execute(params, result_handling_type != ResultHandlingType::ALL);
	}

	void Callback() override {
		auto &statement = Get<Statement>();
		Napi::Env env = statement.Env();
		Napi::HandleScope scope(env);

		auto cb = callback.Value();
		// if there is no callback we dont have to do anything
		if (!cb.IsFunction()) {
			result.reset();
			return;
		}
		if (!statement.statement->success) {
            cb.MakeCallback(statement.Value(), {Utils::CreateError(env, statement.statement->error)});
            return;
        }
		// if there was an error we need to say so
		if (!result->success) {
			cb.MakeCallback(statement.Value(), {Utils::CreateError(env, result->error)});
			return;
		}

		switch (result_handling_type) {
		case ResultHandlingType::RUN:
			cb.MakeCallback(statement.Value(), {env.Null()});
			break;
		case ResultHandlingType::EACH:
			while (true) {
				auto chunk = result->Fetch();
				if (chunk->size() == 0) {
					break;
				}
				auto chunk_converted = convert_chunk(env, result->names, *chunk);
				if (!chunk_converted.IsArray()) {
					// error was set before
					return;
				}
				for (duckdb::idx_t row_idx = 0; row_idx < chunk->size(); row_idx++) {
					callback.Value().MakeCallback(statement.Value(),
					                              {env.Null(), chunk_converted.ToObject().Get(row_idx)});
				}
			}

			break;
		case ResultHandlingType::ALL: {
			auto materialized_result = (duckdb::MaterializedQueryResult *)result.get();
			Napi::Array result_arr(Napi::Array::New(env, materialized_result->collection.count));

			duckdb::idx_t out_idx = 0;
			while (true) {
				auto chunk = result->Fetch();
				if (chunk->size() == 0) {
					break;
				}
				auto chunk_converted = convert_chunk(env, result->names, *chunk);
				if (!chunk_converted.IsArray()) {
					// error was set before
					return;
				}
				for (duckdb::idx_t row_idx = 0; row_idx < chunk->size(); row_idx++) {
                    result_arr.Set(out_idx++, chunk_converted.ToObject().Get(row_idx));
				}
			}

			cb.MakeCallback(statement.Value(), {env.Null(), result_arr});
		} break;
		}
	}
	std::vector<duckdb::Value> params;
	unique_ptr<duckdb::QueryResult> result;
	ResultHandlingType result_handling_type;
};


unique_ptr<StatementParam> Statement::HandleArgs(const Napi::CallbackInfo &info) {
	size_t start_idx = ignore_first_param ? 1 : 0;
    auto params = duckdb::make_unique<StatementParam>();

	for (auto i = start_idx; i < info.Length(); i++) {
		auto &p = info[i];
		if (p.IsFunction()) {
            params->callback = p.As<Napi::Function>();
			continue;
		}
		if (p.IsUndefined()) {
			continue;
		}
        params->params.push_back(bind_parameter(p));
	}
	return params;
}


Napi::Value Statement::All(const Napi::CallbackInfo &info) {
	connection_ref->database_ref->Schedule(
	    info.Env(),
	    duckdb::make_unique<RunPreparedTask>(*this, HandleArgs(info), ResultHandlingType::ALL));
	return info.This();
}

Napi::Value Statement::Run(const Napi::CallbackInfo &info) {
    auto params = HandleArgs(info);
	connection_ref->database_ref->Schedule(
	    info.Env(),
	    duckdb::make_unique<RunPreparedTask>(*this, HandleArgs(info), ResultHandlingType::RUN));
	return info.This();
}

Napi::Value Statement::Each(const Napi::CallbackInfo &info) {
    auto params = HandleArgs(info);
	connection_ref->database_ref->Schedule(
	    info.Env(),
	    duckdb::make_unique<RunPreparedTask>(*this, HandleArgs(info), ResultHandlingType::EACH));
	return info.This();
}

struct FinalizeTask : public Task {
	FinalizeTask(Statement &statement_, Napi::Function callback_) : Task(statement_, callback_) {
	}

	void DoWork() override {
		auto &statement = Get<Statement>();
		statement.statement.reset();
	}

	void Callback() override {
		auto &statement = Get<Statement>();

		// somehow the function can disappear mid-flight (?)
		Napi::HandleScope scope(statement.Env());
		auto cb = callback.Value();
		if (!cb.IsFunction()) {
			return;
		}
		cb.MakeCallback(statement.Value(), {statement.Env().Null()});
	}
};

Napi::Value Statement::Finalize_(const Napi::CallbackInfo &info) {
    Napi::Env env = info.Env();

    if (info.Length() <= 0 || !info[0].IsFunction()) {
        Napi::TypeError::New(env, "Callback expected").ThrowAsJavaScriptException();
        return env.Null();
    }

	Napi::HandleScope scope(env);
	auto callback = info[0].As<Napi::Function>();
	connection_ref->database_ref->Schedule(env, duckdb::make_unique<FinalizeTask>(*this, callback));
	return env.Null();
}

} // namespace node_duckdb
