#include "duckdb_node.hpp"

namespace node_duckdb {

Napi::FunctionReference Statement::constructor;

Napi::Object Statement::Init(Napi::Env env, Napi::Object exports) {
	Napi::HandleScope scope(env);

	Napi::Function t =
	    DefineClass(env, "Statement",
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
		auto env = statement.Env();
		Napi::HandleScope scope(env);

		auto cb = callback.Value();
		if (!statement.statement->success) {
			cb.MakeCallback(statement.Value(), {Utils::CreateError(env, statement.statement->error)});
			return;
		}
		cb.MakeCallback(statement.Value(), {env.Null(), statement.Value()});
	}
};

Statement::Statement(const Napi::CallbackInfo &info) : Napi::ObjectWrap<Statement>(info) {

	Napi::Env env = info.Env();
	int length = info.Length();

	if (length <= 0 || !Connection::HasInstance(info[0])) {
		Napi::TypeError::New(env, "Connection object expected").ThrowAsJavaScriptException();
		return;
	} else if (length <= 1 || !info[1].IsString()) {
		Napi::TypeError::New(env, "SQL query expected").ThrowAsJavaScriptException();
		return;
	}

	connection_ref = Napi::ObjectWrap<Connection>::Unwrap(info[0].As<Napi::Object>());
	connection_ref->Ref();

	sql = info[1].As<Napi::String>();

	Napi::Function callback;
	if (info.Length() > 1 && info[2].IsFunction()) {
		callback = info[2].As<Napi::Function>();
	}

	// TODO we can have parameters here as well. Forward if that is the case.
	Value().As<Napi::Object>().DefineProperty(
	    Napi::PropertyDescriptor::Value("sql", info[1].As<Napi::String>(), napi_default));
	connection_ref->database_ref->Schedule(env, duckdb::make_unique<PrepareTask>(*this, callback));
}

Statement::~Statement() {
	connection_ref->Unref();
	connection_ref = nullptr;
}

// A Napi InstanceOf for Javascript Objects "Date" and "RegExp"
static bool other_instance_of(Napi::Object source, const char *object_type) {
	if (strcmp(object_type, "Date") == 0) {
		return source.InstanceOf(source.Env().Global().Get(object_type).As<Napi::Function>());
	} else if (strcmp(object_type, "RegExp") == 0) {
		return source.InstanceOf(source.Env().Global().Get(object_type).As<Napi::Function>());
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
		return duckdb::Value::BLOB(std::string(buffer.Data(), buffer.Length()));
#if (NAPI_VERSION > 4)
	} else if (source.IsDate()) {
		const auto micros = int64_t(source.As<Napi::Date>().ValueOf()) * duckdb::Interval::MICROS_PER_MSEC;
		if (micros % duckdb::Interval::MICROS_PER_DAY) {
			return duckdb::Value::TIMESTAMP(duckdb::timestamp_t(micros));
		} else {
			const auto days = int32_t(micros / duckdb::Interval::MICROS_PER_DAY);
			return duckdb::Value::DATE(duckdb::date_t(days));
		}
#endif
	} else if (source.IsObject()) {
		return duckdb::Value(source.ToString().Utf8Value());
	}
	return duckdb::Value();
}

static Napi::Value convert_col_val(Napi::Env &env, duckdb::Value dval, duckdb::LogicalTypeId id) {
	Napi::Value value;

	// TODO templateroo here
	switch (id) {
	case duckdb::LogicalTypeId::BOOLEAN: {
		value = Napi::Boolean::New(env, duckdb::BooleanValue::Get(dval));
	} break;
	case duckdb::LogicalTypeId::INTEGER: {
		value = Napi::Number::New(env, duckdb::IntegerValue::Get(dval));
	} break;
	case duckdb::LogicalTypeId::FLOAT: {
		value = Napi::Number::New(env, duckdb::FloatValue::Get(dval));
	} break;
	case duckdb::LogicalTypeId::DOUBLE: {
		value = Napi::Number::New(env, duckdb::DoubleValue::Get(dval));
	} break;
	case duckdb::LogicalTypeId::BIGINT: {
		value = Napi::Number::New(env, duckdb::BigIntValue::Get(dval));
	} break;
	case duckdb::LogicalTypeId::HUGEINT: {
		value = Napi::Number::New(env, dval.GetValue<double>());
	} break;
	case duckdb::LogicalTypeId::DECIMAL: {
		value = Napi::Number::New(env, dval.GetValue<double>());
	} break;
	case duckdb::LogicalTypeId::INTERVAL: {
		auto interval = duckdb::IntervalValue::Get(dval);
		auto object_value = Napi::Object::New(env);
		object_value.Set("months", interval.months);
		object_value.Set("days", interval.days);
		object_value.Set("micros", interval.micros);
		value = object_value;
	} break;
#if (NAPI_VERSION > 4)
	case duckdb::LogicalTypeId::DATE: {
		const auto scale = duckdb::Interval::SECS_PER_DAY * duckdb::Interval::MSECS_PER_SEC;
		value = Napi::Date::New(env, double(dval.GetValue<int32_t>() * scale));
	} break;
	case duckdb::LogicalTypeId::TIMESTAMP:
	case duckdb::LogicalTypeId::TIMESTAMP_TZ: {
		value = Napi::Date::New(env, double(dval.GetValue<int64_t>() / duckdb::Interval::MICROS_PER_MSEC));
	} break;
#endif
	case duckdb::LogicalTypeId::VARCHAR: {
		value = Napi::String::New(env, duckdb::StringValue::Get(dval));
	} break;
	case duckdb::LogicalTypeId::BLOB: {
		auto &blob = duckdb::StringValue::Get(dval);
		value = Napi::Buffer<char>::Copy(env, blob.c_str(), blob.length());
	} break;
	case duckdb::LogicalTypeId::SQLNULL: {
		value = env.Null();
	} break;
	case duckdb::LogicalTypeId::LIST: {
		auto child_type = duckdb::ListType::GetChildType(dval.type());
		auto &child_values = duckdb::ListValue::GetChildren(dval);
		auto object_value = Napi::Array::New(env);
		for (duckdb::idx_t child_idx = 0; child_idx < child_values.size(); child_idx++) {
			auto child_value = child_values.at(child_idx);
			object_value.Set(child_idx, convert_col_val(env, child_value, child_type.id()));
		}
		value = object_value;
	} break;
	case duckdb::LogicalTypeId::STRUCT: {
		auto &child_types = duckdb::StructType::GetChildTypes(dval.type());
		auto &child_values = duckdb::StructValue::GetChildren(dval);
		auto object_value = Napi::Object::New(env);
		for (duckdb::idx_t child_idx = 0; child_idx < child_values.size(); child_idx++) {
			auto child_value = child_values.at(child_idx);
			auto child_type = child_types.at(child_idx);
			object_value.Set(child_type.first, convert_col_val(env, child_value, child_type.second.id()));
		}
		value = object_value;
	} break;
	default:
		Napi::Error::New(env, "Data type is not supported " + dval.type().ToString()).ThrowAsJavaScriptException();
		return env.Null();
	}

	return value;
}

static Napi::Value convert_chunk(Napi::Env &env, std::vector<std::string> names, duckdb::DataChunk &chunk) {
	Napi::EscapableHandleScope scope(env);
	std::vector<Napi::String> node_names;
	assert(names.size() == chunk.ColumnCount());
	for (auto &name : names) {
		node_names.push_back(Napi::String::New(env, name));
	}
	Napi::Array result(Napi::Array::New(env, chunk.size()));

	for (duckdb::idx_t row_idx = 0; row_idx < chunk.size(); row_idx++) {
		Napi::Object row_result = Napi::Object::New(env);

		for (duckdb::idx_t col_idx = 0; col_idx < chunk.ColumnCount(); col_idx++) {
			duckdb::Value dval = chunk.GetValue(col_idx, row_idx);
			if (dval.IsNull()) {
				row_result.Set(node_names[col_idx], env.Null());
				continue;
			}

			row_result.Set(node_names[col_idx], convert_col_val(env, dval, chunk.data[col_idx].GetType().id()));
		}
		result.Set(row_idx, row_result);
	}

	return scope.Escape(result);
}

enum RunType { RUN, EACH, ALL };

struct StatementParam {
	std::vector<duckdb::Value> params;
	Napi::Function callback;
	Napi::Function complete;
};

struct RunPreparedTask : public Task {
	RunPreparedTask(Statement &statement_, duckdb::unique_ptr<StatementParam> params_, RunType run_type_)
	    : Task(statement_, params_->callback), params(move(params_)), run_type(run_type_) {
	}

	void DoWork() override {
		auto &statement = Get<Statement>();
		// ignorant folk arrive here without caring about the prepare callback error
		if (!statement.statement || !statement.statement->success) {
			return;
		}

		result = statement.statement->Execute(params->params, run_type != RunType::ALL);
	}

	void Callback() override {
		auto &statement = Get<Statement>();
		Napi::Env env = statement.Env();
		Napi::HandleScope scope(env);

		auto cb = callback.Value();
		// if there was an error we need to say so
		if (!statement.statement) {
			cb.MakeCallback(statement.Value(), {Utils::CreateError(env, "statement was finalized")});
			return;
		}
		if (!statement.statement->success) {
			cb.MakeCallback(statement.Value(), {Utils::CreateError(env, statement.statement->error)});
			return;
		}
		if (!statement.statement->success) {
			cb.MakeCallback(statement.Value(), {Utils::CreateError(env, statement.statement->error)});
			return;
		}
		if (!result->success) {
			cb.MakeCallback(statement.Value(), {Utils::CreateError(env, result->error)});
			return;
		}

		switch (run_type) {
		case RunType::RUN:
			cb.MakeCallback(statement.Value(), {env.Null()});
			break;
		case RunType::EACH: {
			duckdb::idx_t count = 0;
			while (true) {
				auto chunk = result->Fetch();
				if (!chunk || chunk->size() == 0) {
					break;
				}

				auto chunk_converted = convert_chunk(env, result->names, *chunk).ToObject();
				if (!chunk_converted.IsArray()) {
					// error was set before
					return;
				}
				for (duckdb::idx_t row_idx = 0; row_idx < chunk->size(); row_idx++) {
					cb.MakeCallback(statement.Value(), {env.Null(), chunk_converted.Get(row_idx)});
					count++;
				}
			}
			if (!params->complete.IsUndefined() && params->complete.IsFunction()) {
				params->complete.MakeCallback(statement.Value(), {env.Null(), Napi::Number::New(env, count)});
			}
			break;
		}
		case RunType::ALL: {
			auto materialized_result = (duckdb::MaterializedQueryResult *)result.get();
			Napi::Array result_arr(Napi::Array::New(env, materialized_result->collection.Count()));

			duckdb::idx_t out_idx = 0;
			while (true) {
				auto chunk = result->Fetch();
				if (!chunk || chunk->size() == 0) {
					break;
				}
				// ToObject has to happen here otherwise the converted chunk gets garbage collected for some reason
				auto chunk_converted = convert_chunk(env, result->names, *chunk).ToObject();
				if (!chunk_converted.IsArray()) {
					// error was set before
					return;
				}
				for (duckdb::idx_t row_idx = 0; row_idx < chunk->size(); row_idx++) {
					result_arr.Set(out_idx++, chunk_converted.Get(row_idx));
				}
			}

			cb.MakeCallback(statement.Value(), {env.Null(), result_arr});
		} break;
		}
	}
	std::unique_ptr<duckdb::QueryResult> result;
	duckdb::unique_ptr<StatementParam> params;
	RunType run_type;
};

duckdb::unique_ptr<StatementParam> Statement::HandleArgs(const Napi::CallbackInfo &info) {
	size_t start_idx = ignore_first_param ? 1 : 0;
	auto params = duckdb::make_unique<StatementParam>();

	for (auto i = start_idx; i < info.Length(); i++) {
		auto &p = info[i];
		if (p.IsFunction()) {
			if (!params->callback.IsUndefined()) { // we already saw a callback, so this is the finalizer
				params->complete = p.As<Napi::Function>();
			} else {
				params->callback = p.As<Napi::Function>();
			}
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
	connection_ref->database_ref->Schedule(info.Env(),
	                                       duckdb::make_unique<RunPreparedTask>(*this, HandleArgs(info), RunType::ALL));
	return info.This();
}

Napi::Value Statement::Run(const Napi::CallbackInfo &info) {
	auto params = HandleArgs(info);
	connection_ref->database_ref->Schedule(info.Env(),
	                                       duckdb::make_unique<RunPreparedTask>(*this, HandleArgs(info), RunType::RUN));
	return info.This();
}

Napi::Value Statement::Each(const Napi::CallbackInfo &info) {
	auto params = HandleArgs(info);
	connection_ref->database_ref->Schedule(
	    info.Env(), duckdb::make_unique<RunPreparedTask>(*this, HandleArgs(info), RunType::EACH));
	return info.This();
}

struct FinalizeTask : public Task {
	FinalizeTask(Statement &statement_, Napi::Function callback_) : Task(statement_, callback_) {
	}

	void DoWork() override {
		// TODO why does this break stuff?
		// Get<Statement>().statement.reset();
	}
};

Napi::Value Statement::Finalize_(const Napi::CallbackInfo &info) {
	Napi::Env env = info.Env();
	Napi::HandleScope scope(env);

	Napi::Function callback;

	if (info.Length() > 0 && info[0].IsFunction()) {
		callback = info[0].As<Napi::Function>();
	}

	connection_ref->database_ref->Schedule(env, duckdb::make_unique<FinalizeTask>(*this, callback));
	return env.Null();
}

} // namespace node_duckdb
