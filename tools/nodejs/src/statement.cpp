#include "duckdb_node.hpp"

namespace node_duckdb {

Napi::FunctionReference Statement::constructor;

Napi::Object Statement::Init(Napi::Env env, Napi::Object exports) {
	Napi::HandleScope scope(env);

	Napi::Function t = DefineClass(
	    env, "Result", {InstanceMethod("run", &Statement::Run), InstanceMethod("finalize", &Statement::Finalize_)});

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
		printf("RunPreparedTask::DoWork()\n");
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

Napi::Value Statement::Finalize_(const Napi::CallbackInfo &info) {
	statement.reset();
}

}