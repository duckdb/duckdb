#include "duckdb/parser/expression/constant_expression.hpp"
#include "duckdb/parser/expression/function_expression.hpp"
#include "duckdb/parser/tableref/table_function_ref.hpp"
#include "duckdb/storage/buffer_manager.hpp"
#include "duckdb_node.hpp"
#include "napi.h"

#include <iostream>
#include <thread>

namespace node_duckdb {

Napi::FunctionReference Database::Init(Napi::Env env, Napi::Object exports) {
	Napi::HandleScope scope(env);

	Napi::Function t = DefineClass(
	    env, "Database",
	    {InstanceMethod("close_internal", &Database::Close), InstanceMethod("wait", &Database::Wait),
	     InstanceMethod("serialize", &Database::Serialize), InstanceMethod("parallelize", &Database::Parallelize),
	     InstanceMethod("connect", &Database::Connect), InstanceMethod("interrupt", &Database::Interrupt),
	     InstanceMethod("registerReplacementScan", &Database::RegisterReplacementScan)});

	exports.Set("Database", t);

	return Napi::Persistent(t);
}

struct OpenTask : public Task {
	OpenTask(Database &database_, std::string filename_, duckdb::AccessMode access_mode_, Napi::Object config_,
	         Napi::Function callback_)
	    : Task(database_, callback_), filename(filename_) {

		duckdb_config.options.access_mode = access_mode_;
		Napi::Env env = database_.Env();
		Napi::HandleScope scope(env);

		if (!config_.IsUndefined()) {
			const Napi::Array config_names = config_.GetPropertyNames();

			for (duckdb::idx_t config_idx = 0; config_idx < config_names.Length(); config_idx++) {
				std::string key = config_names.Get(config_idx).As<Napi::String>();
				std::string val = config_.Get(key).As<Napi::String>();
				try {
					duckdb_config.SetOptionByName(key, duckdb::Value(val));
				} catch (std::exception &e) {
					Napi::TypeError::New(env, "Failed to set configuration option " + key + ": " + e.what())
					    .ThrowAsJavaScriptException();
					return;
				}
			}
		}
	}

	void DoWork() override {
		try {
			Get<Database>().database = duckdb::make_uniq<duckdb::DuckDB>(filename, &duckdb_config);
			success = true;

		} catch (const duckdb::Exception &ex) {
			error = duckdb::PreservedError(ex);
		} catch (std::exception &ex) {
			error = duckdb::PreservedError(ex);
		}
	}

	void Callback() override {
		auto &database = Get<Database>();
		Napi::Env env = database.Env();

		vector<napi_value> args;
		if (!success) {
			args.push_back(Utils::CreateError(env, error));
		} else {
			args.push_back(env.Null());
		}

		Napi::HandleScope scope(env);

		callback.Value().MakeCallback(database.Value(), args);
	}

	std::string filename;
	duckdb::DBConfig duckdb_config;
	duckdb::PreservedError error;
	bool success = false;
};

Database::Database(const Napi::CallbackInfo &info)
    : Napi::ObjectWrap<Database>(info), task_inflight(false), env(info.Env()) {
	auto env = info.Env();

	if (info.Length() < 1 || !info[0].IsString()) {
		Napi::TypeError::New(env, "Database location expected").ThrowAsJavaScriptException();
		return;
	}
	std::string filename = info[0].As<Napi::String>();
	unsigned int pos = 1;

	duckdb::AccessMode access_mode = duckdb::AccessMode::AUTOMATIC;

	int mode = 0;
	if (info.Length() >= pos && info[pos].IsNumber() && Utils::OtherIsInt(info[pos].As<Napi::Number>())) {
		mode = info[pos++].As<Napi::Number>().Int32Value();
		if (mode == DUCKDB_NODEJS_READONLY) {
			access_mode = duckdb::AccessMode::READ_ONLY;
		}
	}

	Napi::Object config;
	if (info.Length() >= pos && info[pos].IsObject() && !info[pos].IsFunction()) {
		config = info[pos++].As<Napi::Object>();
	}

	Napi::Function callback;
	if (info.Length() >= pos && info[pos].IsFunction()) {
		callback = info[pos++].As<Napi::Function>();
	}

	Schedule(env, duckdb::make_uniq<OpenTask>(*this, filename, access_mode, config, callback));
}

Database::~Database() {
	Napi::MemoryManagement::AdjustExternalMemory(env, -bytes_allocated);
}

void Database::Schedule(Napi::Env env, duckdb::unique_ptr<Task> task) {
	{
		std::lock_guard<std::mutex> lock(task_mutex);
		task_queue.push(std::move(task));
	}
	Process(env);
}

static void TaskExecuteCallback(napi_env e, void *data) {
	auto holder = (TaskHolder *)data;
	holder->task->DoWork();
}

static void TaskCompleteCallback(napi_env e, napi_status status, void *data) {
	duckdb::unique_ptr<TaskHolder> holder((TaskHolder *)data);
	holder->db->TaskComplete(e);
	holder->task->DoCallback();
}

void Database::TaskComplete(Napi::Env env) {
	{
		std::lock_guard<std::mutex> lock(task_mutex);
		task_inflight = false;
	}
	Process(env);

	if (database) {
		// Bookkeeping: tell node (and the node GC in particular) how much
		// memory we're using, such that it can make better decisions on when to
		// trigger collections.
		auto &buffer_manager = duckdb::BufferManager::GetBufferManager(*database->instance);
		auto current_bytes = buffer_manager.GetUsedMemory();
		Napi::MemoryManagement::AdjustExternalMemory(env, current_bytes - bytes_allocated);
		bytes_allocated = current_bytes;
	}
}

void Database::Process(Napi::Env env) {
	std::lock_guard<std::mutex> lock(task_mutex);
	if (task_queue.empty()) {
		return;
	}
	if (task_inflight) {
		return;
	}
	task_inflight = true;

	auto task = std::move(task_queue.front());
	task_queue.pop();

	auto holder = new TaskHolder();
	holder->task = std::move(task);
	holder->db = this;

	napi_create_async_work(env, nullptr, Napi::String::New(env, "duckdb.Database.Task"), TaskExecuteCallback,
	                       TaskCompleteCallback, holder, &holder->request);

	napi_queue_async_work(env, holder->request);
}

Napi::Value Database::Parallelize(const Napi::CallbackInfo &info) {
	return Serialize(info);
}

Napi::Value Database::Serialize(const Napi::CallbackInfo &info) {
	Napi::Env env = info.Env();
	if (info.Length() < 1) {
		return info.This();
	}
	if (info.Length() < 1 || !info[0].IsFunction()) {
		Napi::TypeError::New(env, "Callback expected").ThrowAsJavaScriptException();
		return env.Null();
	}
	Napi::HandleScope scope(env);
	info[0].As<Napi::Function>().MakeCallback(info.This(), {});
	Process(env);
	return info.This();
}

struct WaitTask : public Task {
	WaitTask(Database &database, Napi::Function callback) : Task(database, callback) {
	}

	void DoWork() override {
		// nop
	}
};

Napi::Value Database::Wait(const Napi::CallbackInfo &info) {
	Schedule(info.Env(), duckdb::make_uniq<WaitTask>(*this, info[0].As<Napi::Function>()));
	return info.This();
}

struct CloseTask : public Task {
	CloseTask(Database &database, Napi::Function callback) : Task(database, callback) {
	}

	void DoWork() override {
		auto &database = Get<Database>();
		if (database.database) {
			database.database.reset();
			success = true;
		} else {
			success = false;
		}
	}

	void Callback() override {
		auto &database = Get<Database>();
		auto env = database.Env();
		Napi::HandleScope scope(env);

		auto cb = callback.Value();
		if (!success) {
			cb.MakeCallback(database.Value(), {Utils::CreateError(env, "Database was already closed")});
			return;
		}
		cb.MakeCallback(database.Value(), {env.Null(), database.Value()});
	}

	bool success = false;
};

Napi::Value Database::Close(const Napi::CallbackInfo &info) {
	Napi::Function callback;
	if (info.Length() > 0 && info[0].IsFunction()) {
		callback = info[0].As<Napi::Function>();
	}

	Schedule(info.Env(), duckdb::make_uniq<CloseTask>(*this, callback));

	return info.This();
}

Napi::Value Database::Interrupt(const Napi::CallbackInfo &info) {
	return info.This();
}

Napi::Value Database::Connect(const Napi::CallbackInfo &info) {
	Napi::HandleScope scope(env);
	return Connection::NewInstance(Value());
}

struct JSRSArgs {
	std::string table = "";
	std::string function = "";
	vector<duckdb::Value> parameters;
	bool done = false;
	duckdb::PreservedError error;
};

struct NodeReplacementScanData : duckdb::ReplacementScanData {
	NodeReplacementScanData(duckdb_node_rs_function_t rs) : rs(std::move(rs)) {};
	duckdb_node_rs_function_t rs;
};

void DuckDBNodeRSLauncher(Napi::Env env, Napi::Function jsrs, std::nullptr_t *, JSRSArgs *jsargs) {
	try {
		Napi::EscapableHandleScope scope(env);
		auto arg = Napi::String::New(env, jsargs->table);
		auto result = jsrs({arg});
		if (result && result.IsObject()) {
			auto obj = result.As<Napi::Object>();
			jsargs->function = obj.Get("function").ToString().Utf8Value();
			auto parameters = obj.Get("parameters");
			if (parameters.IsArray()) {
				auto paramArray = parameters.As<Napi::Array>();
				for (uint32_t i = 0; i < paramArray.Length(); i++) {
					jsargs->parameters.push_back(Utils::BindParameter(paramArray.Get(i)));
				}
			} else {
				throw duckdb::Exception("Expected parameter array");
			}
		} else if (!result.IsNull()) {
			throw duckdb::Exception("Invalid scan replacement result");
		}
	} catch (const duckdb::Exception &e) {
		jsargs->error = duckdb::PreservedError(e);
	} catch (const std::exception &e) {
		jsargs->error = duckdb::PreservedError(e);
	}
	jsargs->done = true;
}

static duckdb::unique_ptr<duckdb::TableRef>
ScanReplacement(duckdb::ClientContext &context, const std::string &table_name, duckdb::ReplacementScanData *data) {
	JSRSArgs jsargs;
	jsargs.table = table_name;
	((NodeReplacementScanData *)data)->rs.BlockingCall(&jsargs);
	while (!jsargs.done) {
		std::this_thread::yield();
	}
	if (jsargs.error) {
		jsargs.error.Throw();
	}
	if (jsargs.function != "") {
		auto table_function = duckdb::make_uniq<duckdb::TableFunctionRef>();
		duckdb::vector<duckdb::unique_ptr<duckdb::ParsedExpression>> children;
		for (auto &param : jsargs.parameters) {
			children.push_back(duckdb::make_uniq<duckdb::ConstantExpression>(std::move(param)));
		}
		table_function->function = duckdb::make_uniq<duckdb::FunctionExpression>(jsargs.function, std::move(children));
		return std::move(table_function);
	}
	return nullptr;
}

struct RegisterRsTask : public Task {
	RegisterRsTask(Database &database, duckdb_node_rs_function_t rs, Napi::Promise::Deferred deferred)
	    : Task(database), rs(std::move(rs)), deferred(deferred) {
	}

	void DoWork() override {
		auto &database = Get<Database>();
		if (database.database) {
			database.database->instance->config.replacement_scans.emplace_back(
			    ScanReplacement, duckdb::make_uniq<NodeReplacementScanData>(rs));
		}
	}

	void DoCallback() override {
		deferred.Resolve(deferred.Env().Undefined());
	}

	duckdb_node_rs_function_t rs;
	Napi::Promise::Deferred deferred;
};

Napi::Value Database::RegisterReplacementScan(const Napi::CallbackInfo &info) {
	auto env = info.Env();
	auto deferred = Napi::Promise::Deferred::New(info.Env());
	if (info.Length() < 1) {
		Napi::TypeError::New(env, "Replacement scan callback expected").ThrowAsJavaScriptException();
		return env.Null();
	}
	Napi::Function rs_callback = info[0].As<Napi::Function>();
	auto rs =
	    duckdb_node_rs_function_t::New(env, rs_callback, "duckdb_node_rs_" + std::to_string(replacement_scan_count++),
	                                   0, 1, nullptr, [](Napi::Env, void *, std::nullptr_t *ctx) {});
	rs.Unref(env);

	Schedule(info.Env(), duckdb::make_uniq<RegisterRsTask>(*this, rs, deferred));

	return deferred.Promise();
}

} // namespace node_duckdb
