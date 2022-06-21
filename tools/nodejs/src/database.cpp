#include "duckdb_node.hpp"
#include "parquet-amalgamation.hpp"

namespace node_duckdb {

Napi::FunctionReference Database::constructor;

Napi::Object Database::Init(Napi::Env env, Napi::Object exports) {
	Napi::HandleScope scope(env);

	Napi::Function t = DefineClass(
	    env, "Database",
	    {InstanceMethod("close", &Database::Close), InstanceMethod("wait", &Database::Wait),
	     InstanceMethod("serialize", &Database::Serialize), InstanceMethod("parallelize", &Database::Parallelize),
	     InstanceMethod("connect", &Database::Connect), InstanceMethod("interrupt", &Database::Interrupt)});

	constructor = Napi::Persistent(t);
	constructor.SuppressDestruct();

	exports.Set("Database", t);
	return exports;
}

struct OpenTask : public Task {
	OpenTask(Database &database_, std::string filename_, bool read_only_, Napi::Function callback_)
	    : Task(database_, callback_), filename(filename_), read_only(read_only_) {
	}

	void DoWork() override {
		try {
			duckdb::DBConfig config;
			if (read_only) {
				config.access_mode = duckdb::AccessMode::READ_ONLY;
			}
			Get<Database>().database = duckdb::make_unique<duckdb::DuckDB>(filename, &config);
			duckdb::ParquetExtension extension;
			extension.Load(*Get<Database>().database);
			success = true;

		} catch (std::exception &ex) {
			error = ex.what();
		}
	}

	void Callback() override {
		auto &database = Get<Database>();
		Napi::Env env = database.Env();

		std::vector<napi_value> args;
		if (!success) {
			args.push_back(Utils::CreateError(env, error));
		} else {
			args.push_back(env.Null());
		}

		Napi::HandleScope scope(env);

		callback.Value().MakeCallback(database.Value(), args);
	}

	std::string filename;
	bool read_only = false;
	std::string error = "";
	bool success = false;
};

Database::Database(const Napi::CallbackInfo &info) : Napi::ObjectWrap<Database>(info), task_inflight(false) {
	auto env = info.Env();
	if (info.Length() < 1 || !info[0].IsString()) {
		Napi::TypeError::New(env, "Database location expected").ThrowAsJavaScriptException();
		return;
	}
	std::string filename = info[0].As<Napi::String>();
	unsigned int pos = 1;
	int mode = 0;
	if (info.Length() >= pos && info[pos].IsNumber() && Utils::OtherIsInt(info[pos].As<Napi::Number>())) {
		mode = info[pos++].As<Napi::Number>().Int32Value();
	}

	Napi::Function callback;
	if (info.Length() >= pos && info[pos].IsFunction()) {
		callback = info[pos++].As<Napi::Function>();
	}

	Schedule(env, duckdb::make_unique<OpenTask>(*this, filename, mode == DUCKDB_NODEJS_READONLY, callback));
}

void Database::Schedule(Napi::Env env, std::unique_ptr<Task> task) {
	{
		std::lock_guard<std::mutex> lock(task_mutex);
		task_queue.push(move(task));
	}
	Process(env);
}

static void task_execute(napi_env e, void *data) {
	auto holder = (TaskHolder *)data;
	holder->task->DoWork();
}

static void task_complete(napi_env e, napi_status status, void *data) {
	std::unique_ptr<TaskHolder> holder((TaskHolder *)data);
	holder->db->TaskComplete(e);
	if (holder->task->callback.Value().IsFunction()) {
		holder->task->Callback();
	}
}

void Database::TaskComplete(Napi::Env env) {
	{
		std::lock_guard<std::mutex> lock(task_mutex);
		task_inflight = false;
	}
	Process(env);
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

	auto task = move(task_queue.front());
	task_queue.pop();

	auto holder = new TaskHolder();
	holder->task = move(task);
	holder->db = this;

	napi_create_async_work(env, NULL, Napi::String::New(env, "duckdb.Database.Task"), task_execute, task_complete,
	                       holder, &holder->request);

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
	WaitTask(Database &database_, Napi::Function callback_) : Task(database_, callback_) {
	}

	void DoWork() override {
		// nop
	}
};

Napi::Value Database::Wait(const Napi::CallbackInfo &info) {
	Schedule(info.Env(), duckdb::make_unique<WaitTask>(*this, info[0].As<Napi::Function>()));
	return info.This();
}

struct CloseTask : public Task {
	CloseTask(Database &database_, Napi::Function callback_) : Task(database_, callback_) {
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

	Schedule(info.Env(), duckdb::make_unique<CloseTask>(*this, callback));

	return info.This();
}

Napi::Value Database::Interrupt(const Napi::CallbackInfo &info) {
	return info.This();
}

Napi::Value Database::Connect(const Napi::CallbackInfo &info) {
	return Connection::constructor.New({Value()});
}

} // namespace node_duckdb
