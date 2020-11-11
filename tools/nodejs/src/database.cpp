#include "duckdb_node.hpp"

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
	OpenTask(Database &database_, std::string filename_, Napi::Function callback_)
	    : Task(database_, callback_), filename(filename_) {
	}

	void DoWork() override {
		try {
			Get<Database>().database = duckdb::make_unique<duckdb::DuckDB>(filename);
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
	std::string error = "";
	bool success = false;
};

// TODO handle parameters here
Database::Database(const Napi::CallbackInfo &info) : Napi::ObjectWrap<Database>(info), task_inflight(false) {
	if (info.Length() <= 0 || !info[0].IsString()) {
		throw std::runtime_error("eek4");
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

	Schedule(info.Env(), duckdb::make_unique<OpenTask>(*this, filename, callback));
}

void Database::Schedule(Napi::Env env, unique_ptr<Task> task) {
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
	holder->task->Callback();
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
	if (task_inflight) {
		return;
	}
	task_inflight = true;

	if (task_queue.empty()) {
		return;
	}
	auto task = move(task_queue.front());
	task_queue.pop();

	auto holder = new TaskHolder();
	holder->task = move(task);
	holder->db = this;

	// TODO do we need to do error checking here?
	napi_create_async_work(env, NULL, Napi::String::New(env, "duckdb.Database.Task"), task_execute, task_complete, holder,
	                       &holder->request);

	napi_queue_async_work(env, holder->request);
}

Napi::Value Database::Parallelize(const Napi::CallbackInfo &info) {
	return Serialize(info);
}

// TODO check params
// TODO can we get multiple params here?
Napi::Value Database::Serialize(const Napi::CallbackInfo &info) {
	info[0].As<Napi::Function>().MakeCallback(info.This(), {});
	Process(info.Env());
	return info.This();
}

// TODO implement these
Napi::Value Database::Wait(const Napi::CallbackInfo &info) {
	return info.This();
}

struct CloseTask : public Task {
	CloseTask(Database &database_) : Task(database_, database_.Env().Null().As<Napi::Function>()) {
	}

	void DoWork() override {
		Get<Database>().database.reset();
	}

	void Callback() override {
	}
};

Napi::Value Database::Close(const Napi::CallbackInfo &info) {
	Schedule(info.Env(), duckdb::make_unique<CloseTask>(*this));

	return info.This();
}

Napi::Value Database::Interrupt(const Napi::CallbackInfo &info) {
	return info.This();
}

Napi::Value Database::Connect(const Napi::CallbackInfo &info) {
	return Connection::constructor.New({Value()});
}

} // namespace node_duckdb
