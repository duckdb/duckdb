#include "duckdb_node.hpp"

namespace node_duckdb {

Napi::Object Database::Init(Napi::Env env, Napi::Object exports) {
	Napi::HandleScope scope(env);

	Napi::Function t = DefineClass(
	    env, "Database",
	    {InstanceMethod("run", &Database::Run), InstanceMethod("all", &Database::All),
	     InstanceMethod("prepare", &Database::Prepare), InstanceMethod("close", &Database::Close),
	     InstanceMethod("wait", &Database::Wait), InstanceMethod("serialize", &Database::Serialize),
	     InstanceMethod("parallelize", &Database::Parallelize), InstanceMethod("connect", &Database::Connect),
	     InstanceMethod("interrupt", &Database::Interrupt), InstanceMethod("each", &Database::Each),
	     InstanceAccessor("open", &Database::OpenGetter, nullptr)});

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

		database.default_connection = Utils::NewUnwrap<Connection>({database.Value()});
		database.default_connection->Ref();

		callback.Value().MakeCallback(database.Value(), args);
	}

	std::string filename;
	std::string error = "";
	bool success = false;
};

Database::Database(const Napi::CallbackInfo &info) : Napi::ObjectWrap<Database>(info) {
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

Database::~Database() {
	printf("~Database\n");
	if (default_connection) {
		default_connection->Unref();
	}
}

void Database::Schedule(Napi::Env env, unique_ptr<Task> task) {
	{
		std::lock_guard<std::mutex> lock(task_mutex);
		task_queue.push(move(task));
	}
	Process(env);
}

static void TaskExecute(napi_env e, void *data) {
	auto holder = (TaskHolder *)data;
	holder->task->DoWork();
}

static void TaskComplete(napi_env e, napi_status status, void *data) {
	std::unique_ptr<TaskHolder> holder((TaskHolder *)data);
	holder->task->Callback();
	holder->db->Process(e);
}

void Database::Process(Napi::Env env) {
	std::lock_guard<std::mutex> lock(task_mutex);

	if (task_queue.empty()) {
		return;
	}
	auto task = move(task_queue.front());
	task_queue.pop();

	auto holder = new TaskHolder();
	holder->task = move(task);
	holder->db = this;

	napi_create_async_work(env, NULL, Napi::String::New(env, "duckdb.Database.Task"), TaskExecute, TaskComplete, holder,
	                       &holder->request);

	napi_queue_async_work(env, holder->request);
}

Napi::Value Database::OpenGetter(const Napi::CallbackInfo &info) {
	return Napi::Boolean::New(this->Env(), this->database != nullptr);
}

Napi::Value Database::Parallelize(const Napi::CallbackInfo &info) {
	return Serialize(info);
}

// TODO check params
Napi::Value Database::Serialize(const Napi::CallbackInfo &info) {
	info[0].As<Napi::Function>().MakeCallback(info.This(), {});
	;
	Process(info.Env());
	return info.This();
}

Napi::Value Database::Run(const Napi::CallbackInfo &info) {
	// TODO check default_connection and database openness here
	default_connection->Run(info);
	return info.This();
}

Napi::Value Database::All(const Napi::CallbackInfo &info) {
	// TODO check default_connection and database openness here

	default_connection->All(info);
	return info.This();
}

Napi::Value Database::Each(const Napi::CallbackInfo &info) {
	// TODO check default_connection and database openness here

	default_connection->Each(info);
	return info.This();
}

Napi::Value Database::Prepare(const Napi::CallbackInfo &info) {
	// TODO check default_connection and database openness here

	return default_connection->Prepare(info);
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
