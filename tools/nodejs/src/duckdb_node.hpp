#pragma once
#define NODE_ADDON_API_DISABLE_DEPRECATED
#include <napi.h>
#include <queue>
#include <unordered_map>

#include "duckdb.hpp"

namespace node_duckdb {

struct Task {
	Task(Napi::Reference<Napi::Object> &object_, Napi::Function cb_) : object(object_) {
		if (!cb_.IsUndefined() && cb_.IsFunction()) {
			callback = Persistent(cb_); // TODO not sure what this does
		}
		object.Ref();
	}
	virtual void DoWork() = 0;

	virtual void Callback() {
		auto env = object.Env();
		Napi::HandleScope scope(env);
		callback.Value().MakeCallback(object.Value(), {env.Null()});
	};

	virtual ~Task() {
		object.Unref();
	};

	template <class T>
	T &Get() {
		return (T &)object;
	}

	Napi::FunctionReference callback;
	Napi::Reference<Napi::Object> &object;
};

class Connection;

class Database : public Napi::ObjectWrap<Database> {
public:
	Database(const Napi::CallbackInfo &info);
	static Napi::Object Init(Napi::Env env, Napi::Object exports);
	void Process(Napi::Env env);
	void TaskComplete(Napi::Env env);

	void Schedule(Napi::Env env, std::unique_ptr<Task> task);

	static bool HasInstance(Napi::Value val) {
		Napi::Env env = val.Env();
		Napi::HandleScope scope(env);
		if (!val.IsObject())
			return false;
		Napi::Object obj = val.As<Napi::Object>();
		return obj.InstanceOf(constructor.Value());
	}

public:
	Napi::Value Connect(const Napi::CallbackInfo &info);
	Napi::Value Wait(const Napi::CallbackInfo &info);
	Napi::Value Serialize(const Napi::CallbackInfo &info);
	Napi::Value Parallelize(const Napi::CallbackInfo &info);
	Napi::Value Interrupt(const Napi::CallbackInfo &info);
	Napi::Value Close(const Napi::CallbackInfo &info);

public:
	constexpr static int DUCKDB_NODEJS_ERROR = -1;
	constexpr static int DUCKDB_NODEJS_READONLY = 1;
	std::unique_ptr<duckdb::DuckDB> database;

private:
	// TODO this task queue can also live in the connection?
	std::queue<std::unique_ptr<Task>> task_queue;
	std::mutex task_mutex;
	bool task_inflight;
	static Napi::FunctionReference constructor;
};

struct JSArgs;
void DuckDBNodeUDFLauncher(Napi::Env env, Napi::Function jsudf, nullptr_t *, JSArgs *data);

typedef Napi::TypedThreadSafeFunction<nullptr_t, JSArgs, DuckDBNodeUDFLauncher> DuckDBNodeUDFFUnction;

class Connection : public Napi::ObjectWrap<Connection> {
public:
	Connection(const Napi::CallbackInfo &info);
	~Connection();
	static Napi::Object Init(Napi::Env env, Napi::Object exports);

public:
	Napi::Value Prepare(const Napi::CallbackInfo &info);
	Napi::Value Exec(const Napi::CallbackInfo &info);
	Napi::Value Register(const Napi::CallbackInfo &info);
	Napi::Value Unregister(const Napi::CallbackInfo &info);

	static bool HasInstance(Napi::Value val) {
		Napi::Env env = val.Env();
		Napi::HandleScope scope(env);
		if (!val.IsObject())
			return false;
		Napi::Object obj = val.As<Napi::Object>();
		return obj.InstanceOf(constructor.Value());
	}

public:
	static Napi::FunctionReference constructor;
	std::unique_ptr<duckdb::Connection> connection;
	Database *database_ref;
	std::unordered_map<std::string, DuckDBNodeUDFFUnction> udfs;
};

struct StatementParam;

class Statement : public Napi::ObjectWrap<Statement> {
public:
	Statement(const Napi::CallbackInfo &info);
	~Statement();
	static Napi::Object Init(Napi::Env env, Napi::Object exports);
	void SetProcessFirstParam() {
		ignore_first_param = false;
	}

public:
	Napi::Value All(const Napi::CallbackInfo &info);
	Napi::Value Each(const Napi::CallbackInfo &info);
	Napi::Value Run(const Napi::CallbackInfo &info);
	Napi::Value Bind(const Napi::CallbackInfo &info);

	Napi::Value Finalize_(const Napi::CallbackInfo &info);

public:
	static Napi::FunctionReference constructor;
	std::unique_ptr<duckdb::PreparedStatement> statement;
	Connection *connection_ref;
	bool ignore_first_param = true;
	std::string sql;

private:
	std::unique_ptr<StatementParam> HandleArgs(const Napi::CallbackInfo &info);
};

struct TaskHolder {
	std::unique_ptr<Task> task;
	napi_async_work request;
	Database *db;
};

class Utils {
public:
	static Napi::Value CreateError(Napi::Env env, std::string msg);
	static bool OtherIsInt(Napi::Number source);

	template <class T>
	static T *NewUnwrap(std::vector<napi_value> args) {
		auto obj = T::constructor.New(args);
		return Napi::ObjectWrap<T>::Unwrap(obj);
	}
};

} // namespace node_duckdb
