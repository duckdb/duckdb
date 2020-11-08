#pragma once
#include <napi.h>
#include "duckdb.hpp"

namespace node_duckdb {

template <class T> static T *construct_node_object(std::vector<napi_value> args) {
	auto obj = T::constructor.New(args);
	return Napi::ObjectWrap<T>::Unwrap(obj);
}

class Database;

class Connection : public Napi::ObjectWrap<Connection> {
public:
	Connection(const Napi::CallbackInfo &info);
	~Connection();
	static Napi::Object Init(Napi::Env env, Napi::Object exports);

	Napi::Value Prepare(const Napi::CallbackInfo &info);
	Napi::Value Run(const Napi::CallbackInfo &info);
	Napi::Value All(const Napi::CallbackInfo &info);
	Napi::Value Exec(const Napi::CallbackInfo &info);

public:
	static Napi::FunctionReference constructor;
	std::unique_ptr<duckdb::Connection> connection;
	Database *database_ref;
};

struct Task {
	Task(Napi::Function cb_) {
		if (!cb_.IsUndefined() && cb_.IsFunction()) {
			callback = Persistent(cb_);
		}
	}
	virtual void DoWork() = 0;
	virtual void Callback() = 0;

	virtual ~Task(){};

	Napi::FunctionReference callback;
};

class Database : public Napi::ObjectWrap<Database> {
public:
	Database(const Napi::CallbackInfo &info);
	static Napi::Object Init(Napi::Env env, Napi::Object exports);
	void Process(Napi::Env env);
	void Schedule(Napi::Env env, unique_ptr<Task> task);

private:
	Napi::Value Prepare(const Napi::CallbackInfo &info);
	Napi::Value Run(const Napi::CallbackInfo &info);
	Napi::Value All(const Napi::CallbackInfo &info);

	Napi::Value Wait(const Napi::CallbackInfo &info);
	Napi::Value Serialize(const Napi::CallbackInfo &info);
	Napi::Value Parallelize(const Napi::CallbackInfo &info);
	Napi::Value Interrupt(const Napi::CallbackInfo &info);
	Napi::Value OpenGetter(const Napi::CallbackInfo &info);
	Napi::Value Close(const Napi::CallbackInfo &info);

public:
	static Napi::FunctionReference constructor;

	constexpr static int DUCKDB_NODEJS_ERROR = -1;
	constexpr static int DUCKDB_NODEJS_READONLY = 1;

	std::unique_ptr<duckdb::DuckDB> database;

private:
	// TODO this task queue can also live in the connection?
	std::queue<unique_ptr<Task>> task_queue;
	std::mutex task_mutex;
};

class Statement : public Napi::ObjectWrap<Statement> {
public:
	Statement(const Napi::CallbackInfo &info);
	~Statement();

	static Napi::Object Init(Napi::Env env, Napi::Object exports);
	Napi::Value All(const Napi::CallbackInfo &info);

private:
	Napi::Value Bind(const Napi::CallbackInfo &info);
	Napi::Value Run(const Napi::CallbackInfo &info);
	Napi::Value Finalize_(const Napi::CallbackInfo &info);

public:
	static Napi::FunctionReference constructor;
	std::unique_ptr<duckdb::PreparedStatement> statement;

private:
	Connection *connection_ref;
};

class Result : public Napi::ObjectWrap<Result> {
public:
	Result(const Napi::CallbackInfo &info);
	static Napi::Object Init(Napi::Env env, Napi::Object exports);

private:
	Napi::Value Get(const Napi::CallbackInfo &info);
	Napi::Value All(const Napi::CallbackInfo &info);
	Napi::Value Each(const Napi::CallbackInfo &info);

public:
	static Napi::FunctionReference constructor;

private:
	std::unique_ptr<duckdb::QueryResult> result;
	Statement *statement_ref;
};

struct TaskHolder {
	unique_ptr<Task> task;
	napi_async_work request;
	Database *db;
};

class Utils {
public:
	static Napi::Value CreateError(Napi::Env env, std::string msg);
	static bool OtherIsInt(Napi::Number source);
};

} // namespace node_duckdb
