#pragma once
#define NODE_ADDON_API_DISABLE_DEPRECATED
#include "duckdb.hpp"

#include <napi.h>
#include <queue>
#include <unordered_map>

namespace node_duckdb {

struct Task {
	Task(Napi::Reference<Napi::Object> &object, Napi::Function cb) : object(object) {
		if (!cb.IsUndefined() && cb.IsFunction()) {
			callback = Persistent(cb); // TODO not sure what this does
		}
		object.Ref();
	}
	explicit Task(Napi::Reference<Napi::Object> &object) : object(object) {
		object.Ref();
	}

	// Called on a worker thread (i.e., not the main event loop thread)
	virtual void DoWork() = 0;

	// Called on the event loop thread after the work has been completed. By
	// default, call the associated callback, if defined. If you're writing
	// a Task that uses promises, override this method instead of Callback.
	virtual void DoCallback() {
		auto env = object.Env();
		Napi::HandleScope scope(env);

		if (!callback.Value().IsUndefined()) {
			Callback();
		}
	}

	// Called on the event loop thread by DoCallback (see above)
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
	explicit Database(const Napi::CallbackInfo &info);
	~Database() override;
	static Napi::Object Init(Napi::Env env, Napi::Object exports);
	void Process(Napi::Env env);
	void TaskComplete(Napi::Env env);

	void Schedule(Napi::Env env, std::unique_ptr<Task> task);

	static bool HasInstance(Napi::Value val) {
		Napi::Env env = val.Env();
		Napi::HandleScope scope(env);
		if (!val.IsObject()) {
			return false;
		}
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
	Napi::Env env;
	int64_t bytes_allocated = 0;
};

struct JSArgs;
void DuckDBNodeUDFLauncher(Napi::Env env, Napi::Function jsudf, std::nullptr_t *, JSArgs *data);

typedef Napi::TypedThreadSafeFunction<std::nullptr_t, JSArgs, DuckDBNodeUDFLauncher> duckdb_node_udf_function_t;

class Connection : public Napi::ObjectWrap<Connection> {
public:
	explicit Connection(const Napi::CallbackInfo &info);
	~Connection() override;
	static Napi::Object Init(Napi::Env env, Napi::Object exports);

public:
	Napi::Value Prepare(const Napi::CallbackInfo &info);
	Napi::Value Exec(const Napi::CallbackInfo &info);
	Napi::Value RegisterUdf(const Napi::CallbackInfo &info);
	Napi::Value UnregisterUdf(const Napi::CallbackInfo &info);
	Napi::Value RegisterBuffer(const Napi::CallbackInfo &info);
	Napi::Value UnRegisterBuffer(const Napi::CallbackInfo &info);

	static bool HasInstance(Napi::Value val) {
		Napi::Env env = val.Env();
		Napi::HandleScope scope(env);
		if (!val.IsObject()) {
			return false;
		}
		Napi::Object obj = val.As<Napi::Object>();
		return obj.InstanceOf(constructor.Value());
	}

public:
	static Napi::FunctionReference constructor;
	std::unique_ptr<duckdb::Connection> connection;
	Database *database_ref;
	std::unordered_map<std::string, duckdb_node_udf_function_t> udfs;
	std::unordered_map<std::string, Napi::Reference<Napi::Array>> array_references;
};

struct StatementParam;

class Statement : public Napi::ObjectWrap<Statement> {
public:
	explicit Statement(const Napi::CallbackInfo &info);
	~Statement() override;
	static Napi::Object Init(Napi::Env env, Napi::Object exports);
	void SetProcessFirstParam() {
		ignore_first_param = false;
	}

public:
	Napi::Value All(const Napi::CallbackInfo &info);
	Napi::Value ArrowIPCAll(const Napi::CallbackInfo &info);
	Napi::Value Each(const Napi::CallbackInfo &info);
	Napi::Value Run(const Napi::CallbackInfo &info);
	Napi::Value Finish(const Napi::CallbackInfo &info);
	Napi::Value Stream(const Napi::CallbackInfo &info);

public:
	static Napi::FunctionReference constructor;
	std::unique_ptr<duckdb::PreparedStatement> statement;
	Connection *connection_ref;
	bool ignore_first_param = true;
	std::string sql;

private:
	std::unique_ptr<StatementParam> HandleArgs(const Napi::CallbackInfo &info);
};

class QueryResult : public Napi::ObjectWrap<QueryResult> {
public:
	explicit QueryResult(const Napi::CallbackInfo &info);
	~QueryResult() override;
	static Napi::Object Init(Napi::Env env, Napi::Object exports);
	std::unique_ptr<duckdb::QueryResult> result;

public:
	static Napi::FunctionReference constructor;
	Napi::Value NextChunk(const Napi::CallbackInfo &info);
	Napi::Value NextIpcBuffer(const Napi::CallbackInfo &info);
	duckdb::shared_ptr<ArrowSchema> cschema;

private:
	Database *database_ref;
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

Napi::Array EncodeDataChunk(Napi::Env env, duckdb::DataChunk &chunk, bool with_types, bool with_data);

} // namespace node_duckdb
