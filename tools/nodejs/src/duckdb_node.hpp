#pragma once
#include <napi.h>
#include "duckdb.hpp"

namespace node_duckdb {


class Database;

class Connection {
public:
    Connection(Database& database_ref_) : database_ref(database_ref_) {}

public:
    constexpr static int DUCKDB_NODEJS_ERROR = -1;
    constexpr static int DUCKDB_NODEJS_READONLY = 1;
private:
    Database& database_ref;
    std::unique_ptr<duckdb::Connection> connection;
};



struct Task {
    virtual void Run(Napi::Env env) = 0;
    virtual ~Task() {};
};



class Database : public Napi::ObjectWrap<Database> {


public:

	Database(const Napi::CallbackInfo& info);
    static Napi::Object Init(Napi::Env env, Napi::Object exports);
    void Process(Napi::Env env);

private:
    Napi::Value Exec(const Napi::CallbackInfo& info);
    Napi::Value Wait(const Napi::CallbackInfo& info);
    Napi::Value Serialize(const Napi::CallbackInfo& info);
    Napi::Value Parallelize(const Napi::CallbackInfo& info);
    Napi::Value Interrupt(const Napi::CallbackInfo& info);
    Napi::Value OpenGetter(const Napi::CallbackInfo& info);
    Napi::Value Close(const Napi::CallbackInfo& info);
	void Schedule(Napi::Env env, unique_ptr<Task> task);

public:
    constexpr static int DUCKDB_NODEJS_ERROR = -1;
    constexpr static int DUCKDB_NODEJS_READONLY = 1;
	std::unique_ptr<Connection> default_connection;
    std::unique_ptr<duckdb::DuckDB> database;
    std::queue<unique_ptr<Task>> task_queue;

};

}