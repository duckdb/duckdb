
#ifndef NODE_DUCKDB_SRC_DATABASE_H
#define NODE_DUCKDB_SRC_DATABASE_H


#include <assert.h>
#include <string>
#include <queue>

#include "duckdb.hpp"
#include <napi.h>

//#include "async.h"

using namespace Napi;

namespace node_duckdb {

class Database;


class Database : public Napi::ObjectWrap<Database> {
public:
#if NAPI_VERSION < 6
    static Napi::FunctionReference constructor;
#endif
    static Napi::Object Init(Napi::Env env, Napi::Object exports);

    static inline bool HasInstance(Napi::Value val) {
        Napi::Env env = val.Env();
        Napi::HandleScope scope(env);
        if (!val.IsObject()) return false;
        Napi::Object obj = val.As<Napi::Object>();
#if NAPI_VERSION < 6
        return obj.InstanceOf(constructor.Value());
#else
        Napi::FunctionReference* constructor =
            env.GetInstanceData<Napi::FunctionReference>();
        return obj.InstanceOf(constructor->Value());
#endif
    }

    struct Baton {
        napi_async_work request = NULL;
        Database* db;
        Napi::FunctionReference callback;
        int status;
        std::string message;

        Baton(Database* db_, Napi::Function cb_) :
                db(db_), status(0) {
            db->Ref();
            if (!cb_.IsUndefined() && cb_.IsFunction()) {
                callback.Reset(cb_, 1);
            }
        }
        virtual ~Baton() {
            if (request) napi_delete_async_work(db->Env(), request);
            db->Unref();
            callback.Reset();
        }
    };

    struct OpenBaton : Baton {
        std::string filename;
        int mode;
        OpenBaton(Database* db_, Napi::Function cb_, const char* filename_, int mode_) :
            Baton(db_, cb_), filename(filename_), mode(mode_) {}
    };

    struct ExecBaton : Baton {
        std::string sql;
        ExecBaton(Database* db_, Napi::Function cb_, const char* sql_) :
            Baton(db_, cb_), sql(sql_) {}
    };


    typedef void (*Work_Callback)(Baton* baton);

    struct Call {
        Call(Work_Callback cb_, Baton* baton_, bool exclusive_ = false) :
            callback(cb_), exclusive(exclusive_), baton(baton_) {};
        Work_Callback callback;
        bool exclusive;
        Baton* baton;
    };

    bool IsOpen() { return open; }
    bool IsLocked() { return locked; }

    friend class Statement;

    void init() {
        open = false;
        closing = false;
        locked = false;
        pending = 0;
        serialize = false;
    }

    Database(const Napi::CallbackInfo& info);

    ~Database() {
        RemoveCallbacks();
        _conn_handle.reset();
        _db_handle.reset();
        open = false;
    }

protected:
    static void Work_BeginOpen(Baton* baton);
    static void Work_Open(napi_env env, void* data);
    static void Work_AfterOpen(napi_env env, napi_status status, void* data);

    Napi::Value OpenGetter(const Napi::CallbackInfo& info);

    void Schedule(Work_Callback callback, Baton* baton, bool exclusive = false);
    void Process();

    Napi::Value Exec(const Napi::CallbackInfo& info);
    static void Work_BeginExec(Baton* baton);
    static void Work_Exec(napi_env env, void* data);
    static void Work_AfterExec(napi_env env, napi_status status, void* data);

    Napi::Value Wait(const Napi::CallbackInfo& info);
    static void Work_Wait(Baton* baton);

    Napi::Value Close(const Napi::CallbackInfo& info);
    static void Work_BeginClose(Baton* baton);
    static void Work_Close(napi_env env, void* data);
    static void Work_AfterClose(napi_env env, napi_status status, void* data);
	
    Napi::Value Serialize(const Napi::CallbackInfo& info);
    Napi::Value Parallelize(const Napi::CallbackInfo& info);

    Napi::Value Configure(const Napi::CallbackInfo& info);

    Napi::Value Interrupt(const Napi::CallbackInfo& info);

    static void SetBusyTimeout(Baton* baton);

    void RemoveCallbacks();

protected:
    std::unique_ptr<duckdb::DuckDB> _db_handle;
    std::unique_ptr<duckdb::Connection> _conn_handle;

    bool open;
    bool closing;
    bool locked;
    unsigned int pending;

    bool serialize;

    std::queue<Call*> queue;
};

}

#endif // NODE_DUCKDB_SRC_DATABASE_H
