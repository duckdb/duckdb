#ifndef NODE_SQLITE3_SRC_STATEMENT_H
#define NODE_SQLITE3_SRC_STATEMENT_H

#include <cstdlib>
#include <cstring>
#include <string>
#include <queue>
#include <vector>

#include <napi.h>
#include <uv.h>

#include "database.h"

using namespace Napi;

namespace node_duckdb {


typedef std::vector<duckdb::Value> Row;
typedef std::vector<Row*> Rows;
typedef Row Parameters;


class Statement : public Napi::ObjectWrap<Statement> {
public:
    static Napi::Object Init(Napi::Env env, Napi::Object exports);
    static Napi::Value New(const Napi::CallbackInfo& info);

    struct Baton {
        napi_async_work request = NULL;
        Statement* stmt;
        Napi::FunctionReference callback;
        Parameters parameters;

        Baton(Statement* stmt_, Napi::Function cb_) : stmt(stmt_) {
            stmt->Ref();
            callback.Reset(cb_, 1);
        }
        virtual ~Baton() {
            if (request) napi_delete_async_work(stmt->Env(), request);
            stmt->Unref();
            callback.Reset();
        }
    };

    struct RowBaton : Baton {
        RowBaton(Statement* stmt_, Napi::Function cb_) :
            Baton(stmt_, cb_) {}
        Row row;
    };

    struct RunBaton : Baton {
        RunBaton(Statement* stmt_, Napi::Function cb_) :
            Baton(stmt_, cb_), inserted_id(0), changes(0) {}
        duckdb::idx_t inserted_id;
        int changes;
    };

    struct RowsBaton : Baton {
        RowsBaton(Statement* stmt_, Napi::Function cb_) :
            Baton(stmt_, cb_) {}
        Rows rows;
    };

    struct Async;

    struct EachBaton : Baton {
        Napi::FunctionReference completed;
        Async* async; // Isn't deleted when the baton is deleted.

        EachBaton(Statement* stmt_, Napi::Function cb_) :
            Baton(stmt_, cb_) {}
        virtual ~EachBaton() {
            completed.Reset();
        }
    };

    struct PrepareBaton : Database::Baton {
        Statement* stmt;
        std::string sql;
        PrepareBaton(Database* db_, Napi::Function cb_, Statement* stmt_) :
            Baton(db_, cb_), stmt(stmt_) {
            stmt->Ref();
        }
        virtual ~PrepareBaton() {
            stmt->Unref();
            if (!db->IsOpen() && db->IsLocked()) {
                // The database handle was closed before the statement could be
                // prepared.
                stmt->Finalize_();
            }
        }
    };

    typedef void (*Work_Callback)(Baton* baton);

    struct Call {
        Call(Work_Callback cb_, Baton* baton_) : callback(cb_), baton(baton_) {};
        Work_Callback callback;
        Baton* baton;
    };

    struct Async {
        uv_async_t watcher;
        Statement* stmt;
        Rows data;
        std::mutex mutex;
		bool completed;
        int retrieved;

        // Store the callbacks here because we don't have
        // access to the baton in the async callback.
        Napi::FunctionReference item_cb;
        Napi::FunctionReference completed_cb;

        Async(Statement* st, uv_async_cb async_cb) :
                stmt(st), completed(false), retrieved(0) {
            watcher.data = this;
            stmt->Ref();
            uv_loop_t *loop;
            napi_get_uv_event_loop(stmt->Env(), &loop);
            uv_async_init(loop, &watcher, async_cb);
        }

        ~Async() {
            stmt->Unref();
            item_cb.Reset();
            completed_cb.Reset();
        }
    };

    void init(Database* db_) {
        db = db_;
        status = 0;
        prepared = false;
        locked = true;
        finalized = false;
        db->Ref();
    }

    Statement(const Napi::CallbackInfo& info);

    ~Statement() {
        if (!finalized) Finalize_();
    }

    WORK_DEFINITION(Bind);
    WORK_DEFINITION(Get);
    WORK_DEFINITION(Run);
    WORK_DEFINITION(All);
    WORK_DEFINITION(Each);
    WORK_DEFINITION(Reset);

    Napi::Value Finalize_(const Napi::CallbackInfo& info);

protected:
    static void Work_BeginPrepare(Database::Baton* baton);
    static void Work_Prepare(napi_env env, void* data);
    static void Work_AfterPrepare(napi_env env, napi_status status, void* data);

    static void AsyncEach(uv_async_t* handle);
    static void CloseCallback(uv_handle_t* handle);

    static void Finalize_(Baton* baton);
    void Finalize_();

    template <class T> inline duckdb::Value BindParameter(const Napi::Value source, T pos);
    template <class T> T* Bind(const Napi::CallbackInfo& info, int start = 0, int end = -1);
    bool Bind(const Parameters &parameters);

    static void GetRow(Row* row, duckdb::PreparedStatement* stmt);
    static Napi::Value RowToJS(Napi::Env env, Row* row);
    void Schedule(Work_Callback callback, Baton* baton);
    void Process();
    void CleanQueue();
    template <class T> static void Error(T* baton);

protected:
    Database* db;

    std::unique_ptr<duckdb::PreparedStatement> _stmt_handle;
    std::unique_ptr<duckdb::QueryResult> _res_handle;
    std::unique_ptr<duckdb::DataChunk> _chunk_handle;

	duckdb::idx_t chunk_offset = 0;
    int status;
    std::string message;

    bool prepared;
    bool locked;
    bool finalized;
    std::queue<Call*> queue;
};

}

#endif
