#include <string.h>
#include <napi.h>

#include "macros.h"
#include "database.h"
#include "statement.h"

using namespace node_duckdb;

#if NAPI_VERSION < 6
Napi::FunctionReference Database::constructor;
#endif

Napi::Object Database::Init(Napi::Env env, Napi::Object exports) {
    Napi::HandleScope scope(env);

    Napi::Function t = DefineClass(env, "Database", {
        InstanceMethod("close", &Database::Close),
        InstanceMethod("exec", &Database::Exec),
        InstanceMethod("wait", &Database::Wait),
        InstanceMethod("serialize", &Database::Serialize),
        InstanceMethod("parallelize", &Database::Parallelize),
        InstanceMethod("configure", &Database::Configure),
        InstanceMethod("interrupt", &Database::Interrupt),
        InstanceAccessor("open", &Database::OpenGetter, nullptr)
    });

#if NAPI_VERSION < 6
    constructor = Napi::Persistent(t);
    constructor.SuppressDestruct();
#else
    Napi::FunctionReference* constructor = new Napi::FunctionReference();
    *constructor = Napi::Persistent(t);
    env.SetInstanceData<Napi::FunctionReference>(constructor);
#endif

    exports.Set("Database", t);
    return exports;
}

void Database::Process() {
    Napi::Env env = this->Env();
    Napi::HandleScope scope(env);

    if (!open && locked && !queue.empty()) {
        EXCEPTION(Napi::String::New(env, "Database handle is closed"), -1, exception);
        Napi::Value argv[] = { exception };
        bool called = false;

        // Call all callbacks with the error object.
        while (!queue.empty()) {
            std::unique_ptr<Call> call(queue.front());
            queue.pop();
            std::unique_ptr<Baton> baton(call->baton);
            Napi::Function cb = baton->callback.Value();
            if (!cb.IsUndefined() && cb.IsFunction()) {
                TRY_CATCH_CALL(this->Value(), cb, 1, argv);
                called = true;
            }
        }

        // When we couldn't call a callback function, emit an error on the
        // Database object.
        if (!called) {
            Napi::Value info[] = { Napi::String::New(env, "error"), exception };
            EMIT_EVENT(Value(), 2, info);
        }
        return;
    }

    while (open && (!locked || pending == 0) && !queue.empty()) {
        Call *c = queue.front();

        if (c->exclusive && pending > 0) {
            break;
        }

        queue.pop();
        std::unique_ptr<Call> call(c);
        locked = call->exclusive;
        call->callback(call->baton);

        if (locked) break;
    }
}

void Database::Schedule(Work_Callback callback, Baton* baton, bool exclusive) {
    Napi::Env env = this->Env();
    Napi::HandleScope scope(env);

    if (!open && locked) {
        EXCEPTION(Napi::String::New(env, "Database is closed"), -1, exception);
        Napi::Function cb = baton->callback.Value();
        // We don't call the actual callback, so we have to make sure that
        // the baton gets destroyed.
        delete baton;
        if (!cb.IsUndefined() && cb.IsFunction()) {
            Napi::Value argv[] = { exception };
            TRY_CATCH_CALL(Value(), cb, 1, argv);
        }
        else {
            Napi::Value argv[] = { Napi::String::New(env, "error"), exception };
            EMIT_EVENT(Value(), 2, argv);
        }
        return;
    }

    if (!open || ((locked || exclusive || serialize) && pending > 0)) {
        queue.push(new Call(callback, baton, exclusive || serialize));
    }
    else {
        locked = exclusive;
        callback(baton);
    }
}

Database::Database(const Napi::CallbackInfo& info) : Napi::ObjectWrap<Database>(info) {
    init();
    Napi::Env env = info.Env();

    if (info.Length() <= 0 || !info[0].IsString()) {
        Napi::TypeError::New(env, "String expected").ThrowAsJavaScriptException();
        return;
    }
    std::string filename = info[0].As<Napi::String>();

    unsigned int pos = 1;

//    int mode;
//    if (info.Length() >= pos && info[pos].IsNumber() && OtherIsInt(info[pos].As<Napi::Number>())) {
//        mode = info[pos++].As<Napi::Number>().Int32Value();
//    }
//    else {
//		// FIXME
//       // mode = SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE | SQLITE_OPEN_FULLMUTEX;
//    }

    Napi::Function callback;
    if (info.Length() >= pos && info[pos].IsFunction()) {
        callback = info[pos++].As<Napi::Function>();
    }

    info.This().As<Napi::Object>().DefineProperty(Napi::PropertyDescriptor::Value("filename", info[0].As<Napi::String>(), napi_default));
    //info.This().As<Napi::Object>().DefineProperty(Napi::PropertyDescriptor::Value("mode", Napi::Number::New(env, mode), napi_default));

    // Start opening the database.
    OpenBaton* baton = new OpenBaton(this, callback, filename.c_str(), 0);
    Work_BeginOpen(baton);
}

void Database::Work_BeginOpen(Baton* baton) {
    Napi::Env env = baton->db->Env();
    int status = napi_create_async_work(
        env, NULL, Napi::String::New(env, "duckdb.Database.Open"),
        Work_Open, Work_AfterOpen, baton, &baton->request
    );
    assert(status == 0);
    napi_queue_async_work(env, baton->request);
}

void Database::Work_Open(napi_env e, void* data) {
    OpenBaton* baton = static_cast<OpenBaton*>(data);
    Database* db = baton->db;

	try {
		db->_db_handle = make_unique<duckdb::DuckDB>(baton->filename);
        db->_conn_handle = make_unique<duckdb::Connection>(*db->_db_handle);
    } catch (...) {
		baton->status = -1;
		baton->message = "Failed to open database";
	}
}

void Database::Work_AfterOpen(napi_env e, napi_status status, void* data) {
    std::unique_ptr<OpenBaton> baton(static_cast<OpenBaton*>(data));

    Database* db = baton->db;

    Napi::Env env = db->Env();
    Napi::HandleScope scope(env);

    Napi::Value argv[1];
    if (baton->status != 0) {
        EXCEPTION(Napi::String::New(env, baton->message.c_str()), baton->status, exception);
        argv[0] = exception;
    }
    else {
        db->open = true;
        argv[0] = env.Null();
    }

    Napi::Function cb = baton->callback.Value();

    if (!cb.IsUndefined() && cb.IsFunction()) {
        TRY_CATCH_CALL(db->Value(), cb, 1, argv);
    }
    else if (!db->open) {
        Napi::Value info[] = { Napi::String::New(env, "error"), argv[0] };
        EMIT_EVENT(db->Value(), 2, info);
    }

    if (db->open) {
        Napi::Value info[] = { Napi::String::New(env, "open") };
        EMIT_EVENT(db->Value(), 1, info);
        db->Process();
    }
}

Napi::Value Database::OpenGetter(const Napi::CallbackInfo& info) {
    Napi::Env env = this->Env();
    Database* db = this;
    return Napi::Boolean::New(env, db->open);
}

Napi::Value Database::Close(const Napi::CallbackInfo& info) {
    Napi::Env env = info.Env();
    Database* db = this;
    OPTIONAL_ARGUMENT_FUNCTION(0, callback);

    Baton* baton = new Baton(db, callback);
    db->Schedule(Work_BeginClose, baton, true);

    return info.This();
}

void Database::Work_BeginClose(Baton* baton) {
    assert(baton->db->locked);
    assert(baton->db->open);
    assert(baton->db->_db_handle);
    assert(baton->db->_conn_handle);

    assert(baton->db->pending == 0);

    baton->db->pending++;
    baton->db->RemoveCallbacks();
    baton->db->closing = true;

    Napi::Env env = baton->db->Env();

    int status = napi_create_async_work(
        env, NULL, Napi::String::New(env, "duckdb.Database.Close"),
        Work_Close, Work_AfterClose, baton, &baton->request
    );
    assert(status == 0);
    napi_queue_async_work(env, baton->request);
}

void Database::Work_Close(napi_env e, void* data) {
    Baton* baton = static_cast<Baton*>(data);
    Database* db = baton->db;

	db->_conn_handle.reset();
    db->_db_handle.reset();

}

void Database::Work_AfterClose(napi_env e, napi_status status, void* data) {
    std::unique_ptr<Baton> baton(static_cast<Baton*>(data));

    Database* db = baton->db;

    Napi::Env env = db->Env();
    Napi::HandleScope scope(env);

    db->pending--;
    db->closing = false;

    Napi::Value argv[1];
    if (baton->status != 0) {
        EXCEPTION(Napi::String::New(env, baton->message.c_str()), baton->status, exception);
        argv[0] = exception;
    }
    else {
        db->open = false;
        // Leave db->locked to indicate that this db object has reached
        // the end of its life.
        argv[0] = env.Null();
    }

    Napi::Function cb = baton->callback.Value();

    // Fire callbacks.
    if (!cb.IsUndefined() && cb.IsFunction()) {
        TRY_CATCH_CALL(db->Value(), cb, 1, argv);
    }
    else if (db->open) {
        Napi::Value info[] = { Napi::String::New(env, "error"), argv[0] };
        EMIT_EVENT(db->Value(), 2, info);
    }

    if (!db->open) {
        Napi::Value info[] = { Napi::String::New(env, "close"), argv[0] };
        EMIT_EVENT(db->Value(), 1, info);
        db->Process();
    }
}

Napi::Value Database::Serialize(const Napi::CallbackInfo& info) {
    Napi::Env env = this->Env();
    Database* db = this;
    OPTIONAL_ARGUMENT_FUNCTION(0, callback);

    bool before = db->serialize;
    db->serialize = true;

    if (!callback.IsEmpty() && callback.IsFunction()) {
        TRY_CATCH_CALL(info.This(), callback, 0, NULL, info.This());
        db->serialize = before;
    }

    db->Process();

    return info.This();
}

Napi::Value Database::Parallelize(const Napi::CallbackInfo& info) {
    Napi::Env env = this->Env();
    Database* db = this;
    OPTIONAL_ARGUMENT_FUNCTION(0, callback);

    bool before = db->serialize;
    db->serialize = false;

    if (!callback.IsEmpty() && callback.IsFunction()) {
        TRY_CATCH_CALL(info.This(), callback, 0, NULL, info.This());
        db->serialize = before;
    }

    db->Process();

    return info.This();
}

Napi::Value Database::Configure(const Napi::CallbackInfo& info) {
    Napi::Env env = this->Env();
    Database* db = this;

    REQUIRE_ARGUMENTS(2);
// TODO read_only
	/*
    if (info[0].StrictEquals( Napi::String::New(env, "trace"))) {
        Napi::Function handle;
        Baton* baton = new Baton(db, handle);
        db->Schedule(RegisterTraceCallback, baton);
    }
    else if (info[0].StrictEquals( Napi::String::New(env, "profile"))) {
        Napi::Function handle;
        Baton* baton = new Baton(db, handle);
        db->Schedule(RegisterProfileCallback, baton);
    }
    else if (info[0].StrictEquals( Napi::String::New(env, "busyTimeout"))) {
        if (!info[1].IsNumber()) {
            Napi::TypeError::New(env, "Value must be an integer").ThrowAsJavaScriptException();
            return env.Null();
        }
        Napi::Function handle;
        Baton* baton = new Baton(db, handle);
        baton->status = info[1].As<Napi::Number>().Int32Value();
        db->Schedule(SetBusyTimeout, baton);
    }
    else {
        Napi::TypeError::New(env, (StringConcat(
#if V8_MAJOR_VERSION > 6
            info.GetIsolate(),
#endif
            info[0].As<Napi::String>(),
            Napi::String::New(env, " is not a valid configuration option")
        )).Utf8Value().c_str()).ThrowAsJavaScriptException();
        return env.Null();
    }
*/
    db->Process();

    return info.This();
}

Napi::Value Database::Interrupt(const Napi::CallbackInfo& info) {
    Napi::Env env = this->Env();
    Database* db = this;

    if (!db->open) {
        Napi::Error::New(env, "Database is not open").ThrowAsJavaScriptException();
        return env.Null();
    }

    if (db->closing) {
        Napi::Error::New(env, "Database is closing").ThrowAsJavaScriptException();
        return env.Null();
    }

	// FIXME
    //sqlite3_interrupt(db->_handle);
    return info.This();
}

Napi::Value Database::Exec(const Napi::CallbackInfo& info) {
    Napi::Env env = this->Env();
    Database* db = this;

    REQUIRE_ARGUMENT_STRING(0, sql);
    OPTIONAL_ARGUMENT_FUNCTION(1, callback);

    Baton* baton = new ExecBaton(db, callback, sql.c_str());
    db->Schedule(Work_BeginExec, baton, true);

    return info.This();
}

void Database::Work_BeginExec(Baton* baton) {
    assert(baton->db->locked);
    assert(baton->db->open);
    assert(baton->db->_db_handle);
    assert(baton->db->_conn_handle);

    assert(baton->db->pending == 0);
    baton->db->pending++;
    Napi::Env env = baton->db->Env();
    int status = napi_create_async_work(
        env, NULL, Napi::String::New(env, "duckdb.Database.Exec"),
        Work_Exec, Work_AfterExec, baton, &baton->request
    );
    assert(status == 0);
    napi_queue_async_work(env, baton->request);
}

void Database::Work_Exec(napi_env e, void* data) {
    ExecBaton* baton = static_cast<ExecBaton*>(data);
	auto res = baton->db->_conn_handle->Query(baton->sql);
	if (!res->success) {
        baton->message = res->error;
		baton->status = -1;
	}
}

void Database::Work_AfterExec(napi_env e, napi_status status, void* data) {
    std::unique_ptr<ExecBaton> baton(static_cast<ExecBaton*>(data));

    Database* db = baton->db;
    db->pending--;

    Napi::Env env = db->Env();
    Napi::HandleScope scope(env);

    Napi::Function cb = baton->callback.Value();

	// FIXME this 0 is ugly
    if (baton->status != 0) {
        EXCEPTION(Napi::String::New(env, baton->message.c_str()), baton->status, exception);

        if (!cb.IsUndefined() && cb.IsFunction()) {
            Napi::Value argv[] = { exception };
            TRY_CATCH_CALL(db->Value(), cb, 1, argv);
        }
        else {
            Napi::Value info[] = { Napi::String::New(env, "error"), exception };
            EMIT_EVENT(db->Value(), 2, info);
        }
    }
    else if (!cb.IsUndefined() && cb.IsFunction()) {
        Napi::Value argv[] = { env.Null() };
        TRY_CATCH_CALL(db->Value(), cb, 1, argv);
    }

    db->Process();
}

Napi::Value Database::Wait(const Napi::CallbackInfo& info) {
    Napi::Env env = info.Env();
    Database* db = this;

    OPTIONAL_ARGUMENT_FUNCTION(0, callback);

    Baton* baton = new Baton(db, callback);
    db->Schedule(Work_Wait, baton, true);

    return info.This();
}

void Database::Work_Wait(Baton* b) {
    std::unique_ptr<Baton> baton(b);

    Napi::Env env = baton->db->Env();
    Napi::HandleScope scope(env);

    assert(baton->db->locked);
    assert(baton->db->open);
    assert(baton->db->_db_handle);
    assert(baton->db->_conn_handle);

    assert(baton->db->pending == 0);

    Napi::Function cb = baton->callback.Value();
    if (!cb.IsUndefined() && cb.IsFunction()) {
        Napi::Value argv[] = { env.Null() };
        TRY_CATCH_CALL(baton->db->Value(), cb, 1, argv);
    }

    baton->db->Process();
}



void Database::RemoveCallbacks() {

}
