#include <string.h>
#include <napi.h>
#include <uv.h>

#include "macros.h"
#include "database.h"
#include "statement.h"

using namespace node_duckdb;

Napi::Object Statement::Init(Napi::Env env, Napi::Object exports) {
    Napi::HandleScope scope(env);

    Napi::Function t = DefineClass(env, "Statement", {
      InstanceMethod("bind", &Statement::Bind),
      InstanceMethod("get", &Statement::Get),
      InstanceMethod("run", &Statement::Run),
      InstanceMethod("all", &Statement::All),
      InstanceMethod("each", &Statement::Each),
      InstanceMethod("reset", &Statement::Reset),
      InstanceMethod("finalize", &Statement::Finalize_),
    });

    exports.Set("Statement", t);
    return exports;
}

// A Napi InstanceOf for Javascript Objects "Date" and "RegExp"
bool OtherInstanceOf(Napi::Object source, const char* object_type) {
    if (strncmp(object_type, "Date", 4) == 0) {
        return source.InstanceOf(source.Env().Global().Get("Date").As<Function>());
    } else if (strncmp(object_type, "RegExp", 6) == 0) {
        return source.InstanceOf(source.Env().Global().Get("RegExp").As<Function>());
    }

    return false;
}

void Statement::Process() {
    if (finalized && !queue.empty()) {
        return CleanQueue();
    }

    while (prepared && !locked && !queue.empty()) {
        std::unique_ptr<Call> call(queue.front());
        queue.pop();

        call->callback(call->baton);
    }
}

void Statement::Schedule(Work_Callback callback, Baton* baton) {
    if (finalized) {
        queue.push(new Call(callback, baton));
        CleanQueue();
    }
    else if (!prepared || locked) {
        queue.push(new Call(callback, baton));
    }
    else {
        callback(baton);
    }
}

template <class T> void Statement::Error(T* baton) {
    Statement* stmt = baton->stmt;

    Napi::Env env = stmt->Env();
    Napi::HandleScope scope(env);

    // Fail hard on logic errors.
    assert(stmt->status);
    EXCEPTION(Napi::String::New(env, stmt->message.c_str()), exception);

    Napi::Function cb = baton->callback.Value();

    if (!cb.IsUndefined() && cb.IsFunction()) {
        Napi::Value argv[] = { exception };
        TRY_CATCH_CALL(stmt->Value(), cb, 1, argv);
    }
    else {
        Napi::Value argv[] = { Napi::String::New(env, "error"), exception };
        EMIT_EVENT(stmt->Value(), 2, argv);
    }
}

// { Database db, String sql, Array params, Function callback }
Statement::Statement(const Napi::CallbackInfo& info) : Napi::ObjectWrap<Statement>(info) {
    Napi::Env env = info.Env();
    int length = info.Length();

    if (length <= 0 || !Database::HasInstance(info[0])) {
        Napi::TypeError::New(env, "Database object expected").ThrowAsJavaScriptException();
        return;
    }
    else if (length <= 1 || !info[1].IsString()) {
        Napi::TypeError::New(env, "SQL query expected").ThrowAsJavaScriptException();
        return;
    }
    else if (length > 2 && !info[2].IsUndefined() && !info[2].IsFunction()) {
        Napi::TypeError::New(env, "Callback expected").ThrowAsJavaScriptException();
        return;
    }

    Database* db = Napi::ObjectWrap<Database>::Unwrap(info[0].As<Napi::Object>());
    Napi::String sql = info[1].As<Napi::String>();

    info.This().As<Napi::Object>().DefineProperty(Napi::PropertyDescriptor::Value("sql", sql, napi_default));

    init(db);
    Statement* stmt = this;

    PrepareBaton* baton = new PrepareBaton(db, info[2].As<Napi::Function>(), stmt);
    baton->sql = std::string(sql.As<Napi::String>().Utf8Value().c_str());
    db->Schedule(Work_BeginPrepare, baton);
}

void Statement::Work_BeginPrepare(Database::Baton* baton) {
    assert(baton->db->open);
    baton->db->pending++;
    Napi::Env env = baton->db->Env();
    int status = napi_create_async_work(
        env, NULL, Napi::String::New(env, "duckdb.Statement.Prepare"),
        Work_Prepare, Work_AfterPrepare, baton, &baton->request
    );
    assert(status == 0);
    napi_queue_async_work(env, baton->request);
}

void Statement::Work_Prepare(napi_env e, void* data) {
    STATEMENT_INIT(PrepareBaton);
	stmt->_stmt_handle = baton->db->_conn_handle->Prepare(baton->sql);
    stmt->_res_handle.reset();
	if (!stmt->_stmt_handle->success) {
        stmt->message =stmt->_stmt_handle->error;
		stmt->_stmt_handle.reset();
		stmt->status = -1;
	}
}

void Statement::Work_AfterPrepare(napi_env e, napi_status status, void* data) {
    std::unique_ptr<PrepareBaton> baton(static_cast<PrepareBaton*>(data));
    Statement* stmt = baton->stmt;

    Napi::Env env = stmt->Env();
    Napi::HandleScope scope(env);

    if (stmt->status != 0) {
        Error(baton.get());
        stmt->Finalize_();
    }
    else {
        stmt->prepared = true;
        if (!baton->callback.IsEmpty() && baton->callback.Value().IsFunction()) {
            Napi::Function cb = baton->callback.Value();
            Napi::Value argv[] = { env.Null() };
            TRY_CATCH_CALL(stmt->Value(), cb, 1, argv);
        }
    }

    STATEMENT_END();
}

template <class T> duckdb::Value
                   Statement::BindParameter(const Napi::Value source, T pos) {
    if (source.IsString()) {
        return duckdb::Value(source.As<Napi::String>().Utf8Value());
    }
    else if (OtherInstanceOf(source.As<Object>(), "RegExp")) {
        return duckdb::Value(source.ToString().Utf8Value());
    }
    else if (source.IsNumber()) {
        if (OtherIsInt(source.As<Napi::Number>())) {
			return duckdb::Value::INTEGER(source.As<Napi::Number>().Int32Value());
        } else {
			return duckdb::Value::DOUBLE(source.As<Napi::Number>().DoubleValue());
        }
    }
    else if (source.IsBoolean()) {
		return duckdb::Value::BOOLEAN(source.As<Napi::Boolean>().Value());
    }
    else if (source.IsNull()) {
		return duckdb::Value();
    }
    else if (source.IsBuffer()) {
        Napi::Buffer<char> buffer = source.As<Napi::Buffer<char>>();
        return duckdb::Value::BLOB(string(buffer.Data(), buffer.Length()));
    }
    else if (OtherInstanceOf(source.As<Object>(), "Date")) {
		// FIXME
        // return new Values::Float(pos, source.ToNumber().DoubleValue());
    }
    else if (source.IsObject()) {
		return duckdb::Value(source.ToString().Utf8Value());
    }
    return duckdb::Value();
}

template <class T> T* Statement::Bind(const Napi::CallbackInfo& info, int start, int last) {
    Napi::Env env = info.Env();
    Napi::HandleScope scope(env);

    if (last < 0) last = info.Length();
    Napi::Function callback;
    if (last > start && info[last - 1].IsFunction()) {
        callback = info[last - 1].As<Napi::Function>();
        last--;
    }

    T* baton = new T(this, callback);

    if (start < last) {
        if (info[start].IsArray()) {
            Napi::Array array = info[start].As<Napi::Array>();
            int length = array.Length();
            // Note: bind parameters start with 1.
            for (int i = 0, pos = 1; i < length; i++, pos++) {
                baton->parameters.push_back(BindParameter((array).Get(i), pos));
            }
        }
        else if (!info[start].IsObject() || OtherInstanceOf(info[start].As<Object>(), "RegExp") || OtherInstanceOf(info[start].As<Object>(), "Date") || info[start].IsBuffer()) {
            // Parameters directly in array.
            // Note: bind parameters start with 1.
            for (int i = start, pos = 1; i < last; i++, pos++) {
                baton->parameters.push_back(BindParameter(info[i], pos));
            }
        }
        else if (info[start].IsObject()) {
            Napi::Object object = info[start].As<Napi::Object>();
            Napi::Array array = object.GetPropertyNames();
            int length = array.Length();
            for (int i = 0; i < length; i++) {
                Napi::Value name = (array).Get(i);
                Napi::Number num = name.ToNumber();

                if (num.Int32Value() == num.DoubleValue()) {
                    baton->parameters.push_back(
                        BindParameter((object).Get(name), num.Int32Value()));
                }
                else {
                    baton->parameters.push_back(BindParameter((object).Get(name),
                        name.As<Napi::String>().Utf8Value().c_str()));
                }
            }
        }
        else {
            return NULL;
        }
    }

    return baton;
}

bool Statement::Bind(const Parameters & parameters) {
    if (parameters.size() == 0) {
        return true;
    }

	_res_handle = _stmt_handle->Execute(parameters);
	if (!_res_handle->success) {
        _res_handle.reset();
		return false;
	}

    return true;
}

Napi::Value Statement::Bind(const Napi::CallbackInfo& info) {
    Napi::Env env = info.Env();
    Statement* stmt = this;

    Baton* baton = stmt->Bind<Baton>(info);
    if (baton == NULL) {
        Napi::TypeError::New(env, "Data type is not supported").ThrowAsJavaScriptException();
        return env.Null();
    }
    else {
        stmt->Schedule(Work_BeginBind, baton);
        return info.This();
    }
}

void Statement::Work_BeginBind(Baton* baton) {
    STATEMENT_BEGIN(Bind);
}

void Statement::Work_Bind(napi_env e, void* data) {
    STATEMENT_INIT(Baton);
	printf("work_bind()\n");
	stmt->Bind(baton->parameters);
}

void Statement::Work_AfterBind(napi_env e, napi_status status, void* data) {
    std::unique_ptr<Baton> baton(static_cast<Baton*>(data));
    Statement* stmt = baton->stmt;

    Napi::Env env = stmt->Env();
    Napi::HandleScope scope(env);

    if (stmt->status != 0) {
        Error(baton.get());
    }
    else {
        // Fire callbacks.
        Napi::Function cb = baton->callback.Value();
        if (!cb.IsUndefined() && cb.IsFunction()) {
            Napi::Value argv[] = { env.Null() };
            TRY_CATCH_CALL(stmt->Value(), cb, 1, argv);
        }
    }

    STATEMENT_END();
}



Napi::Value Statement::Get(const Napi::CallbackInfo& info) {
    Napi::Env env = info.Env();
    Statement* stmt = this;

    Baton* baton = stmt->Bind<RowBaton>(info);
    if (baton == NULL) {
        Napi::Error::New(env, "Data type is not supported").ThrowAsJavaScriptException();
        return env.Null();
    }
    else {
        stmt->Schedule(Work_BeginGet, baton);
        return info.This();
    }
}

void Statement::Work_BeginGet(Baton* baton) {
    STATEMENT_BEGIN(Get);
}

void Statement::Work_Get(napi_env e, void* data) {
    STATEMENT_INIT(RowBaton);

	assert(0);

	if (!stmt->_chunk_handle || stmt->chunk_offset > stmt->_chunk_handle->size()) {
        stmt->_chunk_handle = stmt->_res_handle->Fetch();
		stmt->chunk_offset = 0;
    }

	// TODO error handling

   // GetRow(&baton->row, stmt->_stmt_handle);

/* FIXME
    if (stmt->status != SQLITE_DONE || baton->parameters.size()) {

        if (stmt->Bind(baton->parameters)) {
            stmt->status = sqlite3_step(stmt->_handle);

            if (!(stmt->status == SQLITE_ROW || stmt->status == SQLITE_DONE)) {
                stmt->message = std::string(sqlite3_errmsg(stmt->db->_handle));
            }
        }
        if (stmt->status == SQLITE_ROW) {
            // Acquire one result row before returning.
            GetRow(&baton->row, stmt->_handle);
        }
    }
     */
}

void Statement::Work_AfterGet(napi_env e, napi_status status, void* data) {
    std::unique_ptr<RowBaton> baton(static_cast<RowBaton*>(data));
    Statement* stmt = baton->stmt;

    Napi::Env env = stmt->Env();
    Napi::HandleScope scope(env);

	assert(0);

	/* FIXME
    if (stmt->status != SQLITE_ROW && stmt->status != SQLITE_DONE) {
        Error(baton.get());
    }
    else {
        // Fire callbacks.
        Napi::Function cb = baton->callback.Value();
        if (!cb.IsUndefined() && cb.IsFunction()) {
            if (stmt->status == SQLITE_ROW) {
                // Create the result array from the data we acquired.
                Napi::Value argv[] = { env.Null(), RowToJS(env, &baton->row) };
                TRY_CATCH_CALL(stmt->Value(), cb, 2, argv);
            }
            else {
                Napi::Value argv[] = { env.Null() };
                TRY_CATCH_CALL(stmt->Value(), cb, 1, argv);
            }
        }
    } */

    STATEMENT_END();
}

Napi::Value Statement::Run(const Napi::CallbackInfo& info) {
    Napi::Env env = info.Env();
    Statement* stmt = this;

    Baton* baton = stmt->Bind<RunBaton>(info);
    if (baton == NULL) {
        Napi::Error::New(env, "Data type is not supported").ThrowAsJavaScriptException();
        return env.Null();
    }
    else {
        stmt->Schedule(Work_BeginRun, baton);
        return info.This();
    }
}

void Statement::Work_BeginRun(Baton* baton) {
    STATEMENT_BEGIN(Run);
}

void Statement::Work_Run(napi_env e, void* data) {
    STATEMENT_INIT(RunBaton);

    stmt->_res_handle.reset();
    stmt->_res_handle = stmt->_stmt_handle->Execute(baton->parameters);
    if (!stmt->_res_handle->success) {
        stmt->status = -1;
        stmt->message = stmt->_res_handle->error;
        stmt->_res_handle.reset();
    }
}

void Statement::Work_AfterRun(napi_env e, napi_status status, void* data) {
    std::unique_ptr<RunBaton> baton(static_cast<RunBaton*>(data));
    Statement* stmt = baton->stmt;

    Napi::Env env = stmt->Env();
    Napi::HandleScope scope(env);

    if (stmt->status  != 0) {
        Error(baton.get());
    }

    else {
        // Fire callbacks.
        Napi::Function cb = baton->callback.Value();
        if (!cb.IsUndefined() && cb.IsFunction()) {
            (stmt->Value()).Set(Napi::String::New(env, "lastID"), Napi::Number::New(env, baton->inserted_id));
            (stmt->Value()).Set( Napi::String::New(env, "changes"), Napi::Number::New(env, baton->changes));

            Napi::Value argv[] = { env.Null() };
            TRY_CATCH_CALL(stmt->Value(), cb, 1, argv);
        }
    }

    STATEMENT_END();
}

Napi::Value Statement::All(const Napi::CallbackInfo& info) {
    Napi::Env env = info.Env();
    Statement* stmt = this;

    Baton* baton = stmt->Bind<RowsBaton>(info);
    if (baton == NULL) {
        Napi::Error::New(env, "Data type is not supported").ThrowAsJavaScriptException();
        return env.Null();
    }
    else {
        stmt->Schedule(Work_BeginAll, baton);
        return info.This();
    }
}

void Statement::Work_BeginAll(Baton* baton) {
    STATEMENT_BEGIN(All);
}

void Statement::Work_All(napi_env e, void* data) {
    auto baton = ((RowsBaton*)data);
    auto stmt = baton->stmt;

	stmt->_res_handle.reset();

    if (stmt->Bind(baton->parameters)) {
        stmt->_res_handle = stmt->_stmt_handle->Execute(baton->parameters, false);
        if (!stmt->_res_handle->success) {
            stmt->status = -1;
            stmt->message = stmt->_res_handle->error;
            stmt->_res_handle.reset();
        }
    }
}

static Napi::Value convert_chunk(Napi::Env& env, vector<string> names, duckdb::DataChunk& chunk) {
    Napi::EscapableHandleScope scope(env);
	vector<Napi::String> node_names;
	assert(names.size() == chunk.column_count());
	for (auto& name : names) {
        node_names.push_back(Napi::String::New(env, name));
    }
    Napi::Array result(Napi::Array::New(env, chunk.size()));

    for (duckdb::idx_t row_idx = 0; row_idx < chunk.size(); row_idx++) {
        Napi::Object row_result = Napi::Object::New(env);

        for (duckdb::idx_t col_idx = 0; col_idx < chunk.column_count(); col_idx++) {
            Napi::Value value;

			// TODO templateroo here
			// TODO NULL handling
            switch (chunk.data[col_idx].type.id()) {
            case duckdb::LogicalTypeId::INTEGER: {
                value = Napi::Number::New(env, chunk.GetValue(col_idx, row_idx).value_.integer);
            } break;
            case duckdb::LogicalTypeId::BIGINT: {
                value = Napi::Number::New(env, chunk.GetValue(col_idx, row_idx).value_.bigint);
            } break;
            case duckdb::LogicalTypeId::VARCHAR: {
                value = Napi::String::New(env, chunk.GetValue(col_idx, row_idx).str_value);
            } break;

                //                            case SQLITE_BLOB: {
                //                                value = Napi::Buffer<char>::Copy(env, ((Values::Blob*)field)->value, ((Values::Blob*)field)->length);
                //                            } break;
            case duckdb::LogicalTypeId::SQLNULL: {
                value = env.Null();
            } break;
            default:
                Napi::Error::New(env, "Data type is not supported " + chunk.data[col_idx].type.ToString()).ThrowAsJavaScriptException();
                return env.Null();
            }
            row_result.Set(node_names[col_idx], value);
        }
        result.Set(row_idx, row_result);
    }

    return scope.Escape(result);
}

void Statement::Work_AfterAll(napi_env e, napi_status status, void* data) {
    std::unique_ptr<RowsBaton> baton(static_cast<RowsBaton*>(data));
    Statement* stmt = baton->stmt;

    Napi::Env env = stmt->Env();
    Napi::HandleScope scope(env);

    if (stmt->status != 0) {
        Error(baton.get());
		return;
    }

	assert(stmt->_res_handle);
	auto& mat_qr = (duckdb::MaterializedQueryResult&) *stmt->_res_handle;

	// regrettably this needs to know the size
    Napi::Array result(Napi::Array::New(env, mat_qr.collection.count));
    duckdb::idx_t out_idx = 0;
    while (true) {
        auto chunk = mat_qr.Fetch();
        if (chunk->size() == 0) {
            break;
        }
        auto chunk_converted = convert_chunk(env, mat_qr.names, *chunk);
		if (!chunk_converted.IsArray()) {
			// error was set before
			return;
		}
		for (duckdb::idx_t row_idx = 0; row_idx < chunk->size(); row_idx++) {
            result.Set(out_idx++, chunk_converted.ToObject().Get(row_idx));
        }
    }

    // Fire callbacks.
    Napi::Function cb = baton->callback.Value();
    if (!cb.IsUndefined() && cb.IsFunction()) {
        if (mat_qr.collection.count > 0) {
            // Create the result array from the data we acquired.

            // invoke callback
            Napi::Value argv[] = { env.Null(), result };
            TRY_CATCH_CALL(stmt->Value(), cb, 2, argv);
        }
        else {
            // There were no result rows.
            Napi::Value argv[] = {
                env.Null(),
                Napi::Array::New(env, 0)
            };
            TRY_CATCH_CALL(stmt->Value(), cb, 2, argv);
        }
    }

    STATEMENT_END();
}

Napi::Value Statement::Each(const Napi::CallbackInfo& info) {
    Napi::Env env = info.Env();
    Statement* stmt = this;

    int last = info.Length();

    Napi::Function completed;
    if (last >= 2 && info[last - 1].IsFunction() && info[last - 2].IsFunction()) {
        completed = info[--last].As<Napi::Function>();
    }

    EachBaton* baton = stmt->Bind<EachBaton>(info, 0, last);
    if (baton == NULL) {
        Napi::Error::New(env, "Data type is not supported").ThrowAsJavaScriptException();
        return env.Null();
    }
    else {
        baton->completed.Reset(completed, 1);
        stmt->Schedule(Work_BeginEach, baton);
        return info.This();
    }
}

void Statement::Work_BeginEach(Baton* baton) {
    // Only create the Async object when we're actually going into
    // the event loop. This prevents dangling events.
    EachBaton* each_baton = static_cast<EachBaton*>(baton);
    each_baton->async = new Async(each_baton->stmt, reinterpret_cast<uv_async_cb>(AsyncEach));
    each_baton->async->item_cb.Reset(each_baton->callback.Value(), 1);
    each_baton->async->completed_cb.Reset(each_baton->completed.Value(), 1);

    STATEMENT_BEGIN(Each);
}

void Statement::Work_Each(napi_env e, void* data) {
    STATEMENT_INIT(EachBaton);

    Async* async = baton->async;

	stmt->_res_handle.reset();
    stmt->_res_handle = stmt->_stmt_handle->Execute(baton->parameters, true);

	if (!stmt->_res_handle->success) {
        stmt->status = -1;
        stmt->message = stmt->_res_handle->error;
        stmt->_res_handle.reset();
        async->completed = true;

        return;
    }

	async->names = stmt->_res_handle->names;

    while (true) {
        auto chunk = stmt->_res_handle->Fetch();
        if (chunk->size() == 0) {
            break;
        }
        {
            std::lock_guard<std::mutex> lock(async->mutex);
            async->data = move(chunk);
        }
		uv_async_send(&async->watcher);
    }

    async->completed = true;
    uv_async_send(&async->watcher);
}

void Statement::CloseCallback(uv_handle_t* handle) {
    assert(handle != NULL);
    assert(handle->data != NULL);
    Async* async = static_cast<Async*>(handle->data);
    delete async;
}

void Statement::AsyncEach(uv_async_t* handle) {
    Async* async = static_cast<Async*>(handle->data);

    Napi::Env env = async->stmt->Env();
    Napi::HandleScope scope(env);

    while (true) {
        // Get the contents out of the data cache for us to process in the JS callback.
        unique_ptr<duckdb::DataChunk> rows;
		{
			std::lock_guard<std::mutex> lock(async->mutex);
            rows.swap(async->data);
		}
        if (!rows || rows->size() == 0) {
            break;
        }
		auto converted_chunk = convert_chunk(env, async->names, *rows).ToObject();

        Napi::Function cb = async->item_cb.Value();
        if (!cb.IsUndefined() && cb.IsFunction()) {
            Napi::Value argv[2];
            argv[0] = env.Null();

			for (duckdb::idx_t row_idx = 0; row_idx < rows->size(); row_idx++) {
                argv[1] = converted_chunk.Get(row_idx);
                TRY_CATCH_CALL(async->stmt->Value(), cb, 2, argv);
			}
        }
    }

    Napi::Function cb = async->completed_cb.Value();
    if (async->completed) {
        if (!cb.IsEmpty() &&
                cb.IsFunction()) {
            Napi::Value argv[] = {
                env.Null(),
                Napi::Number::New(env, async->retrieved)
            };
            TRY_CATCH_CALL(async->stmt->Value(), cb, 2, argv);
        }
        uv_close(reinterpret_cast<uv_handle_t*>(handle), CloseCallback);
    }
}

void Statement::Work_AfterEach(napi_env e, napi_status status, void* data) {
    std::unique_ptr<EachBaton> baton(static_cast<EachBaton*>(data));
    Statement* stmt = baton->stmt;

    Napi::Env env = stmt->Env();
    Napi::HandleScope scope(env);

    if (stmt->status != 0) {
        Error(baton.get());
    }

    STATEMENT_END();
}

Napi::Value Statement::Reset(const Napi::CallbackInfo& info) {
    Napi::Env env = info.Env();
    Statement* stmt = this;

    OPTIONAL_ARGUMENT_FUNCTION(0, callback);

    Baton* baton = new Baton(stmt, callback);
    stmt->Schedule(Work_BeginReset, baton);

    return info.This();
}

void Statement::Work_BeginReset(Baton* baton) {
    STATEMENT_BEGIN(Reset);
}

void Statement::Work_Reset(napi_env e, void* data) {
    STATEMENT_INIT(Baton);
    // FIXME
    //sqlite3_reset(stmt->_handle);
    stmt->status = 0;
}

void Statement::Work_AfterReset(napi_env e, napi_status status, void* data) {
    std::unique_ptr<Baton> baton(static_cast<Baton*>(data));
    Statement* stmt = baton->stmt;

    Napi::Env env = stmt->Env();
    Napi::HandleScope scope(env);

    // Fire callbacks.
    Napi::Function cb = baton->callback.Value();
    if (!cb.IsUndefined() && cb.IsFunction()) {
        Napi::Value argv[] = { env.Null() };
        TRY_CATCH_CALL(stmt->Value(), cb, 1, argv);
    }

    STATEMENT_END();
}


Napi::Value Statement::Finalize_(const Napi::CallbackInfo& info) {
    Napi::Env env = info.Env();
    Statement* stmt = this;
    OPTIONAL_ARGUMENT_FUNCTION(0, callback);

    Baton* baton = new Baton(stmt, callback);
    stmt->Schedule(Finalize_, baton);

    return stmt->db->Value();
}

void Statement::Finalize_(Baton* b) {
    std::unique_ptr<Baton> baton(b);
    Napi::Env env = baton->stmt->Env();
    Napi::HandleScope scope(env);

    baton->stmt->Finalize_();

    // Fire callback in case there was one.
    Napi::Function cb = baton->callback.Value();
    if (!cb.IsUndefined() && cb.IsFunction()) {
        TRY_CATCH_CALL(baton->stmt->Value(), cb, 0, NULL);
    }
}

void Statement::Finalize_() {
    assert(!finalized);
    finalized = true;
    CleanQueue();
    // Finalize returns the status code of the last operation. We already fired
    // error events in case those failed.
   // sqlite3_finalize(_handle);
    _res_handle.reset();
    _stmt_handle.reset();
    _chunk_handle.reset();
    db->Unref();
}

void Statement::CleanQueue() {
    Napi::Env env = this->Env();
    Napi::HandleScope scope(env);

    if (prepared && !queue.empty()) {
        // This statement has already been prepared and is now finalized.
        // Fire error for all remaining items in the queue.
        EXCEPTION(Napi::String::New(env, "Statement is already finalized"),  exception);
        Napi::Value argv[] = { exception };
        bool called = false;

        // Clear out the queue so that this object can get GC'ed.
        while (!queue.empty()) {
            std::unique_ptr<Call> call(queue.front());
            queue.pop();

            std::unique_ptr<Baton> baton(call->baton);
            Napi::Function cb = baton->callback.Value();

            if (prepared && !cb.IsEmpty() &&
                cb.IsFunction()) {
                TRY_CATCH_CALL(Value(), cb, 1, argv);
                called = true;
            }
        }

        // When we couldn't call a callback function, emit an error on the
        // Statement object.
        if (!called) {
            Napi::Value info[] = { Napi::String::New(env, "error"), exception };
            EMIT_EVENT(Value(), 2, info);
        }
    }
    else while (!queue.empty()) {
        // Just delete all items in the queue; we already fired an event when
        // preparing the statement failed.
        std::unique_ptr<Call> call(queue.front());
        queue.pop();
        // We don't call the actual callback, so we have to make sure that
        // the baton gets destroyed.
        delete call->baton;
    }
}
