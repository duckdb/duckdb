#include "duckdb_node.hpp"


#define DEFINE_CONSTANT_INTEGER(target, constant, name)                        \
    Napi::PropertyDescriptor::Value(#name, Napi::Number::New(env, constant),   \
        static_cast<napi_property_attributes>(napi_enumerable | napi_configurable)),

namespace {

Napi::Object RegisterModule(Napi::Env env, Napi::Object exports) {
    Napi::HandleScope scope(env);

    node_duckdb::Database::Init(env, exports);
//    Statement::Init(env, exports);

    exports.DefineProperties({
	    DEFINE_CONSTANT_INTEGER(exports, node_duckdb::Database::DUCKDB_NODEJS_ERROR, ERROR)
        DEFINE_CONSTANT_INTEGER(exports, node_duckdb::Database::DUCKDB_NODEJS_READONLY, OPEN_READONLY) // same as SQLite
        DEFINE_CONSTANT_INTEGER(exports, 0, OPEN_READWRITE) // ignored
        DEFINE_CONSTANT_INTEGER(exports, 0, OPEN_CREATE) // ignored
        DEFINE_CONSTANT_INTEGER(exports, 0, OPEN_FULLMUTEX) // ignored
        DEFINE_CONSTANT_INTEGER(exports, 0, OPEN_SHAREDCACHE) // ignored
        DEFINE_CONSTANT_INTEGER(exports, 0, OPEN_PRIVATECACHE) // ignored
    });

    return exports;
}

}

NODE_API_MODULE(node_duckdb, RegisterModule);

namespace node_duckdb {


static std::string GetString(const Napi::CallbackInfo& info, size_t i) {
    if (info.Length() > i || !info[i].IsString()) {
        throw std::runtime_error("eek2");
    }
    return info[i].As<Napi::String>();
}

static Napi::Function GetFunction(const Napi::CallbackInfo& info, size_t i) {
    if (info.Length() > i || !info[i].IsFunction()) {
        throw std::runtime_error("eek3");
    }
    return info[i].As<Napi::Function>();
}

static bool OtherIsInt(Napi::Number source) {
    double orig_val = source.DoubleValue();
    double int_val = (double)source.Int32Value();
    if (orig_val == int_val) {
        return true;
    } else {
        return false;
    }
}


struct TaskHolder  {
    unique_ptr<Task> task;
    napi_async_work request;
	Database* db;
};

static void  TaskExecute(napi_env e, void* data) {
    auto holder = (TaskHolder*) data;
    holder->task->Run(e);
}

static void  TaskComplete(napi_env e, napi_status status, void* data) {
    auto holder = (TaskHolder*) data;
	holder->task.reset();
	holder->db->Process(e);
    delete holder;
}

void Database::Schedule(Napi::Env env, unique_ptr<Task> task) {
    task_queue.push(move(task));
    Process(env);
}



struct OpenTask : public Task {
    OpenTask(Database& database_, std::string filename_, Napi::Function callback_) : database(database_), filename(filename_), callback(callback_) {
	}

	void Run(Napi::Env env) override {
        printf("Run()\n");
		try {
			database.database = duckdb::make_unique<duckdb::DuckDB>(filename);
		} catch (...) {
            // TODO handle exception
		}

     //   if (!callback.IsUndefined() && callback.IsFunction()) {
            callback.MakeCallback(database.Value(), {env.Null()});
       // }
	}
	Database& database;
	std::string filename;
    Napi::Function callback;
};


Database::Database(const Napi::CallbackInfo& info) : Napi::ObjectWrap<Database>(info) {

	database = duckdb::make_unique<duckdb::DuckDB>();
    default_connection = duckdb::make_unique<Connection>(*this);

    if (info.Length() <= 0 || !info[0].IsString()) {
        throw std::runtime_error("eek4");

    }
    std::string filename = info[0].As<Napi::String>();
    unsigned int pos = 1;
    int mode = 0;
    if (info.Length() >= pos && info[pos].IsNumber() && OtherIsInt(info[pos].As<Napi::Number>())) {
        mode = info[pos++].As<Napi::Number>().Int32Value();
    }

    Napi::Function callback;
    if (info.Length() >= pos && info[pos].IsFunction()) {
        callback = info[pos++].As<Napi::Function>();
    }

    Schedule(info.Env(), duckdb::make_unique<OpenTask>(*this, filename, callback));
}


void Database::Process(Napi::Env env) {
	if (task_queue.empty()) {
		return;
	}
	auto task = move(task_queue.front());
	task_queue.pop();

    auto holder = new TaskHolder();
    holder->task = move(task);
	holder->db = this;

    napi_create_async_work(
        env, NULL, Napi::String::New(env, "duckdb.Database.Task"),
        TaskExecute, TaskComplete, holder, &holder->request
    );

    napi_queue_async_work(env, holder->request);

}


Napi::Object Database::Init(Napi::Env env, Napi::Object exports) {
    Napi::HandleScope scope(env);

    Napi::Function t = DefineClass(env, "Database", {
        InstanceMethod("close", &Database::Close),
        InstanceMethod("exec", &Database::Exec),
        InstanceMethod("wait", &Database::Wait),
        InstanceMethod("serialize", &Database::Serialize),
        InstanceMethod("parallelize", &Database::Parallelize),
        InstanceMethod("interrupt", &Database::Interrupt),
        InstanceAccessor("open", &Database::OpenGetter, nullptr)
    });

    exports.Set("Database", t);
    return exports;
}

Napi::Value Database::OpenGetter(const Napi::CallbackInfo& info) {
    return Napi::Boolean::New(this->Env(), this->database != nullptr);
}

Napi::Value Database::Parallelize(const Napi::CallbackInfo& info) {
    return Serialize(info);
}

Napi::Value Database::Serialize(const Napi::CallbackInfo& info) {
	GetFunction(info, 0).MakeCallback(info.This(), {});;
    Process(info.Env());
	return info.This();
}

Napi::Value Database::Exec(const Napi::CallbackInfo& info) {
    auto param_sql = GetString(info, 0);
    auto callback = GetString(info, 1);

//    Baton* baton = new ExecBaton(db, callback, sql.c_str());
//    db->Schedule(Work_BeginExec, baton, true);

    return info.This();
}

Napi::Value Database::Wait(const Napi::CallbackInfo& info) {
    auto callback = GetString(info, 1);

    return info.This();
}

Napi::Value Database::Close(const Napi::CallbackInfo& info) {
    auto callback = GetString(info, 1);

	return info.This();
}

Napi::Value Database::Interrupt(const Napi::CallbackInfo& info) {
    auto callback = GetString(info, 1);

	return info.This();
}

}

