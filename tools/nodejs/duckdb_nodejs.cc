#include <napi.h>
#include "duckdb.hpp"

using namespace std;
using namespace duckdb;

class DuckDBNodeConnection : public Napi::ObjectWrap<DuckDBNodeConnection> {
public:
	DuckDBNodeConnection(const Napi::CallbackInfo &info) : Napi::ObjectWrap<DuckDBNodeConnection>(info) {
		Napi::Env env = info.Env();
		Napi::HandleScope scope(env);
		std::string dbname = ":memory:";
		if (info.Length() == 1) {
			if (!info[0].IsString()) {
				Napi::TypeError::New(info.Env(), "Database path expected").ThrowAsJavaScriptException();
			}
			dbname = info[0].ToString().Utf8Value();
		}
		db = make_unique<DuckDB>(dbname);
		con = make_unique<Connection>(*db);
	}

	static Napi::Object Init(Napi::Env env, Napi::Object exports) {
		Napi::HandleScope scope(env);

		Napi::Function func = DefineClass(env, "connection",
		                                  {InstanceMethod("query", &DuckDBNodeConnection::Query),
		                                   InstanceMethod("disconnect", &DuckDBNodeConnection::Disconnect)});

		constructor = Napi::Persistent(func);
		constructor.SuppressDestruct();

		exports.Set("connect", func);
		return exports;
	}

private:
	Napi::Value Query(const Napi::CallbackInfo &info) {
		if (!con) {
			Napi::Error::New(info.Env(), "Connection was closed").ThrowAsJavaScriptException();
		}
		if (info.Length() != 1 || !info[0].IsString()) {
			Napi::TypeError::New(info.Env(), "Query String expected").ThrowAsJavaScriptException();
		}

		auto query = info[0].ToString().Utf8Value();
		auto result = con->Query(query);

		if (!result->success) {
            Napi::Error::New(info.Env(), "Query failed: " + result->error).ThrowAsJavaScriptException();
        }

		auto ncol = result->types.size();
		auto nrow = result->collection.count;

		auto column_names = Napi::Array::New(info.Env(), ncol);
        auto column_types = Napi::Array::New(info.Env(), ncol);
        auto result_data = Napi::Array::New(info.Env(), ncol);

        for (idx_t col_idx = 0; col_idx < ncol; col_idx++) {
			column_names.Set((uint32_t)col_idx, Napi::String::New(info.Env(), result->names[col_idx]));
            column_types.Set((uint32_t)col_idx, Napi::String::New(info.Env(), SQLTypeToString(result->sql_types[col_idx])));

			// FIXME this is horribly inefficient. Use typed arrays
            auto column_data = Napi::Array::New(info.Env(), nrow);

            for (idx_t row_idx = 0; row_idx < nrow; row_idx++) {
                switch(result->sql_types[col_idx].id) {
                case SQLTypeId::TINYINT:
                case SQLTypeId::SMALLINT:
                case SQLTypeId::INTEGER:
                case SQLTypeId::BIGINT:
                case SQLTypeId::FLOAT:
                case SQLTypeId::DOUBLE:
                    column_data.Set((uint32_t)row_idx, Napi::Number::New(info.Env(), result->collection.GetValue(col_idx, row_idx).CastAs(TypeId::DOUBLE).value_.double_));
					break;
                case SQLTypeId::VARCHAR:
                    column_data.Set((uint32_t)row_idx, Napi::String::New(info.Env(), result->collection.GetValue(col_idx, row_idx).str_value));
                    break;
                default:
                    Napi::TypeError::New(info.Env(), "Unsupported data type " + SQLTypeToString(result->sql_types[col_idx])).ThrowAsJavaScriptException();
                }
			}
			result_data.Set((uint32_t)col_idx, column_data);
        }

		auto result_obj = Napi::Object::New(info.Env());

		result_obj.Set("names", column_names);
        result_obj.Set("types", column_types);
        result_obj.Set("data", result_data);

        return result_obj;
	}

	Napi::Value Disconnect(const Napi::CallbackInfo &info) {
		if (con) {
			con.reset();
			db.reset();
			return Napi::Boolean::New(info.Env(), true);
        }
		return Napi::Boolean::New(info.Env(), false);
	}

	static Napi::FunctionReference constructor;
	unique_ptr<DuckDB> db;
	unique_ptr<Connection> con;
};

Napi::FunctionReference DuckDBNodeConnection::constructor;

Napi::Object InitAll(Napi::Env env, Napi::Object exports) {
	return DuckDBNodeConnection::Init(env, exports);
}

NODE_API_MODULE(addon, InitAll)
