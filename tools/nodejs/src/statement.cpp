#include "duckdb.hpp"
#include "duckdb_node.hpp"
#include "napi.h"

#include <algorithm>
#include <cassert>
#include <iostream>
#include <string>
#include <regex>

#include "duckdb/common/helper.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/types.hpp"

using duckdb::unique_ptr;
using duckdb::vector;

namespace node_duckdb {

Napi::FunctionReference Statement::Init(Napi::Env env, Napi::Object exports) {
	Napi::HandleScope scope(env);

	Napi::Function t =
	    DefineClass(env, "Statement",
	                {InstanceMethod("run", &Statement::Run), InstanceMethod("all", &Statement::All),
	                 InstanceMethod("arrowIPCAll", &Statement::ArrowIPCAll), InstanceMethod("each", &Statement::Each),
	                 InstanceMethod("finalize", &Statement::Finish), InstanceMethod("stream", &Statement::Stream),
	                 InstanceMethod("columns", &Statement::Columns)});

	exports.Set("Statement", t);

	return Napi::Persistent(t);
}

static unique_ptr<duckdb::PreparedStatement> PrepareManyInternal(Statement &statement) {
	auto &connection = statement.connection_ref->connection;
	vector<unique_ptr<duckdb::SQLStatement>> statements;
	try {
		if (connection == nullptr) {
			throw duckdb::ConnectionException("Connection was never established or has been closed already");
		}

		// Prepare all statements
		statements = connection->ExtractStatements(statement.sql);
		if (statements.empty()) {
			throw duckdb::InvalidInputException("No statement to prepare!");
		}

		// if there are multiple statements, we directly execute the statements besides the last one
		// we only return the result of the last statement to the user, unless one of the previous statements fails
		for (idx_t i = 0; i + 1 < statements.size(); i++) {
			auto pending_query = connection->PendingQuery(std::move(statements[i]));
			auto res = pending_query->Execute();
			if (res->HasError()) {
				return duckdb::make_uniq<duckdb::PreparedStatement>(res->GetErrorObject());
			}
		}

		return connection->Prepare(std::move(statements.back()));
	} catch (const duckdb::Exception &ex) {
		return duckdb::make_uniq<duckdb::PreparedStatement>(duckdb::PreservedError(ex));
	} catch (std::exception &ex) {
		return duckdb::make_uniq<duckdb::PreparedStatement>(duckdb::PreservedError(ex));
	}
}

struct PrepareTask : public Task {
	PrepareTask(Statement &statement, Napi::Function callback) : Task(statement, callback) {
	}

	void DoWork() override {
		auto &statement = Get<Statement>();
		statement.statement = PrepareManyInternal(statement);
	}

	void Callback() override {
		auto &statement = Get<Statement>();
		auto env = statement.Env();
		Napi::HandleScope scope(env);

		auto cb = callback.Value();
		if (statement.statement->HasError()) {
			cb.MakeCallback(statement.Value(), {Utils::CreateError(env, statement.statement->error)});
			return;
		}
		cb.MakeCallback(statement.Value(), {env.Null(), statement.Value()});
	}
};

Statement::Statement(const Napi::CallbackInfo &info) : Napi::ObjectWrap<Statement>(info) {

	Napi::Env env = info.Env();
	int length = info.Length();

	if (length <= 0 || !Connection::HasInstance(info[0])) {
		Napi::TypeError::New(env, "Connection object expected").ThrowAsJavaScriptException();
		return;
	} else if (length <= 1 || !info[1].IsString()) {
		Napi::TypeError::New(env, "SQL query expected").ThrowAsJavaScriptException();
		return;
	}

	connection_ref = Napi::ObjectWrap<Connection>::Unwrap(info[0].As<Napi::Object>());
	connection_ref->Ref();

	sql = info[1].As<Napi::String>();

	Napi::Function callback;
	if (info.Length() > 1 && info[2].IsFunction()) {
		callback = info[2].As<Napi::Function>();
	}

	// TODO we can have parameters here as well. Forward if that is the case.
	Value().As<Napi::Object>().DefineProperty(
	    Napi::PropertyDescriptor::Value("sql", info[1].As<Napi::String>(), napi_default));
	connection_ref->database_ref->Schedule(env, duckdb::make_uniq<PrepareTask>(*this, callback));
}

Statement::~Statement() {
	connection_ref->Unref();
	connection_ref = nullptr;
}

static Napi::Value convert_col_val(Napi::Env &env, duckdb::Value dval, duckdb::LogicalTypeId id) {
	Napi::Value value;

	if (dval.IsNull()) {
		return env.Null();
	}

	// TODO templateroo here
	switch (id) {
	case duckdb::LogicalTypeId::BOOLEAN: {
		value = Napi::Boolean::New(env, duckdb::BooleanValue::Get(dval));
	} break;
	case duckdb::LogicalTypeId::TINYINT: {
		value = Napi::Number::New(env, duckdb::TinyIntValue::Get(dval));
	} break;
	case duckdb::LogicalTypeId::SMALLINT: {
		value = Napi::Number::New(env, duckdb::SmallIntValue::Get(dval));
	} break;
	case duckdb::LogicalTypeId::INTEGER: {
		value = Napi::Number::New(env, duckdb::IntegerValue::Get(dval));
	} break;
	case duckdb::LogicalTypeId::BIGINT: {
		value = Napi::BigInt::New(env, duckdb::BigIntValue::Get(dval));
	} break;
	case duckdb::LogicalTypeId::UTINYINT: {
		value = Napi::Number::New(env, duckdb::UTinyIntValue::Get(dval));
	} break;
	case duckdb::LogicalTypeId::USMALLINT: {
		value = Napi::Number::New(env, duckdb::USmallIntValue::Get(dval));
	} break;
	case duckdb::LogicalTypeId::UINTEGER: {
		value = Napi::Number::New(env, duckdb::UIntegerValue::Get(dval));
	} break;
	case duckdb::LogicalTypeId::UBIGINT: {
		value = Napi::BigInt::New(env, duckdb::UBigIntValue::Get(dval));
	} break;
	case duckdb::LogicalTypeId::FLOAT: {
		value = Napi::Number::New(env, duckdb::FloatValue::Get(dval));
	} break;
	case duckdb::LogicalTypeId::DOUBLE: {
		value = Napi::Number::New(env, duckdb::DoubleValue::Get(dval));
	} break;
	case duckdb::LogicalTypeId::HUGEINT: {
		auto val = duckdb::HugeIntValue::Get(dval);
		auto negative = val.upper < 0;
		if (negative) {
			duckdb::Hugeint::NegateInPlace(val); // remove signing bit
		}
		D_ASSERT(val.upper >= 0);
		const uint64_t words[] = {val.lower, static_cast<uint64_t>(val.upper)};
		value = Napi::BigInt::New(env, negative, 2, words);
	} break;
	case duckdb::LogicalTypeId::DECIMAL: {
		value = Napi::Number::New(env, dval.GetValue<double>());
	} break;
	case duckdb::LogicalTypeId::INTERVAL: {
		auto interval = duckdb::IntervalValue::Get(dval);
		auto object_value = Napi::Object::New(env);
		object_value.Set("months", interval.months);
		object_value.Set("days", interval.days);
		object_value.Set("micros", interval.micros);
		value = object_value;
	} break;
#if (NAPI_VERSION > 4)
	case duckdb::LogicalTypeId::DATE: {
		const auto scale = duckdb::Interval::SECS_PER_DAY * duckdb::Interval::MSECS_PER_SEC;
		value = Napi::Date::New(env, double(dval.GetValue<int32_t>() * scale));
	} break;
	case duckdb::LogicalTypeId::TIMESTAMP:
	case duckdb::LogicalTypeId::TIMESTAMP_TZ: {
		value = Napi::Date::New(env, double(dval.GetValue<int64_t>() / duckdb::Interval::MICROS_PER_MSEC));
	} break;
#endif
	case duckdb::LogicalTypeId::VARCHAR: {
		value = Napi::String::New(env, duckdb::StringValue::Get(dval));
	} break;
	case duckdb::LogicalTypeId::BLOB: {
		auto &blob = duckdb::StringValue::Get(dval);
		value = Napi::Buffer<char>::Copy(env, blob.c_str(), blob.length());
	} break;
	case duckdb::LogicalTypeId::SQLNULL: {
		value = env.Null();
	} break;
	case duckdb::LogicalTypeId::LIST: {
		auto child_type = duckdb::ListType::GetChildType(dval.type());
		auto &child_values = duckdb::ListValue::GetChildren(dval);
		auto object_value = Napi::Array::New(env);
		for (duckdb::idx_t child_idx = 0; child_idx < child_values.size(); child_idx++) {
			auto child_value = child_values.at(child_idx);
			object_value.Set(child_idx, convert_col_val(env, child_value, child_type.id()));
		}
		value = object_value;
	} break;
	case duckdb::LogicalTypeId::STRUCT: {
		auto &child_types = duckdb::StructType::GetChildTypes(dval.type());
		auto &child_values = duckdb::StructValue::GetChildren(dval);
		auto object_value = Napi::Object::New(env);
		for (duckdb::idx_t child_idx = 0; child_idx < child_values.size(); child_idx++) {
			auto child_value = child_values.at(child_idx);
			auto child_type = child_types.at(child_idx);
			object_value.Set(child_type.first, convert_col_val(env, child_value, child_type.second.id()));
		}
		value = object_value;
	} break;
	default:
		value = Napi::String::New(env, dval.ToString());
	}

	return value;
}

static Napi::Value convert_chunk(Napi::Env &env, vector<std::string> names, duckdb::DataChunk &chunk) {
	Napi::EscapableHandleScope scope(env);
	vector<Napi::String> node_names;
	assert(names.size() == chunk.ColumnCount());
	node_names.reserve(names.size());
	for (auto &name : names) {
		node_names.push_back(Napi::String::New(env, name));
	}
	Napi::Array result(Napi::Array::New(env, chunk.size()));

	for (duckdb::idx_t row_idx = 0; row_idx < chunk.size(); row_idx++) {
		Napi::Object row_result = Napi::Object::New(env);

		for (duckdb::idx_t col_idx = 0; col_idx < chunk.ColumnCount(); col_idx++) {
			duckdb::Value dval = chunk.GetValue(col_idx, row_idx);
			row_result.Set(node_names[col_idx], convert_col_val(env, dval, chunk.data[col_idx].GetType().id()));
		}
		result.Set(row_idx, row_result);
	}

	return scope.Escape(result);
}

enum RunType { RUN, EACH, ALL, ARROW_ALL };

struct StatementParam {
	vector<duckdb::Value> params;
	Napi::Function callback;
	Napi::Function complete;
};

struct RunPreparedTask : public Task {
	RunPreparedTask(Statement &statement, unique_ptr<StatementParam> params, RunType run_type)
	    : Task(statement, params->callback), params(std::move(params)), run_type(run_type) {
	}

	void DoWork() override {
		auto &statement = Get<Statement>();
		// ignorant folk arrive here without caring about the prepare callback error
		if (!statement.statement || statement.statement->HasError()) {
			return;
		}

		result =
		    statement.statement->Execute(params->params, run_type != RunType::ALL && run_type != RunType::ARROW_ALL);
	}

	void Callback() override {
		auto &statement = Get<Statement>();
		Napi::Env env = statement.Env();
		Napi::HandleScope scope(env);

		auto cb = callback.Value();
		// if there was an error we need to say so
		if (!statement.statement) {
			cb.MakeCallback(statement.Value(), {Utils::CreateError(env, "statement was finalized")});
			return;
		}
		if (statement.statement->HasError()) {
			cb.MakeCallback(statement.Value(), {Utils::CreateError(env, statement.statement->GetErrorObject())});
			return;
		}
		if (result->HasError()) {
			cb.MakeCallback(statement.Value(), {Utils::CreateError(env, result->GetErrorObject())});
			return;
		}

		switch (run_type) {
		case RunType::RUN:
			cb.MakeCallback(statement.Value(), {env.Null()});
			break;
		case RunType::EACH: {
			duckdb::idx_t count = 0;
			while (true) {
				Napi::HandleScope scope(env);

				auto chunk = result->Fetch();
				if (!chunk || chunk->size() == 0) {
					break;
				}

				auto chunk_converted = convert_chunk(env, result->names, *chunk).ToObject();
				if (!chunk_converted.IsArray()) {
					// error was set before
					return;
				}
				for (duckdb::idx_t row_idx = 0; row_idx < chunk->size(); row_idx++) {
					cb.MakeCallback(statement.Value(), {env.Null(), chunk_converted.Get(row_idx)});
					count++;
				}
			}
			if (!params->complete.IsUndefined() && params->complete.IsFunction()) {
				params->complete.MakeCallback(statement.Value(), {env.Null(), Napi::Number::New(env, count)});
			}
			break;
		}
		case RunType::ALL: {
			auto materialized_result = (duckdb::MaterializedQueryResult *)result.get();
			Napi::Array result_arr(Napi::Array::New(env, materialized_result->RowCount()));

			duckdb::idx_t out_idx = 0;
			while (true) {
				auto chunk = result->Fetch();
				if (!chunk || chunk->size() == 0) {
					break;
				}
				// ToObject has to happen here otherwise the converted chunk gets garbage collected for some reason
				auto chunk_converted = convert_chunk(env, result->names, *chunk).ToObject();
				if (!chunk_converted.IsArray()) {
					// error was set before
					return;
				}
				for (duckdb::idx_t row_idx = 0; row_idx < chunk->size(); row_idx++) {
					result_arr.Set(out_idx++, chunk_converted.Get(row_idx));
				}
			}

			cb.MakeCallback(statement.Value(), {env.Null(), result_arr});
		} break;
		case RunType::ARROW_ALL: {
			auto materialized_result = (duckdb::MaterializedQueryResult *)result.get();
			// +1 is for null bytes at end of stream
			Napi::Array result_arr(Napi::Array::New(env, materialized_result->RowCount() + 1));

			auto deleter = [](Napi::Env, void *finalizeData, void *hint) {
				delete static_cast<std::shared_ptr<duckdb::QueryResult> *>(hint);
			};
			std::shared_ptr<duckdb::QueryResult> result_ptr = std::move(result);

			duckdb::idx_t out_idx = 1;
			for (auto &chunk : materialized_result->Collection().Chunks()) {
				if (chunk.size() == 0) {
					continue;
				}

				D_ASSERT(chunk.ColumnCount() == 2);
				D_ASSERT(chunk.data[0].GetType() == duckdb::LogicalType::BLOB);
				D_ASSERT(chunk.data[1].GetType() == duckdb::LogicalType::BOOLEAN);

				for (duckdb::idx_t row_idx = 0; row_idx < chunk.size(); row_idx++) {
					duckdb::string_t blob = duckdb::FlatVector::GetData<duckdb::string_t>(chunk.data[0])[row_idx];
					bool is_header = chunk.data[1].GetData()[row_idx];

					// Create shared pointer to give (shared) ownership to ArrayBuffer, not that for these materialized
					// query results, the string data is owned by the QueryResult
					auto result_ref_ptr = new std::shared_ptr<duckdb::QueryResult>(result_ptr);

					auto array_buffer =
					    Napi::ArrayBuffer::New(env, (void *)blob.GetData(), blob.GetSize(), deleter, result_ref_ptr);

					auto typed_array = Napi::Uint8Array::New(env, blob.GetSize(), array_buffer, 0);

					// TODO we should handle this in duckdb probably
					if (is_header) {
						result_arr.Set((uint32_t)0, typed_array);
					} else {
						D_ASSERT(out_idx < materialized_result->RowCount());
						result_arr.Set(out_idx++, typed_array);
					}
				}
			}

			// TODO we should handle this in duckdb probably
			auto null_arr = Napi::Uint8Array::New(env, 4);
			memset(null_arr.Data(), '\0', 4);
			result_arr.Set(out_idx++, null_arr);

			// Confirm all rows are set
			if (materialized_result->RowCount() > 0) {
				// Non empty results should have their
				D_ASSERT(out_idx == materialized_result->RowCount() + 1);
			} else {
				D_ASSERT(out_idx == 2);
			}

			cb.MakeCallback(statement.Value(), {env.Null(), result_arr});
		} break;
		}
	}
	unique_ptr<duckdb::QueryResult> result;
	unique_ptr<StatementParam> params;
	RunType run_type;
};

struct RunQueryTask : public Task {
	RunQueryTask(Statement &statement, unique_ptr<StatementParam> params, Napi::Promise::Deferred deferred)
	    : Task(statement), deferred(deferred), params(std::move(params)) {
	}

	void DoWork() override {
		auto &statement = Get<Statement>();
		if (!statement.statement || statement.statement->HasError()) {
			return;
		}

		result = statement.statement->Execute(params->params, true);
	}

	void DoCallback() override {
		auto &statement = Get<Statement>();
		Napi::Env env = statement.Env();
		Napi::HandleScope scope(env);

		if (!statement.statement) {
			deferred.Reject(Utils::CreateError(env, "statement was finalized"));
		} else if (statement.statement->HasError()) {
			deferred.Reject(Utils::CreateError(env, statement.statement->GetErrorObject()));
		} else if (result->HasError()) {
			deferred.Reject(Utils::CreateError(env, result->GetErrorObject()));
		} else {
			auto db = statement.connection_ref->database_ref->Value();
			auto query_result = QueryResult::NewInstance(db);
			auto unwrapped = QueryResult::Unwrap(query_result);
			unwrapped->result = std::move(result);
			deferred.Resolve(query_result);
		}
	}

	Napi::Promise::Deferred deferred;
	unique_ptr<duckdb::QueryResult> result;
	unique_ptr<StatementParam> params;
};

unique_ptr<StatementParam> Statement::HandleArgs(const Napi::CallbackInfo &info) {
	size_t start_idx = ignore_first_param ? 1 : 0;
	auto params = duckdb::make_uniq<StatementParam>();

	for (auto i = start_idx; i < info.Length(); i++) {
		auto &p = info[i];
		if (p.IsFunction()) {
			if (!params->callback.IsUndefined()) { // we already saw a callback, so this is the finalizer
				params->complete = p.As<Napi::Function>();
			} else {
				params->callback = p.As<Napi::Function>();
			}
			continue;
		}
		if (p.IsUndefined()) {
			continue;
		}
		params->params.push_back(Utils::BindParameter(p));
	}
	return params;
}

Napi::Value Statement::All(const Napi::CallbackInfo &info) {
	connection_ref->database_ref->Schedule(info.Env(),
	                                       duckdb::make_uniq<RunPreparedTask>(*this, HandleArgs(info), RunType::ALL));
	return info.This();
}

Napi::Value Statement::ArrowIPCAll(const Napi::CallbackInfo &info) {
	connection_ref->database_ref->Schedule(
	    info.Env(), duckdb::make_uniq<RunPreparedTask>(*this, HandleArgs(info), RunType::ARROW_ALL));
	return info.This();
}

Napi::Value Statement::Run(const Napi::CallbackInfo &info) {
	connection_ref->database_ref->Schedule(info.Env(),
	                                       duckdb::make_uniq<RunPreparedTask>(*this, HandleArgs(info), RunType::RUN));
	return info.This();
}

Napi::Value Statement::Each(const Napi::CallbackInfo &info) {
	connection_ref->database_ref->Schedule(info.Env(),
	                                       duckdb::make_uniq<RunPreparedTask>(*this, HandleArgs(info), RunType::EACH));
	return info.This();
}

Napi::Value Statement::Stream(const Napi::CallbackInfo &info) {
	auto deferred = Napi::Promise::Deferred::New(info.Env());
	connection_ref->database_ref->Schedule(info.Env(),
	                                       duckdb::make_uniq<RunQueryTask>(*this, HandleArgs(info), deferred));
	return deferred.Promise();
}

static Napi::Value TypeToObject(Napi::Env &env, const duckdb::LogicalType &type) {
	auto obj = Napi::Object::New(env);

	auto id = duckdb::LogicalTypeIdToString(type.id());
	obj.Set("id", id);
	obj.Set("sql_type", type.ToString());

	if (type.HasAlias()) {
		obj.Set("alias", type.GetAlias());
	}

	switch (type.id()) {
	case duckdb::LogicalTypeId::STRUCT: {
		auto &child_types = duckdb::StructType::GetChildTypes(type);
		auto arr = Napi::Array::New(env, child_types.size());
		for (size_t i = 0; i < child_types.size(); i++) {
			auto child_name = child_types[i].first;
			auto child_type = child_types[i].second;
			auto child_obj = Napi::Object::New(env);
			child_obj.Set("name", child_name);
			child_obj.Set("type", TypeToObject(env, child_type));
			arr.Set(i, child_obj);
		}
		obj.Set("children", arr);
	} break;
	case duckdb::LogicalTypeId::LIST: {
		auto &child_type = duckdb::ListType::GetChildType(type);
		obj.Set("child", TypeToObject(env, child_type));
	} break;
	case duckdb::LogicalTypeId::MAP: {
		auto &key_type = duckdb::MapType::KeyType(type);
		auto &value_type = duckdb::MapType::ValueType(type);
		obj.Set("key", TypeToObject(env, key_type));
		obj.Set("value", TypeToObject(env, value_type));
	} break;
	case duckdb::LogicalTypeId::ENUM: {
		auto &values_vec = duckdb::EnumType::GetValuesInsertOrder(type);
		auto enum_size = duckdb::EnumType::GetSize(type);
		auto arr = Napi::Array::New(env, enum_size);
		for (size_t i = 0; i < enum_size; i++) {
			auto child_name = values_vec.GetValue(i).GetValue<duckdb::string>();
			arr.Set(i, child_name);
		}
		obj.Set("values", arr);
	} break;
	case duckdb::LogicalTypeId::UNION: {
		auto child_count = duckdb::UnionType::GetMemberCount(type);
		auto arr = Napi::Array::New(env, child_count);
		for (size_t i = 0; i < child_count; i++) {
			auto &child_name = duckdb::UnionType::GetMemberName(type, i);
			auto &child_type = duckdb::UnionType::GetMemberType(type, i);
			auto child_obj = Napi::Object::New(env);
			child_obj.Set("name", child_name);
			child_obj.Set("type", TypeToObject(env, child_type));
			arr.Set(i, child_obj);
		}
		obj.Set("children", arr);
	} break;
	case duckdb::LogicalTypeId::DECIMAL: {
		auto width = duckdb::DecimalType::GetWidth(type);
		auto scale = duckdb::DecimalType::GetScale(type);
		obj.Set("width", width);
		obj.Set("scale", scale);
	} break;
	default:
		break;
	}
	return obj;
}

Napi::Value Statement::Columns(const Napi::CallbackInfo &info) {
	Napi::Env env = info.Env();

	if (!statement) {
		return env.Null();
	}

	auto &names = statement->GetNames();
	auto &types = statement->GetTypes();
	auto arr = Napi::Array::New(env, names.size());
	for (size_t i = 0; i < names.size(); i++) {
		auto obj = Napi::Object::New(env);
		obj.Set("name", Napi::String::New(env, names[i]));
		obj.Set("type", TypeToObject(env, types[i]));
		arr.Set(i, obj);
	}
	return arr;
}

struct FinishTask : public Task {
	FinishTask(Statement &statement, Napi::Function callback) : Task(statement, callback) {
	}

	void DoWork() override {
		// TODO why does this break stuff?
		// Get<Statement>().statement.reset();
	}
};

Napi::Value Statement::Finish(const Napi::CallbackInfo &info) {
	Napi::Env env = info.Env();
	Napi::HandleScope scope(env);

	Napi::Function callback;

	if (info.Length() > 0 && info[0].IsFunction()) {
		callback = info[0].As<Napi::Function>();
	}

	connection_ref->database_ref->Schedule(env, duckdb::make_uniq<FinishTask>(*this, callback));
	return env.Null();
}
Napi::Object Statement::NewInstance(Napi::Env env, const vector<napi_value> &args) {
	return NodeDuckDB::GetData(env)->statement_constructor.New(args);
}

Napi::FunctionReference QueryResult::Init(Napi::Env env, Napi::Object exports) {
	Napi::HandleScope scope(env);

	Napi::Function t = DefineClass(env, "QueryResult",
	                               {InstanceMethod("nextChunk", &QueryResult::NextChunk),
	                                InstanceMethod("nextIpcBuffer", &QueryResult::NextIpcBuffer)});

	exports.Set("QueryResult", t);

	return Napi::Persistent(t);
}

QueryResult::QueryResult(const Napi::CallbackInfo &info) : Napi::ObjectWrap<QueryResult>(info) {
	database_ref = Napi::ObjectWrap<Database>::Unwrap(info[0].As<Napi::Object>());
	database_ref->Ref();
}

QueryResult::~QueryResult() {
	database_ref->Unref();
	database_ref = nullptr;
}

struct GetChunkTask : public Task {
	GetChunkTask(QueryResult &query_result, Napi::Promise::Deferred deferred) : Task(query_result), deferred(deferred) {
	}

	void DoWork() override {
		auto &query_result = Get<QueryResult>();
		chunk = query_result.result->Fetch();
	}

	void DoCallback() override {
		auto &query_result = Get<QueryResult>();
		Napi::Env env = query_result.Env();
		Napi::HandleScope scope(env);

		if (chunk == nullptr || chunk->size() == 0) {
			deferred.Resolve(env.Null());
			return;
		}

		auto chunk_converted = convert_chunk(env, query_result.result->names, *chunk).ToObject();
		if (!chunk_converted.IsArray()) {
			deferred.Reject(Utils::CreateError(env, "internal error: chunk is not array"));
		} else {
			deferred.Resolve(chunk_converted);
		}
	}

	Napi::Promise::Deferred deferred;
	unique_ptr<duckdb::DataChunk> chunk;
};

struct GetNextArrowIpcTask : public Task {
	GetNextArrowIpcTask(QueryResult &query_result, Napi::Promise::Deferred deferred)
	    : Task(query_result), deferred(deferred) {
	}

	void DoWork() override {
		auto &query_result = Get<QueryResult>();
		chunk = query_result.result->Fetch();
	}

	void DoCallback() override {
		auto &query_result = Get<QueryResult>();
		Napi::Env env = query_result.Env();
		Napi::HandleScope scope(env);

		if (chunk == nullptr || chunk->size() == 0) {
			deferred.Resolve(env.Null());
			return;
		}

		// Arrow IPC streams should be a single column of a single blob
		D_ASSERT(chunk->size() == 1 && chunk->ColumnCount() == 2);
		D_ASSERT(chunk->data[0].GetType() == duckdb::LogicalType::BLOB);

		duckdb::string_t blob = *(duckdb::string_t *)(chunk->data[0].GetData());

		// Transfer ownership and Construct ArrayBuffer
		auto data_chunk_ptr = new unique_ptr<duckdb::DataChunk>();
		*data_chunk_ptr = std::move(chunk);
		auto deleter = [](Napi::Env, void *finalizeData, void *hint) {
			delete static_cast<unique_ptr<duckdb::DataChunk> *>(hint);
		};
		auto array_buffer =
		    Napi::ArrayBuffer::New(env, (void *)blob.GetData(), blob.GetSize(), deleter, data_chunk_ptr);

		deferred.Resolve(array_buffer);
	}

	Napi::Promise::Deferred deferred;
	unique_ptr<duckdb::DataChunk> chunk;
};

Napi::Value QueryResult::NextChunk(const Napi::CallbackInfo &info) {
	auto env = info.Env();
	auto deferred = Napi::Promise::Deferred::New(env);
	database_ref->Schedule(env, duckdb::make_uniq<GetChunkTask>(*this, deferred));

	return deferred.Promise();
}

// Should only be called on an arrow ipc query
Napi::Value QueryResult::NextIpcBuffer(const Napi::CallbackInfo &info) {
	auto env = info.Env();
	auto deferred = Napi::Promise::Deferred::New(env);
	database_ref->Schedule(env, duckdb::make_uniq<GetNextArrowIpcTask>(*this, deferred));
	return deferred.Promise();
}

Napi::Object QueryResult::NewInstance(const Napi::Object &db) {
	return NodeDuckDB::GetData(db.Env())->query_result_constructor.New({db});
}

} // namespace node_duckdb
