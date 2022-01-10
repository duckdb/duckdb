#include "org_duckdb_DuckDBNative.h"
#include "duckdb.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/appender.hpp"

using namespace duckdb;
using namespace std;

static string byte_array_to_string(JNIEnv *env, jbyteArray ba_j) {
	idx_t len = env->GetArrayLength(ba_j);
	string ret;
	ret.resize(len);

	jbyte *bytes = (jbyte *)env->GetByteArrayElements(ba_j, NULL);

	for (idx_t i = 0; i < len; i++) {
		ret[i] = bytes[i];
	}
	env->ReleaseByteArrayElements(ba_j, bytes, 0);

	return ret;
}

static jobject decode_charbuffer_to_jstring(JNIEnv *env, const char *d_str, idx_t d_str_len) {
	// TODO cache this somewhere, probably slow to look this stuff for every string
	jclass charset_class = env->FindClass("java/nio/charset/Charset");
	jclass charbuffer_class = env->FindClass("java/nio/CharBuffer");
	jmethodID for_name =
		env->GetStaticMethodID(charset_class, "forName", "(Ljava/lang/String;)Ljava/nio/charset/Charset;");
	jobject charset = env->CallStaticObjectMethod(charset_class, for_name, env->NewStringUTF("UTF-8"));
	jmethodID charset_decode =
		env->GetMethodID(charset_class, "decode", "(Ljava/nio/ByteBuffer;)Ljava/nio/CharBuffer;");
	jmethodID charbuffer_to_string = env->GetMethodID(charbuffer_class, "toString", "()Ljava/lang/String;");

	auto bb = env->NewDirectByteBuffer((void *)d_str, d_str_len);
	auto j_cb = env->CallObjectMethod(charset, charset_decode, bb);
	auto j_str = env->CallObjectMethod(j_cb, charbuffer_to_string);
	return j_str;
}

JNIEXPORT jobject JNICALL Java_org_duckdb_DuckDBNative_duckdb_1jdbc_1startup(JNIEnv *env, jclass, jbyteArray database_j,
																			 jboolean read_only) {
	auto database = byte_array_to_string(env, database_j);
	DBConfig config;
	if (read_only) {
		config.access_mode = AccessMode::READ_ONLY;
	}
	try {
		auto db = new DuckDB(database, &config);
		return env->NewDirectByteBuffer(db, 0);
	} catch (exception &e) {
		env->ThrowNew(env->FindClass("java/sql/SQLException"), e.what());
	}
	return nullptr;
}

JNIEXPORT void JNICALL Java_org_duckdb_DuckDBNative_duckdb_1jdbc_1shutdown(JNIEnv *env, jclass, jobject db_ref_buf) {
	auto db_ref = (DuckDB *)env->GetDirectBufferAddress(db_ref_buf);
	if (db_ref) {
		delete db_ref;
	}
}

JNIEXPORT jobject JNICALL Java_org_duckdb_DuckDBNative_duckdb_1jdbc_1connect(JNIEnv *env, jclass, jobject db_ref_buf) {
	auto db_ref = (DuckDB *)env->GetDirectBufferAddress(db_ref_buf);
	try {
		auto conn = new Connection(*db_ref);
		return env->NewDirectByteBuffer(conn, 0);
	} catch (exception &e) {
		env->ThrowNew(env->FindClass("java/sql/SQLException"), e.what());
	}
	return nullptr;
}

JNIEXPORT void JNICALL Java_org_duckdb_DuckDBNative_duckdb_1jdbc_1set_1auto_1commit(JNIEnv *env, jclass,
																					jobject conn_ref_buf,
																					jboolean auto_commit) {
	auto conn_ref = (Connection *)env->GetDirectBufferAddress(conn_ref_buf);
	if (!conn_ref || !conn_ref->context) {
		env->ThrowNew(env->FindClass("java/sql/SQLException"), "Invalid connection");
	}
	conn_ref->context->RunFunctionInTransaction([&]() { conn_ref->SetAutoCommit(auto_commit); });
}

JNIEXPORT jboolean JNICALL Java_org_duckdb_DuckDBNative_duckdb_1jdbc_1get_1auto_1commit(JNIEnv *env, jclass,
																						jobject conn_ref_buf) {
	auto conn_ref = (Connection *)env->GetDirectBufferAddress(conn_ref_buf);
	if (!conn_ref) {
		env->ThrowNew(env->FindClass("java/sql/SQLException"), "Invalid connection");
	}
	return conn_ref->IsAutoCommit();
}

JNIEXPORT void JNICALL Java_org_duckdb_DuckDBNative_duckdb_1jdbc_1disconnect(JNIEnv *env, jclass,
																			 jobject conn_ref_buf) {
	auto conn_ref = (Connection *)env->GetDirectBufferAddress(conn_ref_buf);
	if (conn_ref) {
		delete conn_ref;
	}
}

struct StatementHolder {
	unique_ptr<PreparedStatement> stmt;
};

#include "utf8proc_wrapper.hpp"

JNIEXPORT jobject JNICALL Java_org_duckdb_DuckDBNative_duckdb_1jdbc_1prepare(JNIEnv *env, jclass, jobject conn_ref_buf,
																			 jbyteArray query_j) {
	auto conn_ref = (Connection *)env->GetDirectBufferAddress(conn_ref_buf);
	if (!conn_ref) {
		env->ThrowNew(env->FindClass("java/sql/SQLException"), "Invalid connection");
	}

	auto query = byte_array_to_string(env, query_j);

	auto stmt_ref = new StatementHolder();
	stmt_ref->stmt = conn_ref->Prepare(query);
	if (!stmt_ref->stmt->success) {
		string error_msg = string(stmt_ref->stmt->error);
		stmt_ref->stmt = nullptr;
		env->ThrowNew(env->FindClass("java/sql/SQLException"), error_msg.c_str());
	}
	return env->NewDirectByteBuffer(stmt_ref, 0);
}

struct ResultHolder {
	unique_ptr<QueryResult> res;
	unique_ptr<DataChunk> chunk;
};

JNIEXPORT jobject JNICALL Java_org_duckdb_DuckDBNative_duckdb_1jdbc_1execute(JNIEnv *env, jclass, jobject stmt_ref_buf,
																			 jobjectArray params) {
	auto stmt_ref = (StatementHolder *)env->GetDirectBufferAddress(stmt_ref_buf);
	if (!stmt_ref) {
		env->ThrowNew(env->FindClass("java/sql/SQLException"), "Invalid statement");
	}
	auto res_ref = new ResultHolder();
	vector<Value> duckdb_params;

	idx_t param_len = env->GetArrayLength(params);
	if (param_len != stmt_ref->stmt->n_param) {
		env->ThrowNew(env->FindClass("java/sql/SQLException"), "Parameter count mismatch");
	}

	if (param_len > 0) {
		auto bool_class = env->FindClass("java/lang/Boolean");
		auto byte_class = env->FindClass("java/lang/Byte");
		auto short_class = env->FindClass("java/lang/Short");
		auto integer_class = env->FindClass("java/lang/Integer");
		auto long_class = env->FindClass("java/lang/Long");
		auto float_class = env->FindClass("java/lang/Float");
		auto double_class = env->FindClass("java/lang/Double");
		auto string_class = env->FindClass("java/lang/String");
		auto timestamp_class = env->FindClass("org/duckdb/DuckDBTimestamp");

		for (idx_t i = 0; i < param_len; i++) {
			auto param = env->GetObjectArrayElement(params, i);
			if (param == nullptr) {
				duckdb_params.push_back(Value());
				continue;
			} else if (env->IsInstanceOf(param, bool_class)) {
				duckdb_params.push_back(
					Value::BOOLEAN(env->CallBooleanMethod(param, env->GetMethodID(bool_class, "booleanValue", "()Z"))));
				continue;
			} else if (env->IsInstanceOf(param, byte_class)) {
				duckdb_params.push_back(
					Value::TINYINT(env->CallByteMethod(param, env->GetMethodID(byte_class, "byteValue", "()B"))));
				continue;
			} else if (env->IsInstanceOf(param, short_class)) {
				duckdb_params.push_back(
					Value::SMALLINT(env->CallShortMethod(param, env->GetMethodID(short_class, "shortValue", "()S"))));
				continue;
			} else if (env->IsInstanceOf(param, integer_class)) {
				duckdb_params.push_back(
					Value::INTEGER(env->CallIntMethod(param, env->GetMethodID(integer_class, "intValue", "()I"))));
				continue;
			} else if (env->IsInstanceOf(param, long_class)) {
				duckdb_params.push_back(
					Value::BIGINT(env->CallLongMethod(param, env->GetMethodID(long_class, "longValue", "()J"))));
				continue;
			} else if (env->IsInstanceOf(param, timestamp_class)) {
				duckdb_params.push_back(Value::TIMESTAMP((timestamp_t)env->CallLongMethod(
					param, env->GetMethodID(timestamp_class, "getMicrosEpoch", "()J"))));
				continue;
			} else if (env->IsInstanceOf(param, float_class)) {
				duckdb_params.push_back(
					Value::FLOAT(env->CallFloatMethod(param, env->GetMethodID(float_class, "floatValue", "()F"))));
				continue;
			} else if (env->IsInstanceOf(param, double_class)) {
				duckdb_params.push_back(
					Value::DOUBLE(env->CallDoubleMethod(param, env->GetMethodID(double_class, "doubleValue", "()D"))));
				continue;
			} else if (env->IsInstanceOf(param, string_class)) {
				auto *param_string = env->GetStringUTFChars((jstring)param, 0);
				duckdb_params.push_back(Value(param_string));
				env->ReleaseStringUTFChars((jstring)param, param_string);
				continue;
			} else {
				env->ThrowNew(env->FindClass("java/sql/SQLException"), "Unsupported parameter type");
			}
		}
	}

	res_ref->res = stmt_ref->stmt->Execute(duckdb_params, false);
	if (!res_ref->res->success) {
		string error_msg = string(res_ref->res->error);
		res_ref->res = nullptr;
		env->ThrowNew(env->FindClass("java/sql/SQLException"), error_msg.c_str());
	}
	return env->NewDirectByteBuffer(res_ref, 0);
}

JNIEXPORT void JNICALL Java_org_duckdb_DuckDBNative_duckdb_1jdbc_1release(JNIEnv *env, jclass, jobject stmt_ref_buf) {
	auto stmt_ref = (StatementHolder *)env->GetDirectBufferAddress(stmt_ref_buf);
	if (stmt_ref) {
		delete stmt_ref;
	}
}

JNIEXPORT void JNICALL Java_org_duckdb_DuckDBNative_duckdb_1jdbc_1free_1result(JNIEnv *env, jclass,
																			   jobject res_ref_buf) {
	auto res_ref = (ResultHolder *)env->GetDirectBufferAddress(res_ref_buf);
	if (res_ref) {
		delete res_ref;
	}
}

JNIEXPORT jobject JNICALL Java_org_duckdb_DuckDBNative_duckdb_1jdbc_1meta(JNIEnv *env, jclass, jobject stmt_ref_buf) {

	auto stmt_ref = (StatementHolder *)env->GetDirectBufferAddress(stmt_ref_buf);
	if (!stmt_ref || !stmt_ref->stmt || !stmt_ref->stmt->success) {
		jclass Exception = env->FindClass("java/sql/SQLException");
		env->ThrowNew(Exception, "Invalid statement");
	}

	jclass meta = env->FindClass("org/duckdb/DuckDBResultSetMetaData");
	jmethodID meta_construct = env->GetMethodID(meta, "<init>", "(II[Ljava/lang/String;[Ljava/lang/String;)V");

	auto column_count = stmt_ref->stmt->ColumnCount();
	auto &names = stmt_ref->stmt->GetNames();
	auto &types = stmt_ref->stmt->GetTypes();

	auto name_array = env->NewObjectArray(column_count, env->FindClass("java/lang/String"), nullptr);
	auto type_array = env->NewObjectArray(column_count, env->FindClass("java/lang/String"), nullptr);

	for (idx_t col_idx = 0; col_idx < column_count; col_idx++) {
		env->SetObjectArrayElement(name_array, col_idx,
								   decode_charbuffer_to_jstring(env, names[col_idx].c_str(), names[col_idx].length()));
		env->SetObjectArrayElement(type_array, col_idx, env->NewStringUTF(types[col_idx].ToString().c_str()));
	}

	return env->NewObject(meta, meta_construct, stmt_ref->stmt->n_param, column_count, name_array, type_array);
}

JNIEXPORT jobjectArray JNICALL Java_org_duckdb_DuckDBNative_duckdb_1jdbc_1fetch(JNIEnv *env, jclass,
																				jobject res_ref_buf) {
	auto res_ref = (ResultHolder *)env->GetDirectBufferAddress(res_ref_buf);
	if (!res_ref || !res_ref->res || !res_ref->res->success) {
		jclass Exception = env->FindClass("java/sql/SQLException");
		env->ThrowNew(Exception, "Invalid result set");
	}

	res_ref->chunk = res_ref->res->Fetch();
	if (!res_ref->chunk) {
		res_ref->chunk = make_unique<DataChunk>();
	}
	auto row_count = res_ref->chunk->size();

	auto vec_array = (jobjectArray)env->NewObjectArray(res_ref->chunk->ColumnCount(),
													   env->FindClass("org/duckdb/DuckDBVector"), nullptr);
	for (idx_t col_idx = 0; col_idx < res_ref->chunk->ColumnCount(); col_idx++) {
		auto &vec = res_ref->chunk->data[col_idx];
		auto type_str = env->NewStringUTF(vec.GetType().ToString().c_str());
		// construct nullmask
		auto null_array = env->NewBooleanArray(row_count);
		jboolean *null_array_ptr = env->GetBooleanArrayElements(null_array, nullptr);
		for (idx_t row_idx = 0; row_idx < row_count; row_idx++) {
			null_array_ptr[row_idx] = FlatVector::IsNull(vec, row_idx);
		}
		env->ReleaseBooleanArrayElements(null_array, null_array_ptr, 0);

		jclass vec_class = env->FindClass("org/duckdb/DuckDBVector");
		jmethodID vec_construct = env->GetMethodID(vec_class, "<init>", "(Ljava/lang/String;I[Z)V");
		auto jvec = env->NewObject(vec_class, vec_construct, type_str, (int)row_count, null_array);

		jobject constlen_data = nullptr;
		jobjectArray varlen_data = nullptr;

		switch (vec.GetType().id()) {
		case LogicalTypeId::BOOLEAN:
			constlen_data = env->NewDirectByteBuffer(FlatVector::GetData(vec), row_count * sizeof(bool));
			break;
		case LogicalTypeId::TINYINT:
			constlen_data = env->NewDirectByteBuffer(FlatVector::GetData(vec), row_count * sizeof(int8_t));
			break;
		case LogicalTypeId::SMALLINT:
			constlen_data = env->NewDirectByteBuffer(FlatVector::GetData(vec), row_count * sizeof(int16_t));
			break;
		case LogicalTypeId::INTEGER:
			constlen_data = env->NewDirectByteBuffer(FlatVector::GetData(vec), row_count * sizeof(int32_t));
			break;
		case LogicalTypeId::BIGINT:
			constlen_data = env->NewDirectByteBuffer(FlatVector::GetData(vec), row_count * sizeof(int64_t));
			break;
		case LogicalTypeId::UTINYINT:
			constlen_data = env->NewDirectByteBuffer(FlatVector::GetData(vec), row_count * sizeof(uint8_t));
			break;
		case LogicalTypeId::USMALLINT:
			constlen_data = env->NewDirectByteBuffer(FlatVector::GetData(vec), row_count * sizeof(uint16_t));
			break;
		case LogicalTypeId::UINTEGER:
			constlen_data = env->NewDirectByteBuffer(FlatVector::GetData(vec), row_count * sizeof(uint32_t));
			break;
		case LogicalTypeId::UBIGINT:
			constlen_data = env->NewDirectByteBuffer(FlatVector::GetData(vec), row_count * sizeof(uint64_t));
			break;
		case LogicalTypeId::HUGEINT:
			constlen_data = env->NewDirectByteBuffer(FlatVector::GetData(vec), row_count * sizeof(hugeint_t));
			break;
		case LogicalTypeId::FLOAT:
			constlen_data = env->NewDirectByteBuffer(FlatVector::GetData(vec), row_count * sizeof(float));
			break;
		case LogicalTypeId::DECIMAL: {
			Vector double_vec(LogicalType::DOUBLE);
			VectorOperations::Cast(vec, double_vec, row_count);
			vec.ReferenceAndSetType(double_vec);
			// fall through on purpose
		}
		case LogicalTypeId::DOUBLE:
			constlen_data = env->NewDirectByteBuffer(FlatVector::GetData(vec), row_count * sizeof(double));
			break;
		case LogicalTypeId::TIMESTAMP:
			constlen_data = env->NewDirectByteBuffer(FlatVector::GetData(vec), row_count * sizeof(timestamp_t));
			break;
		case LogicalTypeId::TIME:
		case LogicalTypeId::DATE:
		case LogicalTypeId::INTERVAL: {
			Vector string_vec(LogicalType::VARCHAR);
			VectorOperations::Cast(vec, string_vec, row_count);
			vec.ReferenceAndSetType(string_vec);
			// fall through on purpose
		}
		case LogicalTypeId::VARCHAR:
			varlen_data = env->NewObjectArray(row_count, env->FindClass("java/lang/String"), nullptr);
			for (idx_t row_idx = 0; row_idx < row_count; row_idx++) {
				if (FlatVector::IsNull(vec, row_idx)) {
					continue;
				}
				auto d_str = ((string_t *)FlatVector::GetData(vec))[row_idx];
				auto j_str = decode_charbuffer_to_jstring(env, d_str.GetDataUnsafe(), d_str.GetSize());
				env->SetObjectArrayElement(varlen_data, row_idx, j_str);
			}
			break;
		case LogicalTypeId::BLOB:
			varlen_data = env->NewObjectArray(row_count, env->FindClass("java/nio/ByteBuffer"), nullptr);

			for (idx_t row_idx = 0; row_idx < row_count; row_idx++) {
				if (FlatVector::IsNull(vec, row_idx)) {
					continue;
				}
				auto &d_str = ((string_t *)FlatVector::GetData(vec))[row_idx];
				auto j_obj = env->NewDirectByteBuffer((void *)d_str.GetDataUnsafe(), d_str.GetSize());
				env->SetObjectArrayElement(varlen_data, row_idx, j_obj);
			}
			break;
		default:
			jclass Exception = env->FindClass("java/sql/SQLException");
			env->ThrowNew(Exception, ("Unsupported result column type " + vec.GetType().ToString()).c_str());
		}

		jfieldID constlen_data_field = env->GetFieldID(vec_class, "constlen_data", "Ljava/nio/ByteBuffer;");
		jfieldID varlen_data_field = env->GetFieldID(vec_class, "varlen_data", "[Ljava/lang/Object;");

		env->SetObjectField(jvec, constlen_data_field, constlen_data);
		env->SetObjectField(jvec, varlen_data_field, varlen_data);

		env->SetObjectArrayElement(vec_array, col_idx, jvec);
	}

	return vec_array;
}

JNIEXPORT jint JNICALL Java_org_duckdb_DuckDBNative_duckdb_1jdbc_1fetch_1size(JNIEnv *, jclass) {
	return STANDARD_VECTOR_SIZE;
}

JNIEXPORT jstring JNICALL Java_org_duckdb_DuckDBNative_duckdb_1jdbc_1prepare_1type(JNIEnv *env, jclass,
																				   jobject stmt_ref_buf) {

	auto stmt_ref = (StatementHolder *)env->GetDirectBufferAddress(stmt_ref_buf);
	if (!stmt_ref || !stmt_ref->stmt || !stmt_ref->stmt->success) {
		jclass Exception = env->FindClass("java/sql/SQLException");
		env->ThrowNew(Exception, "Invalid statement");
	}
	return env->NewStringUTF(StatementTypeToString(stmt_ref->stmt->GetStatementType()).c_str());
}

JNIEXPORT jobject JNICALL Java_org_duckdb_DuckDBNative_duckdb_1jdbc_1create_1appender(JNIEnv *env, jclass,
																					  jobject conn_ref_buf,
																					  jbyteArray schema_name_j,
																					  jbyteArray table_name_j) {

	auto conn_ref = (Connection *)env->GetDirectBufferAddress(conn_ref_buf);
	if (!conn_ref || !conn_ref->context) {
		env->ThrowNew(env->FindClass("java/sql/SQLException"), "Invalid connection");
	}
	auto schema_name = byte_array_to_string(env, schema_name_j);
	auto table_name = byte_array_to_string(env, table_name_j);
	try {
		auto appender = new Appender(*conn_ref, schema_name, table_name);
		return env->NewDirectByteBuffer(appender, 0);
	} catch (exception &e) {
		env->ThrowNew(env->FindClass("java/sql/SQLException"), e.what());
	}
	return nullptr;
}

static Appender *get_appender(JNIEnv *env, jobject appender_ref_buf) {
	auto appender_ref = (Appender *)env->GetDirectBufferAddress(appender_ref_buf);
	if (!appender_ref) {
		env->ThrowNew(env->FindClass("java/sql/SQLException"), "Invalid appender");
	}
	return appender_ref;
}

JNIEXPORT void JNICALL Java_org_duckdb_DuckDBNative_duckdb_1jdbc_1appender_1begin_1row(JNIEnv *env, jclass,
																					   jobject appender_ref_buf) {
	try {
		get_appender(env, appender_ref_buf)->BeginRow();
	} catch (exception &e) {
		env->ThrowNew(env->FindClass("java/sql/SQLException"), e.what());
	}
}

JNIEXPORT void JNICALL Java_org_duckdb_DuckDBNative_duckdb_1jdbc_1appender_1end_1row(JNIEnv *env, jclass,
																					 jobject appender_ref_buf) {
	try {
		get_appender(env, appender_ref_buf)->EndRow();
	} catch (exception &e) {
		env->ThrowNew(env->FindClass("java/sql/SQLException"), e.what());
	}
}

JNIEXPORT void JNICALL Java_org_duckdb_DuckDBNative_duckdb_1jdbc_1appender_1flush(JNIEnv *env, jclass,
																				  jobject appender_ref_buf) {
	try {
		get_appender(env, appender_ref_buf)->Flush();
	} catch (exception &e) {
		env->ThrowNew(env->FindClass("java/sql/SQLException"), e.what());
	}
}

JNIEXPORT void JNICALL Java_org_duckdb_DuckDBNative_duckdb_1jdbc_1appender_1close(JNIEnv *env, jclass,
																				  jobject appender_ref_buf) {
	try {
		auto appender = get_appender(env, appender_ref_buf);
		appender->Close();
		delete appender;
	} catch (exception &e) {
		env->ThrowNew(env->FindClass("java/sql/SQLException"), e.what());
	}
}

JNIEXPORT void JNICALL Java_org_duckdb_DuckDBNative_duckdb_1jdbc_1appender_1append_1boolean(JNIEnv *env, jclass,
																							jobject appender_ref_buf,
																							jboolean value) {
	try {
		get_appender(env, appender_ref_buf)->Append((bool)value);
	} catch (exception &e) {
		env->ThrowNew(env->FindClass("java/sql/SQLException"), e.what());
	}
}

JNIEXPORT void JNICALL Java_org_duckdb_DuckDBNative_duckdb_1jdbc_1appender_1append_1byte(JNIEnv *env, jclass,
																						 jobject appender_ref_buf,
																						 jbyte value) {
	try {
		get_appender(env, appender_ref_buf)->Append((int8_t)value);
	} catch (exception &e) {
		env->ThrowNew(env->FindClass("java/sql/SQLException"), e.what());
	}
}

JNIEXPORT void JNICALL Java_org_duckdb_DuckDBNative_duckdb_1jdbc_1appender_1append_1short(JNIEnv *env, jclass,
																						  jobject appender_ref_buf,
																						  jshort value) {
	try {
		get_appender(env, appender_ref_buf)->Append((int16_t)value);
	} catch (exception &e) {
		env->ThrowNew(env->FindClass("java/sql/SQLException"), e.what());
	}
}

JNIEXPORT void JNICALL Java_org_duckdb_DuckDBNative_duckdb_1jdbc_1appender_1append_1int(JNIEnv *env, jclass,
																						jobject appender_ref_buf,
																						jint value) {
	try {
		get_appender(env, appender_ref_buf)->Append((int32_t)value);
	} catch (exception &e) {
		env->ThrowNew(env->FindClass("java/sql/SQLException"), e.what());
	}
}

JNIEXPORT void JNICALL Java_org_duckdb_DuckDBNative_duckdb_1jdbc_1appender_1append_1long(JNIEnv *env, jclass,
																						 jobject appender_ref_buf,
																						 jlong value) {
	try {
		get_appender(env, appender_ref_buf)->Append((int64_t)value);
	} catch (exception &e) {
		env->ThrowNew(env->FindClass("java/sql/SQLException"), e.what());
	}
}

JNIEXPORT void JNICALL Java_org_duckdb_DuckDBNative_duckdb_1jdbc_1appender_1append_1float(JNIEnv *env, jclass,
																						  jobject appender_ref_buf,
																						  jfloat value) {
	try {
		get_appender(env, appender_ref_buf)->Append((float)value);
	} catch (exception &e) {
		env->ThrowNew(env->FindClass("java/sql/SQLException"), e.what());
	}
}

JNIEXPORT void JNICALL Java_org_duckdb_DuckDBNative_duckdb_1jdbc_1appender_1append_1double(JNIEnv *env, jclass,
																						   jobject appender_ref_buf,
																						   jdouble value) {
	try {
		get_appender(env, appender_ref_buf)->Append((double)value);
	} catch (exception &e) {
		env->ThrowNew(env->FindClass("java/sql/SQLException"), e.what());
	}
}

JNIEXPORT void JNICALL Java_org_duckdb_DuckDBNative_duckdb_1jdbc_1appender_1append_1string(JNIEnv *env, jclass,
																						   jobject appender_ref_buf,
																						   jbyteArray value) {
	try {
		auto string_value = byte_array_to_string(env, value);
		get_appender(env, appender_ref_buf)->Append(string_value.c_str());
	} catch (exception &e) {
		env->ThrowNew(env->FindClass("java/sql/SQLException"), e.what());
	}
}
