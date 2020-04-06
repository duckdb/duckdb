#include "duckdb_java.h"
#include "duckdb.hpp"

using namespace duckdb;
using namespace std;


JNIEXPORT jobject JNICALL Java_nl_cwi_da_duckdb_DuckDBNative_duckdb_1jdbc_1startup
  (JNIEnv * env, jclass, jstring, jboolean) {
	auto db = new DuckDB(":memory:");
	return env->NewDirectByteBuffer(db, 0);
}


JNIEXPORT void JNICALL Java_nl_cwi_da_duckdb_DuckDBNative_duckdb_1jdbc_1shutdown
  (JNIEnv * env, jclass, jobject db_ref_buf) {
	auto db_ref = (DuckDB*) env->GetDirectBufferAddress(db_ref_buf);
	if (db_ref) {
		delete db_ref;
	}
}

JNIEXPORT jobject JNICALL Java_nl_cwi_da_duckdb_DuckDBNative_duckdb_1jdbc_1connect
  (JNIEnv * env, jclass, jobject db_ref_buf) {
	auto db_ref = (DuckDB*) env->GetDirectBufferAddress(db_ref_buf);
	auto conn = new Connection(*db_ref);
	return env->NewDirectByteBuffer(conn, 0);
}

JNIEXPORT void JNICALL Java_nl_cwi_da_duckdb_DuckDBNative_duckdb_1jdbc_1disconnect
  (JNIEnv * env, jclass, jobject conn_ref_buf) {
	auto conn_ref = (Connection*) env->GetDirectBufferAddress(conn_ref_buf);
	if (conn_ref) {
		delete conn_ref;
	}
}

struct StatementHolder {
	unique_ptr<PreparedStatement> stmt;
};

JNIEXPORT jobject JNICALL Java_nl_cwi_da_duckdb_DuckDBNative_duckdb_1jdbc_1prepare
  (JNIEnv * env, jclass, jobject conn_ref_buf, jstring query_j) {
	auto conn_ref = (Connection*) env->GetDirectBufferAddress(conn_ref_buf);
	if (!conn_ref) {
        jclass Exception = env->FindClass("java/sql/SQLException");
        env->ThrowNew(Exception, "Invalid connection");
	}

	auto *query_c = env->GetStringUTFChars(query_j, 0);
	auto query = string(query_c);
	env->ReleaseStringUTFChars(query_j, query_c);

	auto stmt_ref = new StatementHolder();
	stmt_ref->stmt = conn_ref->Prepare(query);
	if (!stmt_ref->stmt->success) {
        jclass Exception = env->FindClass("java/sql/SQLException");
        string error_msg = string(stmt_ref->stmt->error);
        stmt_ref->stmt = nullptr;
        env->ThrowNew(Exception,error_msg.c_str());
	}
	return env->NewDirectByteBuffer(stmt_ref, 0);
}

struct ResultHolder {
	unique_ptr<QueryResult> res;
};

JNIEXPORT jobject JNICALL Java_nl_cwi_da_duckdb_DuckDBNative_duckdb_1jdbc_1execute
  (JNIEnv * env, jclass, jobject stmt_ref_buf, jobjectArray params) {
	auto stmt_ref = (StatementHolder*) env->GetDirectBufferAddress(stmt_ref_buf);
	if (!stmt_ref) {
        jclass Exception = env->FindClass("java/sql/SQLException");
        env->ThrowNew(Exception, "Invalid statement");
	}
	auto res_ref = new ResultHolder();
	res_ref->res = stmt_ref->stmt->Execute();
	if (!res_ref->res->success) {
        jclass Exception = env->FindClass("java/sql/SQLException");
        string error_msg = string(res_ref->res->error);
        res_ref->res = nullptr;
        env->ThrowNew(Exception,error_msg.c_str());
	}
	return env->NewDirectByteBuffer(res_ref, 0);
}

JNIEXPORT void JNICALL Java_nl_cwi_da_duckdb_DuckDBNative_duckdb_1jdbc_1release
  (JNIEnv * env, jclass, jobject stmt_ref_buf) {
	auto stmt_ref = (StatementHolder*) env->GetDirectBufferAddress(stmt_ref_buf);
	if (stmt_ref) {
		stmt_ref->stmt = nullptr;
	}
}

JNIEXPORT void JNICALL Java_nl_cwi_da_duckdb_DuckDBNative_duckdb_1jdbc_1free_1result
  (JNIEnv * env, jclass, jobject res_ref_buf) {
	auto res_ref = (ResultHolder*) env->GetDirectBufferAddress(res_ref_buf);
	if (res_ref) {
		res_ref->res = nullptr;
	}
}


JNIEXPORT jobject JNICALL Java_nl_cwi_da_duckdb_DuckDBNative_duckdb_1jdbc_1meta
  (JNIEnv * env, jclass, jobject res_ref_buf) {
	auto res_ref = (ResultHolder*) env->GetDirectBufferAddress(res_ref_buf);
	if (!res_ref) {
		jclass Exception = env->FindClass("java/sql/SQLException");
		env->ThrowNew(Exception, "Invalid result set");
	}
	jclass meta = env->FindClass("nl/cwi/da/duckdb/DuckDBResultSetMetaData");
	jmethodID meta_construct = env->GetMethodID(meta, "<init>", "(ZLjava/lang/String;)V");

	return env->NewObject(meta, meta_construct);
}





