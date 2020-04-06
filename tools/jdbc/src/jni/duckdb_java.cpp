#include "duckdb_java.h"
#include "duckdb.hpp"

using namespace duckdb;
using namespace std;


JNIEXPORT jobject JNICALL Java_nl_cwi_da_duckdb_DuckDBDatabase_duckdb_1jdbc_1startup
  (JNIEnv *env, jobject, jstring, jboolean) {
	auto db = new DuckDB(":memory:");
	return env->NewDirectByteBuffer(db, 0);
}

JNIEXPORT void JNICALL Java_nl_cwi_da_duckdb_DuckDBDatabase_duckdb_1jdbc_1shutdown
  (JNIEnv *env, jobject, jobject db_ref_buf) {
	auto db_ref = (DuckDB*) env->GetDirectBufferAddress(db_ref_buf);
	if (db_ref) {
		delete db_ref;
	}
}

JNIEXPORT jobject JNICALL Java_nl_cwi_da_duckdb_DuckDBDatabase_duckdb_1jdbc_1connect
  (JNIEnv *env, jobject, jobject db_ref_buf) {
	auto db_ref = (DuckDB*) env->GetDirectBufferAddress(db_ref_buf);
	auto conn = new Connection(*db_ref);
	return env->NewDirectByteBuffer(conn, 0);
}

JNIEXPORT void JNICALL Java_nl_cwi_da_duckdb_DuckDBDatabase_duckdb_1jdbc_1disconnect
  (JNIEnv *env, jobject, jobject conn_ref_buf) {
	auto conn_ref = (Connection*) env->GetDirectBufferAddress(conn_ref_buf);
	if (conn_ref) {
		delete conn_ref;
	}
}

struct StatementHolder {
	unique_ptr<PreparedStatement> stmt;
};

JNIEXPORT jobject JNICALL Java_nl_cwi_da_duckdb_DuckDBDatabase_duckdb_1jdbc_1prepare
  (JNIEnv *env , jobject, jobject conn_ref_buf, jstring query_j) {
	auto conn_ref = (Connection*) env->GetDirectBufferAddress(conn_ref_buf);
	if (!conn_ref) {
		// TODO how to throw exceptions?
	}

	auto *query_c = env->GetStringUTFChars(query_j, 0);
	auto query = string(query_c);
	env->ReleaseStringUTFChars(query_j, query_c);

	auto stmt = new StatementHolder();
	stmt->stmt = conn_ref->Prepare(query);
	return env->NewDirectByteBuffer(stmt, 0);
}
