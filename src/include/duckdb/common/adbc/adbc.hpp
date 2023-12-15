//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/adbc/adbc.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/adbc/adbc.h"

#include <string>

namespace duckdb_adbc {

AdbcStatusCode DatabaseNew(struct AdbcDatabase *database, struct AdbcError *error);

AdbcStatusCode DatabaseSetOption(struct AdbcDatabase *database, const char *key, const char *value,
                                 struct AdbcError *error);

AdbcStatusCode DatabaseInit(struct AdbcDatabase *database, struct AdbcError *error);

AdbcStatusCode DatabaseRelease(struct AdbcDatabase *database, struct AdbcError *error);

AdbcStatusCode ConnectionNew(struct AdbcConnection *connection, struct AdbcError *error);

AdbcStatusCode ConnectionSetOption(struct AdbcConnection *connection, const char *key, const char *value,
                                   struct AdbcError *error);

AdbcStatusCode ConnectionInit(struct AdbcConnection *connection, struct AdbcDatabase *database,
                              struct AdbcError *error);

AdbcStatusCode ConnectionRelease(struct AdbcConnection *connection, struct AdbcError *error);

AdbcStatusCode ConnectionGetInfo(struct AdbcConnection *connection, const uint32_t *info_codes,
                                 size_t info_codes_length, struct ArrowArrayStream *out, struct AdbcError *error);

AdbcStatusCode ConnectionGetObjects(struct AdbcConnection *connection, int depth, const char *catalog,
                                    const char *db_schema, const char *table_name, const char **table_type,
                                    const char *column_name, struct ArrowArrayStream *out, struct AdbcError *error);

AdbcStatusCode ConnectionGetTableSchema(struct AdbcConnection *connection, const char *catalog, const char *db_schema,
                                        const char *table_name, struct ArrowSchema *schema, struct AdbcError *error);

AdbcStatusCode ConnectionGetTableTypes(struct AdbcConnection *connection, struct ArrowArrayStream *out,
                                       struct AdbcError *error);

AdbcStatusCode ConnectionReadPartition(struct AdbcConnection *connection, const uint8_t *serialized_partition,
                                       size_t serialized_length, struct ArrowArrayStream *out, struct AdbcError *error);

AdbcStatusCode ConnectionCommit(struct AdbcConnection *connection, struct AdbcError *error);

AdbcStatusCode ConnectionRollback(struct AdbcConnection *connection, struct AdbcError *error);

AdbcStatusCode StatementNew(struct AdbcConnection *connection, struct AdbcStatement *statement,
                            struct AdbcError *error);

AdbcStatusCode StatementRelease(struct AdbcStatement *statement, struct AdbcError *error);

AdbcStatusCode StatementExecuteQuery(struct AdbcStatement *statement, struct ArrowArrayStream *out,
                                     int64_t *rows_affected, struct AdbcError *error);

AdbcStatusCode StatementPrepare(struct AdbcStatement *statement, struct AdbcError *error);

AdbcStatusCode StatementSetSqlQuery(struct AdbcStatement *statement, const char *query, struct AdbcError *error);

AdbcStatusCode StatementSetSubstraitPlan(struct AdbcStatement *statement, const uint8_t *plan, size_t length,
                                         struct AdbcError *error);

AdbcStatusCode StatementBind(struct AdbcStatement *statement, struct ArrowArray *values, struct ArrowSchema *schema,
                             struct AdbcError *error);

AdbcStatusCode StatementBindStream(struct AdbcStatement *statement, struct ArrowArrayStream *stream,
                                   struct AdbcError *error);

AdbcStatusCode StatementGetParameterSchema(struct AdbcStatement *statement, struct ArrowSchema *schema,
                                           struct AdbcError *error);

AdbcStatusCode StatementSetOption(struct AdbcStatement *statement, const char *key, const char *value,
                                  struct AdbcError *error);

AdbcStatusCode StatementExecutePartitions(struct AdbcStatement *statement, struct ArrowSchema *schema,
                                          struct AdbcPartitions *partitions, int64_t *rows_affected,
                                          struct AdbcError *error);

void InitializeADBCError(AdbcError *error);

} // namespace duckdb_adbc

//! This method should only be called when the string is guaranteed to not be NULL
void SetError(struct AdbcError *error, const std::string &message);
// void SetError(struct AdbcError *error, const char *message);
