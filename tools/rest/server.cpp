#include <chrono>
#include <cstdio>

#include "duckdb.hpp"
#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"

#include "httplib.hpp"
#include "json.hpp"



using namespace httplib;
using namespace duckdb;
using namespace nlohmann;

int main(void) {
  Server svr;

  if (!svr.is_valid()) {
    printf("server has an error...\n");
    return -1;
  }

  DBConfig config;
  // TODO take db path and access mode from cmdline params
 // config.access_mode = AccessMode::READ_ONLY;
  DuckDB duckdb(nullptr, &config);

  svr.Get("/query", [&duckdb](const Request &req, Response &resp) {
	auto q =  req.get_param_value("q");
	Connection conn(duckdb);
	conn.EnableProfiling();
	auto res = conn.Query(q);
	// TODO implement timeout for queries
	json j;

	if (res->success) {
		 j = {
			{"query", q},
			{"success", res->success},
			{"ncol", res->types.size()},
			{"nrow", res->collection.count},
			{"statement_type", StatementTypeToString(res->statement_type)},
			{"names", json(res->names)},
			{"name_index_map", json::object()},
			{"types", json::array()},
			{"sql_types", json::array()},
			{"data", json::array()},
			// TODO this unserialize-serialize thing is a bit ugly
			{"profiling", json::parse(conn.GetProfilingInformation(ProfilerPrintFormat::JSON))}
	};
		 for (auto& sql_type : res->sql_types) {
		 		j["sql_types"] += SQLTypeToString(sql_type);
		 		j["data"] += json::array(); // cough
		 	}
		 	for (auto& type : res->types) {
		 		j["types"] += TypeIdToString(type);
		 	}

		 	// make it easier to get col data by name
		 	size_t col_idx = 0;
		 	for (auto& name : res->names) {
				j["name_index_map"][name] = col_idx;
				col_idx++;
			}

		 	// TODO interpret sqltypes for more complex types
			for (auto& chunk : res->collection.chunks) {
				for (size_t col_idx = 0; col_idx < res->types.size(); col_idx++) {
					auto& v = chunk->data[col_idx];
					switch (v.type) {
					case TypeId::BOOLEAN:
					case TypeId::TINYINT:
					case TypeId::SMALLINT:
					case TypeId::INTEGER:
					case TypeId::BIGINT:
						// int types
						v.Cast(TypeId::BIGINT);
						VectorOperations::Exec(v, [&](index_t i, index_t k) {
							int64_t* data_ptr = (int64_t*) v.data;
							if (!v.nullmask[i]) {
								j["data"][col_idx] += data_ptr[i];

							} else {
								j["data"][col_idx] += nullptr;
							}
						});

						break;
					case TypeId::FLOAT:
					case TypeId::DOUBLE:
						v.Cast(TypeId::DOUBLE);

						VectorOperations::Exec(v, [&](index_t i, index_t k) {
							double* data_ptr = (double*) v.data;
							if (!v.nullmask[i]) {
								j["data"][col_idx] += data_ptr[i];

							} else {
								j["data"][col_idx] += nullptr;
							}
						});

						break;
					case TypeId::VARCHAR:

						VectorOperations::Exec(v, [&](index_t i, index_t k) {
							char** data_ptr = (char**) v.data;
							if (!v.nullmask[i]) {
								j["data"][col_idx] += data_ptr[i];

							} else {
								j["data"][col_idx] += nullptr;
							}
						});

						break;

					default:
						throw std::runtime_error("Unsupported Type");
					}
				}
			}
	} else {
		j = {{"query", q},
		{"success", res->success},
		{"error", res->error}};
	}

    resp.set_content(j.dump(), "application/json");
  });

  svr.set_error_handler([](const Request & /*req*/, Response &res) {
    const char *fmt = "<p>Error Status: <span style='color:red;'>%d</span></p>";
    char buf[BUFSIZ];
    snprintf(buf, sizeof(buf), fmt, res.status);
    res.set_content(buf, "text/html");
  });

  printf("Listening on http://localhost:8080\n");

  svr.listen("localhost", 8080);
  return 0;
}
