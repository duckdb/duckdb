#include <pybind11/pybind11.h>
#include <vector>
#include "duckdb.hpp"

namespace py = pybind11;

template<class SRC>
static SRC fetch_scalar(duckdb::Vector &src_vec, duckdb::idx_t offset) {
	auto src_ptr = duckdb::FlatVector::GetData<SRC>(src_vec);
	return src_ptr[offset];
}

struct Result {
	py::tuple fetchone() {
		if (!result) {
			throw std::runtime_error("result closed");
		}
		if (!current_chunk || chunk_offset >= current_chunk->size()) {
			current_chunk = result->Fetch();
			chunk_offset = 0;
		}
		if (current_chunk->size() == 0) {
			return py::none();
		}
		py::tuple res(result->types.size());

		for (duckdb::idx_t i = 0; i < result->types.size(); i++) {
			auto &nullmask = duckdb::FlatVector::Nullmask(
					current_chunk->data[i]);
			if (nullmask[chunk_offset]) {
				res[i] = py::none();
				continue;
			}
			// TODO other types
			switch (result->types[i]) {
			case duckdb::TypeId::INT32:
				res[i] = fetch_scalar<int32_t>(current_chunk->data[i],
						chunk_offset);
				break;
			case duckdb::TypeId::VARCHAR:
				res[i] = fetch_scalar<duckdb::string_t>(current_chunk->data[i],
						chunk_offset).GetData();
				break;
			default:
				throw std::runtime_error("unsupported type");
			}
		}
		chunk_offset++;
		return res;
	}

	py::list fetchall() {
		if (!result) {
			throw std::runtime_error("result closed");
		}
		py::list res;
		while (true) {
			auto fres = fetchone();
			if (fres.is_none()) {
				break;
			}
			res.append(fres);
		}
		return res;
	}
	void close() {
		result = nullptr;
	}
	duckdb::idx_t chunk_offset = 0;

	std::unique_ptr<duckdb::QueryResult> result;
	std::unique_ptr<duckdb::DataChunk> current_chunk;

};

//PyObject *duckdb_cursor_getiter(duckdb_Cursor *self);
//PyObject *duckdb_cursor_iternext(duckdb_Cursor *self);
//PyObject *duckdb_cursor_fetchnumpy(duckdb_Cursor *self);
//PyObject *duckdb_cursor_fetchdf(duckdb_Cursor *self);//

struct Connection {
	// TODO parameters
	std::unique_ptr<Result> execute(std::string query) {
		if (!connection) {
			throw std::runtime_error("connection closed");
		}
		auto res = duckdb::make_unique<Result>();
		res->result = connection->Query(query);
		if (!res->result->success) {
			throw std::runtime_error(res->result->error);
		}
		return res;
	}

	Connection* begin() {
		execute("BEGIN TRANSACTION");
		return this;
	}

	Connection* commit() {
		execute("COMMIT");
		return this;
	}

	Connection* rollback() {
		execute("ROLLBACK");
		return this;
	}

	void close() {
		connection = nullptr;
		database = nullptr;
	}

	// cursor() is stupid
	Connection* cursor() {
		return this;
	}

	std::unique_ptr<duckdb::DuckDB> database;
	std::unique_ptr<duckdb::Connection> connection;
};

std::unique_ptr<Connection> connect(std::string database, bool read_only) {
	auto res = duckdb::make_unique<Connection>();
	res->database = duckdb::make_unique<duckdb::DuckDB>(database);
	res->connection = duckdb::make_unique<duckdb::Connection>(
			*res->database.get());

	return res;
}

PYBIND11_MODULE(duckdb, m) {
	m.def("connect", &connect, "some doc string", py::arg("database") = ":memory:", py::arg("read_only") = false );

	py::class_<Connection>(m, "DuckDBConnection")
	.def("cursor", &Connection::cursor)
	.def("begin", &Connection::begin)
	.def("commit", &Connection::commit)
	.def("rollback", &Connection::rollback)
	.def("execute", &Connection::execute)
	.def("close", &Connection::close);

	py::class_<Result>(m, "DuckDBResult")
	.def("fetchone", &Result::fetchone)
	.def("fetchall", &Result::fetchall)
	.def("close", &Result::close);
}
