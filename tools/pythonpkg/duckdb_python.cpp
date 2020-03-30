#include <pybind11/pybind11.h>
#include <pybind11/numpy.h>
#include <vector>
#include "duckdb.hpp"

namespace py = pybind11;

template<class SRC>
static SRC fetch_scalar(duckdb::Vector &src_vec, duckdb::idx_t offset) {
	auto src_ptr = duckdb::FlatVector::GetData<SRC>(src_vec);
	return src_ptr[offset];
}

struct Result {
	py::object fetchone() {
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


	static py::array fetch_string_column(duckdb::ChunkCollection &collection,
			duckdb::idx_t column) {
		// we cannot directly create an object numpy array
		auto out_l = py::list(collection.count);

		duckdb::idx_t out_offset = 0;
		for (auto &data_chunk : collection.chunks) {
			auto &src = data_chunk->data[column];
			auto src_ptr = duckdb::FlatVector::GetData<duckdb::string_t>(src);
			auto &nullmask = duckdb::FlatVector::Nullmask(src);

			for (duckdb::idx_t i = 0; i < data_chunk->size(); i++) {
				if (nullmask[i]) {
					continue;
				}
				out_l[i + out_offset] = py::str(src_ptr[i].GetData());
			}
			out_offset += data_chunk->size();
		}
		return py::array(out_l);
	}


	template<class SRC, class TGT>
	static py::array fetch_column(duckdb::ChunkCollection &collection,
			duckdb::idx_t column) {
		py::array_t<TGT> out;
		out.resize( { collection.count });
		TGT *out_ptr = out.mutable_data();

		duckdb::idx_t out_offset = 0;
		for (auto &data_chunk : collection.chunks) {
			auto src_ptr = duckdb::FlatVector::GetData<SRC>(data_chunk->data[column]);
			for (duckdb::idx_t i = 0; i < data_chunk->size(); i++) {
				// never mind the nullmask here, will be faster
				out_ptr[i + out_offset] = (TGT) src_ptr[i];
			}
			out_offset += data_chunk->size();
		}
		return out;
	}


	py::dict fetchnumpy() {
		if (!result) {
			throw std::runtime_error("result closed");
		}
		// need to materialize the result if it was streamed because we need the count :/
		duckdb::MaterializedQueryResult *mres = nullptr;
		std::unique_ptr<duckdb::QueryResult> mat_res_holder;
		if (result->type == duckdb::QueryResultType::STREAM_RESULT) {
			mat_res_holder =
					((duckdb::StreamQueryResult*) result.get())->Materialize();
			mres = (duckdb::MaterializedQueryResult*) mat_res_holder.get();
		} else {
			mres = (duckdb::MaterializedQueryResult*) result.get();
		}
		assert(mres);

		py::dict res;
		for (duckdb::idx_t col_idx = 0; col_idx < mres->types.size(); col_idx++) {
			// convert the actual payload
			py::array col_res;
			switch (mres->types[col_idx]) {
			case duckdb::TypeId::INT32:
				col_res = fetch_column<int32_t, int32_t>(mres->collection, col_idx);
				break;
			case duckdb::TypeId::DOUBLE:
				col_res = fetch_column<double, double>(mres->collection, col_idx);
				break;
			case duckdb::TypeId::VARCHAR:
				col_res = fetch_string_column(mres->collection, col_idx);
				break;

			default:
				throw std::runtime_error("unsupported type");
			}

			// convert the nullmask
			py::array_t<bool> nullmask;
			nullmask.resize( { mres->collection.count });
			bool *nullmask_ptr = nullmask.mutable_data();

			duckdb::idx_t out_offset = 0;
			for (auto &data_chunk : mres->collection.chunks) {
				auto &src_nm = duckdb::FlatVector::Nullmask(data_chunk->data[col_idx]);
				for (duckdb::idx_t i = 0; i < data_chunk->size(); i++) {
					nullmask_ptr[i + out_offset] = src_nm[i];
				}
				out_offset += data_chunk->size();
			}

			// create masked array and assign to output
			auto masked_array = py::module::import("numpy.ma").attr("masked_array")(col_res,
					nullmask);
			res[mres->names[col_idx].c_str()] = masked_array;
		}
		return res;
	}

	py::object fetchdf() {
		return py::module::import("pandas").attr("DataFrame").attr("from_dict")(
				fetchnumpy());
	}

	void close() {
		result = nullptr;
	}
	duckdb::idx_t chunk_offset = 0;

	std::unique_ptr<duckdb::QueryResult> result;
	std::unique_ptr<duckdb::DataChunk> current_chunk;

};

//
//PyObject *duckdb_cursor_getiter(duckdb_Cursor *self);
//PyObject *duckdb_cursor_iternext(duckdb_Cursor *self);
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
	.def("fetchnumpy", &Result::fetchnumpy)
	.def("fetchdf", &Result::fetchdf)
	.def("close", &Result::close);
}
