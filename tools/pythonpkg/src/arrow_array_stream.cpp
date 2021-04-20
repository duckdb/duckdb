#include "duckdb/common/assert.hpp"
#include "include/duckdb_python/arrow_array_stream.hpp"

namespace duckdb{

PythonTableArrowArrayStream::PythonTableArrowArrayStream(const py::object &arrow_table) : arrow_table(arrow_table) {
		InitializeFunctionPointers(&stream);
		stream.private_data = this;
		batches = arrow_table.attr("to_batches")();
}

void PythonTableArrowArrayStream::InitializeFunctionPointers(ArrowArrayStream *stream){
    stream->get_schema = PythonTableArrowArrayStream::MyStreamGetSchema;
    stream->get_next = PythonTableArrowArrayStream::MyStreamGetNext;
    stream->release = PythonTableArrowArrayStream::MyStreamRelease;
    stream->get_last_error = PythonTableArrowArrayStream::MyStreamGetLastError;
}

ArrowArrayStream* PythonTableArrowArrayStream::PythonTableArrowArrayStreamFactory(uintptr_t arrow_table){
    const py::object * arrow_table_py = (py::object *) arrow_table;
    auto table_stream =  new PythonTableArrowArrayStream(*arrow_table_py);
    return &table_stream->stream;
}

    int PythonTableArrowArrayStream::PythonTableArrowArrayStream::MyStreamGetSchema(struct ArrowArrayStream *stream, struct ArrowSchema *out) {
		D_ASSERT(stream->private_data);
		py::gil_scoped_acquire acquire;
		auto my_stream = (PythonTableArrowArrayStream *)stream->private_data;
		if (!stream->release) {
			my_stream->last_error = "stream was released";
			return -1;
		}
		auto schema = my_stream->arrow_table.attr("schema");
		if (!py::hasattr(schema, "_export_to_c")) {
			my_stream->last_error = "failed to acquire export_to_c function";
			return -1;
		}
		auto export_to_c = schema.attr("_export_to_c");
		export_to_c((uint64_t)out);
		return 0;
	}

	 int PythonTableArrowArrayStream::MyStreamGetNext(struct ArrowArrayStream *stream, struct ArrowArray *out) {
		D_ASSERT(stream->private_data);
		py::gil_scoped_acquire acquire;
		auto my_stream = (PythonTableArrowArrayStream *)stream->private_data;
		if (!stream->release) {
			my_stream->last_error = "stream was released";
			return -1;
		}
		if (my_stream->batch_idx >= py::len(my_stream->batches)) {
			out->release = nullptr;
			return 0;
		}
		auto stream_batch = my_stream->batches[my_stream->batch_idx++];
		if (!py::hasattr(stream_batch, "_export_to_c")) {
			my_stream->last_error = "failed to acquire export_to_c function";
			return -1;
		}
		auto export_to_c = stream_batch.attr("_export_to_c");
		export_to_c((uint64_t)out);
		return 0;
	}

	 void PythonTableArrowArrayStream::MyStreamRelease(struct ArrowArrayStream *stream) {
		py::gil_scoped_acquire acquire;
		if (!stream->release) {
			return;
		}
		stream->release = nullptr;
		delete (PythonTableArrowArrayStream *)stream->private_data;
	}

	 const char *PythonTableArrowArrayStream::MyStreamGetLastError(struct ArrowArrayStream *stream) {
		py::gil_scoped_acquire acquire;
		if (!stream->release) {
			return "stream was released";
		}
		D_ASSERT(stream->private_data);
		auto my_stream = (PythonTableArrowArrayStream *)stream->private_data;
		return my_stream->last_error.c_str();
	}
}