#pragma once

#include "duckdb_python/pybind11/pybind_wrapper.hpp"
#include "duckdb/common/types.hpp"

namespace duckdb {

struct PyUtil {
	static idx_t PyByteArrayGetSize(py::handle &obj) {
		return PyByteArray_GET_SIZE(obj.ptr()); // NOLINT
	}

	static Py_buffer *PyMemoryViewGetBuffer(py::handle &obj) {
		return PyMemoryView_GET_BUFFER(obj.ptr());
	}

	static bool PyUnicodeIsCompactASCII(py::handle &obj) {
		return PyUnicode_IS_COMPACT_ASCII(obj.ptr());
	}

	static const char *PyUnicodeData(py::handle &obj) {
		return const_char_ptr_cast(PyUnicode_DATA(obj.ptr()));
	}

	static char *PyUnicodeDataMutable(py::handle &obj) {
		return char_ptr_cast(PyUnicode_DATA(obj.ptr()));
	}

	static idx_t PyUnicodeGetLength(py::handle &obj) {
		return PyUnicode_GET_LENGTH(obj.ptr());
	}

	static bool PyUnicodeIsCompact(PyCompactUnicodeObject *obj) {
		return PyUnicode_IS_COMPACT(obj);
	}

	static bool PyUnicodeIsASCII(PyCompactUnicodeObject *obj) {
		return PyUnicode_IS_ASCII(obj);
	}

	static int PyUnicodeKind(py::handle &obj) {
		return PyUnicode_KIND(obj.ptr());
	}

	static Py_UCS1 *PyUnicode1ByteData(py::handle &obj) {
		return PyUnicode_1BYTE_DATA(obj.ptr());
	}

	static Py_UCS2 *PyUnicode2ByteData(py::handle &obj) {
		return PyUnicode_2BYTE_DATA(obj.ptr());
	}

	static Py_UCS4 *PyUnicode4ByteData(py::handle &obj) {
		return PyUnicode_4BYTE_DATA(obj.ptr());
	}
};

} // namespace duckdb
