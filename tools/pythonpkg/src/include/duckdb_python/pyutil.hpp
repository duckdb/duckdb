#pragma once

#include "duckdb_python/pybind11/pybind_wrapper.hpp"
#include "duckdb/common/types.hpp"

namespace duckdb {

struct PyUtil {
	static idx_t PyByteArrayGetSize(PyObject *obj) {
		return PyByteArray_GET_SIZE(obj); // NOLINT
	}

	static Py_buffer *PyMemoryViewGetBuffer(PyObject *obj) {
		return PyMemoryView_GET_BUFFER(obj);
	}

	static bool PyUnicodeIsCompactASCII(PyObject *obj) {
		return PyUnicode_IS_COMPACT_ASCII(obj);
	}

	static const char *PyUnicodeData(PyObject *obj) {
		return const_char_ptr_cast(PyUnicode_DATA(obj));
	}

	static char *PyUnicodeDataMutable(PyObject *obj) {
		return char_ptr_cast(PyUnicode_DATA(obj));
	}

	static idx_t PyUnicodeGetLength(PyObject *obj) {
		return PyUnicode_GET_LENGTH(obj);
	}

	static bool PyUnicodeIsCompact(PyCompactUnicodeObject *obj) {
		return PyUnicode_IS_COMPACT(obj);
	}

	static bool PyUnicodeIsASCII(PyCompactUnicodeObject *obj) {
		return PyUnicode_IS_ASCII(obj);
	}

	static int PyUnicodeKind(PyObject *obj) {
		return PyUnicode_KIND(obj);
	}

	static Py_UCS1 *PyUnicode1ByteData(PyObject *obj) {
		return PyUnicode_1BYTE_DATA(obj);
	}

	static Py_UCS2 *PyUnicode2ByteData(PyObject *obj) {
		return PyUnicode_2BYTE_DATA(obj);
	}

	static Py_UCS4 *PyUnicode4ByteData(PyObject *obj) {
		return PyUnicode_4BYTE_DATA(obj);
	}
};

} // namespace duckdb
