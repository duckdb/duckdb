//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb_python/pybind11/python_object_container.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb_python/pybind11/pybind_wrapper.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb_python/pybind11/gil_wrapper.hpp"
#include "duckdb/common/helper.hpp"

namespace duckdb {

template <typename TGT_PY_TYPE, typename SRC_PY_TYPE>
struct PythonAssignmentFunction {
	typedef void (*assign_t)(TGT_PY_TYPE &, SRC_PY_TYPE);
};

//! Every Python Object Must be created through our container
//! The Container ensures that the GIL is HOLD on Python Object Construction/Destruction/Modification
template <class PY_TYPE>
class PythonObjectContainer {
public:
	PythonObjectContainer() {
	}

	~PythonObjectContainer() {
		py::gil_scoped_acquire acquire;
		py_obj.clear();
	}

	template <class NEW_PY_TYPE>
	void AssignInternal(typename PythonAssignmentFunction<PY_TYPE, NEW_PY_TYPE>::assign_t lambda,
	                    py::handle new_value) {
		D_ASSERT(py::gil_check());
		PY_TYPE obj;
		lambda(obj, new_value);
		PushInternal(obj);
	}

	void Push(PY_TYPE obj) {
		py::gil_scoped_acquire gil;
		PushInternal(std::move(obj));
	}

	const PY_TYPE *GetPointerTop() {
		return &py_obj.back();
	}

private:
	void PushInternal(PY_TYPE obj) {
		py_obj.push_back(obj);
	}

	vector<PY_TYPE> py_obj;
};
} // namespace duckdb
