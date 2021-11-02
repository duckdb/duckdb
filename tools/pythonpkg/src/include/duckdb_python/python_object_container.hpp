//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb_python/python_object_container.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

namespace duckdb {

template <typename TGT_PY_TYPE, typename SRC_PY_TYPE>
struct PythonAssignmentFunction {
	typedef void (*assign_t)(TGT_PY_TYPE &, SRC_PY_TYPE &);
};

//! Every Python Object Must be created through our container
//! The Container ensures that the GIL is HOLD on Python Object Construction/Destruction/Modification
template <class PY_TYPE>
class PythonObjectContainer {
public:
	PythonObjectContainer() {
		py::gil_scoped_acquire acquire;
		py_obj = make_unique<PY_TYPE>();
	}

	~PythonObjectContainer() {
		py::gil_scoped_acquire acquire;
		py_obj.reset();
	}

	template <class NEW_PY_TYPE>
	void Assign(typename PythonAssignmentFunction<PY_TYPE, NEW_PY_TYPE>::assign_t lambda, NEW_PY_TYPE &new_value) {
		py::gil_scoped_acquire acquire;
		lambda(*py_obj, new_value);
	}

	const PY_TYPE *GetPointer() {
		return py_obj.get();
	}

private:
	unique_ptr<PY_TYPE> py_obj;
};
} // namespace duckdb