#include "duckdb_python/python_conversion.hpp"
#include "duckdb_python/pybind_wrapper.hpp"

namespace duckdb {

Value TransformPythonValue(PythonInstanceChecker& instance_checker, py::handle ele) {
	if (ele.is_none()) {
		return Value();
	} else if (py::isinstance<py::bool_>(ele)) {
		return Value::BOOLEAN(ele.cast<bool>());
	} else if (py::isinstance<py::int_>(ele)) {
		return Value::BIGINT(ele.cast<int64_t>());
	} else if (py::isinstance<py::float_>(ele)) {
		return Value::DOUBLE(ele.cast<double>());
	} else if (instance_checker.IsInstanceOf(ele, "Decimal", "decimal")) {
		return py::str(ele).cast<string>();
	} else if (instance_checker.IsInstanceOf(ele, "datetime", "datetime")) {
		auto ptr = ele.ptr();
		auto year = PyDateTime_GET_YEAR(ptr);
		auto month = PyDateTime_GET_MONTH(ptr);
		auto day = PyDateTime_GET_DAY(ptr);
		auto hour = PyDateTime_DATE_GET_HOUR(ptr);
		auto minute = PyDateTime_DATE_GET_MINUTE(ptr);
		auto second = PyDateTime_DATE_GET_SECOND(ptr);
		auto micros = PyDateTime_DATE_GET_MICROSECOND(ptr);
		return Value::TIMESTAMP(year, month, day, hour, minute, second, micros);
	} else if (instance_checker.IsInstanceOf(ele, "time", "datetime")) {
		auto ptr = ele.ptr();
		auto hour = PyDateTime_TIME_GET_HOUR(ptr);
		auto minute = PyDateTime_TIME_GET_MINUTE(ptr);
		auto second = PyDateTime_TIME_GET_SECOND(ptr);
		auto micros = PyDateTime_TIME_GET_MICROSECOND(ptr);
		return Value::TIME(hour, minute, second, micros);
	} else if (instance_checker.IsInstanceOf(ele, "date", "datetime")) {
		auto ptr = ele.ptr();
		auto year = PyDateTime_GET_YEAR(ptr);
		auto month = PyDateTime_GET_MONTH(ptr);
		auto day = PyDateTime_GET_DAY(ptr);
		return Value::DATE(year, month, day);
	} else if (py::isinstance<py::str>(ele)) {
		return ele.cast<string>();
	} else if (py::isinstance<py::memoryview>(ele)) {
		py::memoryview py_view = ele.cast<py::memoryview>();
		PyObject *py_view_ptr = py_view.ptr();
		Py_buffer *py_buf = PyMemoryView_GET_BUFFER(py_view_ptr);
		return Value::BLOB(const_data_ptr_t(py_buf->buf), idx_t(py_buf->len));
	} else if (py::isinstance<py::bytes>(ele)) {
		const string &ele_string = ele.cast<string>();
		return Value::BLOB(const_data_ptr_t(ele_string.data()), ele_string.size());
	} else if (py::isinstance<py::list>(ele)) {
		auto size = py::len(ele);

		if (size == 0) {
			return Value::EMPTYLIST(LogicalType::SQLNULL);
		}

		vector<Value> values;
		values.reserve(size);

		for (auto py_val : ele) {
			values.emplace_back(TransformPythonValue(instance_checker, py_val));
		}

		return Value::LIST(values);
	} else if (py::isinstance<py::dict>(ele)) {
		auto keys = ele.attr("keys")();
		auto values = ele.attr("values")();
		auto size = py::len(keys);

		if (size == 0) {
			return Value::MAP(Value::EMPTYLIST(LogicalType::SQLNULL), Value::EMPTYLIST(LogicalType::SQLNULL));
		}

		vector<Value> key_values;
		vector<Value> val_values;
		key_values.reserve(size);
		val_values.reserve(size);

		for (idx_t i = 0; i < size; i++) {
			key_values.emplace_back(TransformPythonValue(instance_checker, keys.attr("__getitem__")(i)));
			val_values.emplace_back(TransformPythonValue(instance_checker, values.attr("__getitem__")(i)));
		}
		return Value::MAP(Value::LIST(key_values), Value::LIST(val_values));
	} else if (instance_checker.IsInstanceOf(ele, "ndarray", "numpy")) {
		return TransformPythonValue(instance_checker, ele.attr("tolist")());
	} else {
		throw std::runtime_error("TransformPythonValue unknown param type " + py::str(ele.get_type()).cast<string>());
	}
}

} //namespace duckdb
