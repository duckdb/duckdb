#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"

using duckdb::InvalidInputException;
using duckdb::string;
using duckdb::StringUtil;

namespace duckdb {

struct PythonCSVLineTerminator {
public:
	PythonCSVLineTerminator() = delete;
	enum class Type { LINE_FEED, CARRIAGE_RETURN_LINE_FEED };

public:
	static Type FromString(const string &str) {
		if (str == "\\n") {
			return Type::LINE_FEED;
		}
		if (str == "\\r\\n") {
			return Type::CARRIAGE_RETURN_LINE_FEED;
		}
		if (str == "LINE_FEED") {
			return Type::LINE_FEED;
		}
		if (str == "CARRIAGE_RETURN_LINE_FEED") {
			return Type::CARRIAGE_RETURN_LINE_FEED;
		}
		throw InvalidInputException("'%s' is not a recognized type for 'lineterminator'", str);
	}
	static string ToString(Type type) {
		switch (type) {
		case Type::LINE_FEED:
			return "\\n";
		case Type::CARRIAGE_RETURN_LINE_FEED:
			return "\\r\\n";
		default:
			throw NotImplementedException("No conversion for PythonCSVLineTerminator enum to string");
		}
	}
};

} // namespace duckdb

using duckdb::PythonCSVLineTerminator;

namespace py = pybind11;

namespace PYBIND11_NAMESPACE {
namespace detail {

template <>
struct type_caster<PythonCSVLineTerminator::Type> : public type_caster_base<PythonCSVLineTerminator::Type> {
	using base = type_caster_base<PythonCSVLineTerminator::Type>;
	PythonCSVLineTerminator::Type tmp;

public:
	bool load(handle src, bool convert) {
		if (base::load(src, convert)) {
			return true;
		} else if (py::isinstance<py::str>(src)) {
			tmp = duckdb::PythonCSVLineTerminator::FromString(py::str(src));
			value = &tmp;
			return true;
		}
		return false;
	}

	static handle cast(PythonCSVLineTerminator::Type src, return_value_policy policy, handle parent) {
		return base::cast(src, policy, parent);
	}
};

} // namespace detail
} // namespace PYBIND11_NAMESPACE
