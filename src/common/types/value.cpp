

#include "common/types/value.hpp"
#include "common/exception.hpp"

using namespace duckdb;
using namespace std;

Value::Value(const Value &other)
    : type(other.type), is_null(other.is_null), len(other.len) {
	if ((type == TypeId::VARCHAR || type == TypeId::VARBINARY ||
	     type == TypeId::ARRAY) &&
	    other.value_.data) {
		value_.data = new char[len + 1];
		memcpy(value_.data, other.value_.data, other.len + 1);
	} else {
		this->value_ = other.value_;
	}
}

Value Value::CastAs(TypeId new_type) {
	// check if we can just make a copy
	if (new_type == this->type) {
		return *this;
	}
	// have to do a cast
	Value new_value;
	new_value.type = new_type;
	new_value.is_null = this->is_null;
	if (is_null) {
		return new_value;
	}

	throw NotImplementedException("Did not implement value cast yet!");
}

string Value::ToString() const {
	switch (type) {
	case TypeId::BOOLEAN:
		return value_.boolean ? "True" : "False";
	case TypeId::TINYINT:
		return to_string(value_.tinyint);
	case TypeId::SMALLINT:
		return to_string(value_.smallint);
	case TypeId::INTEGER:
		return to_string(value_.integer);
	case TypeId::BIGINT:
		return to_string(value_.bigint);
	case TypeId::DECIMAL:
		return to_string(value_.decimal);
	default:
		throw NotImplementedException("Unimplemented printing");
	}
}
