

#include "common/value.hpp"
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
