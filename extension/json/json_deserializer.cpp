#include "json_deserializer.hpp"

namespace duckdb {

void JsonDeserializer::SetTag(const char *tag) {
    current_tag = tag;
}

// If inside an object, return the value associated by the current tag (property name)
// If inside an array, return the next element in the sequence
yyjson_val* JsonDeserializer::GetNextValue() {
	auto parent_val = Current();
	yyjson_val* val;
	if(yyjson_is_obj(parent_val)) {
		val = yyjson_obj_get(parent_val, current_tag);
	} else if(yyjson_is_arr(parent_val)) {
		val = yyjson_arr_iter_next(arr_iter);
	} else {
		// unreachable?
		throw InternalException("Cannot get value from non-array/object");
	}
	return val;
}

void JsonDeserializer::ThrowTypeError(yyjson_val* val, const char* expected) {
	auto actual = yyjson_get_type_desc(val);
	auto parent = Current();
	if(yyjson_is_obj(parent)) {
		auto msg = StringUtil::Format("property '%s' expected type '%s', but got type: '%s'", current_tag, expected, actual);
	} else if (yyjson_is_arr(parent)) {
		auto msg = StringUtil::Format("Sequence expect child of type '%s', but got type: %s", expected, actual);
	} else {
		// unreachable?
		throw InternalException("cannot get nested value from non object or array-type");
	}
}

//===--------------------------------------------------------------------===//
// Nested Types Hooks
//===--------------------------------------------------------------------===//
idx_t JsonDeserializer::BeginReadList() {
	auto val = GetNextValue();
	if(!yyjson_is_arr(val)){
		ThrowTypeError(val, "array");
	}
	stack.push_back(val);
	return yyjson_arr_size(val);
}

void JsonDeserializer::EndReadList() {
	stack.pop_back();
}

//===--------------------------------------------------------------------===//
// Primitive Types
//===--------------------------------------------------------------------===//
bool JsonDeserializer::ReadBool() {
	auto val = GetNextValue();
	if(!yyjson_is_bool(val)) {
		ThrowTypeError(val, "bool");
	}
	return yyjson_get_bool(val);
}

int32_t JsonDeserializer::ReadSignedInt32() {
	auto val = GetNextValue();
	if(!yyjson_is_sint(val)) {
		ThrowTypeError(val, "int32_t");
	}
	return yyjson_get_sint(val);
}

uint32_t JsonDeserializer::ReadUnsignedInt32() {
	auto val = GetNextValue();
	if(!yyjson_is_uint(val)) {
		ThrowTypeError(val, "uint32_t");
	}
	return yyjson_get_uint(val);
}

float JsonDeserializer::ReadFloat() {
	auto val = GetNextValue();
	if(!yyjson_is_real(val)){
		ThrowTypeError(val, "float");
	}
	return yyjson_get_real(val);
}

double JsonDeserializer::ReadDouble() {
	auto val = GetNextValue();
	if(!yyjson_is_real(val)){
		ThrowTypeError(val, "double");
	}
	return yyjson_get_real(val);
}

string JsonDeserializer::ReadString() {
	auto val = GetNextValue();
	if(!yyjson_is_str(val)){
		ThrowTypeError(val, "string");
	}
	return yyjson_get_str(val);
}



} // namespace duckdb