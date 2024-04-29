#pragma once

#include "duckdb/common/typedefs.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/unordered_map.hpp"

namespace duckdb {

enum class JsonKind : uint8_t {
	OBJECT,
	ARRAY,
	NUMBER,
	STRING,
	BOOLEAN,
	NULLVALUE,
};

class JsonValue {
	JsonKind kind;
	union {
		string *str_value;
		double num_value;
		bool bool_value;
		unordered_map<string, JsonValue> *object_value;
		vector<JsonValue> *array_value;
	};

public:
	JsonValue();
	JsonValue(JsonKind kind);
	JsonValue(const string &value);
	JsonValue(const char *value);
	JsonValue(double value);
	JsonValue(bool value);
	JsonValue(const unordered_map<string, JsonValue> &value);
	JsonValue(const vector<JsonValue> &value);

	// Parses a JSON value from a string
	static JsonValue Parse(const string &str);

	// Returns a string representation of the JSON value, optionally formatted with newlines and tabs
	string ToString(bool format = false) const;

	JsonKind GetType() const;

	// Throws an exception if the value is not an array
	vector<JsonValue> &Items();

	// Throws an exception if the value is not an array
	const vector<JsonValue> &Items() const;

	// Throws an exception if the value is not an object
	unordered_map<string, JsonValue> &Properties();

	// Throws an exception if the value is not an object
	const unordered_map<string, JsonValue> &Properties() const;

	// Throws an exception if the value is not an object
	JsonValue &operator[](const string &key);

	// Throws an exception if the value is not an object or the key is not found
	const JsonValue &operator[](const string &key) const;

	// Throws an exception if the index is out of bounds or the value is not an array
	JsonValue &operator[](idx_t index);

	// Throws an exception if the index is out of bounds or the value is not an array
	const JsonValue &operator[](idx_t index) const;

	bool IsNull() const;

	template <class T>
	bool Is() const & = delete;

	template <class T>
	T &As() & = delete;

	template <class T>
	const T &As() const & = delete;

	bool GetBool() const;
	double GetNumber() const;
	string GetString() const;
	vector<JsonValue> GetArray() const;
	unordered_map<string, JsonValue> GetObject() const;

	// Throws an exception if the value is not an array
	JsonValue &Push(const JsonValue &value);

	// Throws an exception if the value is not an object
	JsonValue &Push(const string &key, const JsonValue &value);

	// Throws an exception if the value is not an array
	JsonValue &Emplace(JsonValue &&value);

	// Throws an exception if the value is not an object
	JsonValue &Emplace(const string &key, JsonValue &&value);

	// Throws an exception if the value is not an array or object
	idx_t Count() const;

	JsonValue(const JsonValue &value);
	JsonValue(JsonValue &&value) noexcept;
	JsonValue &operator=(const JsonValue &value);
	JsonValue &operator=(JsonValue &&value) noexcept;
	~JsonValue();
};

//------------------------------------------------------------------------------
// Primitive Constructors
//------------------------------------------------------------------------------

inline JsonValue::JsonValue() : JsonValue(JsonKind::OBJECT) {
}

inline JsonValue::JsonValue(JsonKind kind) : kind(kind) {
	switch (kind) {
	case JsonKind::BOOLEAN:
		bool_value = false;
		break;
	case JsonKind::NUMBER:
		num_value = 0;
		break;
	case JsonKind::STRING:
		str_value = new string();
		break;
	case JsonKind::OBJECT:
		object_value = new unordered_map<string, JsonValue>();
		break;
	case JsonKind::ARRAY:
		array_value = new vector<JsonValue>();
		break;
	case JsonKind::NULLVALUE:
		break;
	default:
		throw InvalidInputException("Unrecognized JSON kind!");
	}
}

inline JsonValue::JsonValue(const string &value) : kind(JsonKind::STRING) {
	str_value = new string(value);
}

inline JsonValue::JsonValue(const char *value) : kind(JsonKind::STRING) {
	str_value = new string(value);
}

inline JsonValue::JsonValue(double value) : kind(JsonKind::NUMBER) {
	num_value = value;
}

inline JsonValue::JsonValue(bool value) : kind(JsonKind::BOOLEAN) {
	bool_value = value;
}

inline JsonValue::JsonValue(const unordered_map<string, JsonValue> &value) : kind(JsonKind::OBJECT) {
	object_value = new unordered_map<string, JsonValue>(value);
}

inline JsonValue::JsonValue(const vector<JsonValue> &value) : kind(JsonKind::ARRAY) {
	array_value = new vector<JsonValue>(value);
}

//------------------------------------------------------------------------------
// Accessors
//------------------------------------------------------------------------------

inline JsonKind JsonValue::GetType() const {
	return kind;
}

inline vector<JsonValue> &JsonValue::Items() {
	if (kind != JsonKind::ARRAY) {
		throw InvalidTypeException("Cannot access Items on non-ARRAY JSON value!");
	}
	return *array_value;
}

inline const vector<JsonValue> &JsonValue::Items() const {
	if (kind != JsonKind::ARRAY) {
		throw InvalidTypeException("Cannot access Items on non-ARRAY JSON value!");
	}
	return *array_value;
}

inline unordered_map<string, JsonValue> &JsonValue::Properties() {
	if (kind != JsonKind::OBJECT) {
		throw InvalidTypeException("Cannot access Properties on non-OBJECT JSON value!");
	}
	return *object_value;
}

inline const unordered_map<string, JsonValue> &JsonValue::Properties() const {
	if (kind != JsonKind::OBJECT) {
		throw InvalidTypeException("Cannot access Properties on non-OBJECT JSON value!");
	}
	return *object_value;
}

inline JsonValue &JsonValue::operator[](const string &key) {
	if (kind != JsonKind::OBJECT) {
		throw InvalidTypeException("Cannot access Properties on non-OBJECT JSON value!");
	}
	return (*object_value)[key];
}

inline const JsonValue &JsonValue::operator[](const string &key) const {
	if (kind != JsonKind::OBJECT) {
		throw InvalidTypeException("Cannot access Properties on non-OBJECT JSON value!");
	}
	auto entry = object_value->find(key);
	if (entry == object_value->end()) {
		throw InvalidInputException("Key not found in JSON object!");
	}
	return entry->second;
}

inline JsonValue &JsonValue::operator[](idx_t index) {
	if (kind != JsonKind::ARRAY) {
		throw InvalidTypeException("Cannot access Items on non-ARRAY JSON value!");
	}
	if (index >= array_value->size()) {
		throw InvalidInputException("Index out of bounds in JSON array!");
	}
	return (*array_value)[index];
}

inline const JsonValue &JsonValue::operator[](idx_t index) const {
	if (kind != JsonKind::ARRAY) {
		throw InvalidTypeException("Cannot access Items on non-ARRAY JSON value!");
	}
	if (index >= array_value->size()) {
		throw InvalidInputException("Index out of bounds in JSON array!");
	}
	return (*array_value)[index];
}

inline bool JsonValue::IsNull() const {
	return kind == JsonKind::NULLVALUE;
}

template <>
inline bool JsonValue::Is<bool>() const & {
	return kind == JsonKind::BOOLEAN;
}

template <>
inline bool JsonValue::Is<double>() const & {
	return kind == JsonKind::NUMBER;
}

template <>
inline bool JsonValue::Is<string>() const & {
	return kind == JsonKind::STRING;
}

template <>
inline double &JsonValue::As() & {
	if (kind != JsonKind::NUMBER) {
		throw InvalidTypeException("Cannot convert JSON value to DOUBLE!");
	}
	return num_value;
}

template <>
inline const double &JsonValue::As() const & {
	if (kind != JsonKind::NUMBER) {
		throw InvalidTypeException("Cannot convert JSON value to DOUBLE!");
	}
	return num_value;
}

template <>
inline bool &JsonValue::As() & {
	if (kind != JsonKind::BOOLEAN) {
		throw InvalidTypeException("Cannot convert JSON value to BOOLEAN!");
	}
	return bool_value;
}

template <>
inline const bool &JsonValue::As() const & {
	if (kind != JsonKind::BOOLEAN) {
		throw InvalidTypeException("Cannot convert JSON value to BOOLEAN!");
	}
	return bool_value;
}

template <>
inline string &JsonValue::As() & {
	if (kind != JsonKind::STRING) {
		throw InvalidTypeException("Cannot convert JSON value to STRING!");
	}
	return *str_value;
}

template <>
inline const string &JsonValue::As() const & {
	if (kind != JsonKind::STRING) {
		throw InvalidTypeException("Cannot convert JSON value to STRING!");
	}
	return *str_value;
}

inline bool JsonValue::GetBool() const {
	if (kind != JsonKind::BOOLEAN) {
		throw InvalidTypeException("Cannot convert JSON value to BOOLEAN!");
	}
	return bool_value;
}

inline double JsonValue::GetNumber() const {
	if (kind != JsonKind::NUMBER) {
		throw InvalidTypeException("Cannot convert JSON value to DOUBLE!");
	}
	return num_value;
}

inline string JsonValue::GetString() const {
	if (kind != JsonKind::STRING) {
		throw InvalidTypeException("Cannot convert JSON value to STRING!");
	}
	return *str_value;
}

inline vector<JsonValue> JsonValue::GetArray() const {
	if (kind != JsonKind::ARRAY) {
		throw InvalidTypeException("Cannot convert JSON value to ARRAY!");
	}
	return *array_value;
}

inline unordered_map<string, JsonValue> JsonValue::GetObject() const {
	if (kind != JsonKind::OBJECT) {
		throw InvalidTypeException("Cannot convert JSON value to OBJECT!");
	}
	return *object_value;
}

//------------------------------------------------------------------------------
// Array and Object Methods
//------------------------------------------------------------------------------

inline JsonValue &JsonValue::Push(const JsonValue &value) {
	if (kind != JsonKind::ARRAY) {
		throw InvalidTypeException("Cannot push to non-ARRAY JSON value!");
	}
	array_value->push_back(value);
	return array_value->back();
}

inline JsonValue &JsonValue::Push(const string &key, const JsonValue &value) {
	if (kind != JsonKind::OBJECT) {
		throw InvalidTypeException("Cannot push to non-OBJECT JSON value!");
	}
	return object_value->emplace(key, value).first->second;
}

inline JsonValue &JsonValue::Emplace(JsonValue &&value) {
	if (kind != JsonKind::ARRAY) {
		throw InvalidTypeException("Cannot emplace to non-ARRAY JSON value!");
	}
	array_value->emplace_back(std::forward<JsonValue>(value));
	return array_value->back();
}

inline JsonValue &JsonValue::Emplace(const string &key, JsonValue &&value) {
	if (kind != JsonKind::OBJECT) {
		throw InvalidTypeException("Cannot emplace to non-OBJECT JSON value!");
	}
	return object_value->emplace(key, std::forward<JsonValue>(value)).first->second;
}

inline idx_t JsonValue::Count() const {
	switch (kind) {
	case JsonKind::ARRAY:
		return array_value->size();
	case JsonKind::OBJECT:
		return object_value->size();
	default:
		throw InvalidTypeException("Cannot get count of non-ARRAY/OBJECT JSON value!");
	}
}

//------------------------------------------------------------------------------
// Copy, Move and Destructor
//------------------------------------------------------------------------------
inline JsonValue::JsonValue(const JsonValue &value) : kind(value.kind) {
	switch (kind) {
	case JsonKind::STRING:
		str_value = new string(*value.str_value);
		break;
	case JsonKind::OBJECT:
		object_value = new unordered_map<string, JsonValue>(*value.object_value);
		break;
	case JsonKind::ARRAY:
		array_value = new vector<JsonValue>(*value.array_value);
		break;
	default:
		break;
	}
}

inline JsonValue::JsonValue(JsonValue &&value) noexcept : kind(value.kind) {
	switch (kind) {
	case JsonKind::STRING:
		str_value = value.str_value;
		value.str_value = nullptr;
		break;
	case JsonKind::OBJECT:
		object_value = value.object_value;
		value.object_value = nullptr;
		break;
	case JsonKind::ARRAY:
		array_value = value.array_value;
		value.array_value = nullptr;
		break;
	default:
		break;
	}
	value.kind = JsonKind::NULLVALUE;
}

inline JsonValue &JsonValue::operator=(const JsonValue &value) {
	if (&value == this) {
		return *this;
	}
	this->~JsonValue();
	kind = value.kind;
	switch (kind) {
	case JsonKind::STRING:
		str_value = new string(*value.str_value);
		break;
	case JsonKind::OBJECT:
		object_value = new unordered_map<string, JsonValue>(*value.object_value);
		break;
	case JsonKind::ARRAY:
		array_value = new vector<JsonValue>(*value.array_value);
		break;
	default:
		break;
	}
	return *this;
}

inline JsonValue &JsonValue::operator=(JsonValue &&value) noexcept {
	if (&value == this) {
		return *this;
	}
	this->~JsonValue();
	kind = value.kind;
	switch (kind) {
	case JsonKind::STRING:
		str_value = value.str_value;
		value.str_value = nullptr;
		break;
	case JsonKind::OBJECT:
		object_value = value.object_value;
		value.object_value = nullptr;
		break;
	case JsonKind::ARRAY:
		array_value = value.array_value;
		value.array_value = nullptr;
		break;
	default:
		break;
	}
	value.kind = JsonKind::NULLVALUE;
	return *this;
}

inline JsonValue::~JsonValue() {
	switch (kind) {
	case JsonKind::STRING:
		delete str_value;
		break;
	case JsonKind::OBJECT:
		delete object_value;
		break;
	case JsonKind::ARRAY:
		delete array_value;
		break;
	default:
		break;
	}
}

} // namespace duckdb
