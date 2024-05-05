#pragma once

#include "duckdb/common/typedefs.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/unordered_map.hpp"

namespace duckdb {

enum class JsonKind : uint8_t {
	NULLVALUE = 0,
	BOOLEAN,
	NUMBER,
	STRING,
	ARRAY,
	OBJECT,
};

class JsonValue;

using JsonMap = unordered_map<string, JsonValue>;
using JsonArray = vector<JsonValue>;

class JsonValue {
	JsonKind kind;
	union {
		bool bool_value;
		double num_value;
		string *str_value;
		JsonArray *array_value;
		JsonMap *object_value;
	};

public:
	JsonValue();
	// NOLINTBEGIN
	JsonValue(JsonKind kind);
	JsonValue(const string &value);
	JsonValue(const char *value);
	JsonValue(double value);
	JsonValue(bool value);
	JsonValue(const JsonMap &value);
	JsonValue(const JsonArray &value);
	// NOLINTEND

	// Parses a JSON value from a string
	static JsonValue Parse(const string &str);
	// Parses multiple JSON values from a string (e.g. a newline-separated JSON file)
	static JsonArray ParseMany(const string &str);

	// Returns a string representation of the JSON value, optionally formatted with newlines and tabs
	string ToString(bool format = false) const;

	JsonKind GetType() const;

	// Throws an exception if the value is not an object
	JsonValue &operator[](const string &key);

	// Throws an exception if the value is not an object or the key is not found
	const JsonValue &operator[](const string &key) const;

	// Throws an exception if the index is out of bounds or the value is not an array
	JsonValue &operator[](idx_t index);

	// Throws an exception if the index is out of bounds or the value is not an array
	const JsonValue &operator[](idx_t index) const;

	template <class T>
	bool Is() const & = delete;
	bool IsNull() const &;
	bool IsBool() const &;
	bool IsNumber() const &;
	bool IsString() const &;
	bool IsArray() const &;
	bool IsObject() const &;

	template <class T>
	T &As() & = delete;
	bool &AsBool() &;
	double &AsNumber() &;
	string &AsString() &;
	JsonArray &AsArray() &;
	JsonMap &AsObject() &;

	template <class T>
	const T &As() const & = delete;
	const bool &AsBool() const &;
	const double &AsNumber() const &;
	const string &AsString() const &;
	const JsonArray &AsArray() const &;
	const JsonMap &AsObject() const &;

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
	case JsonKind::NULLVALUE:
		break;
	case JsonKind::BOOLEAN:
		bool_value = false;
		break;
	case JsonKind::NUMBER:
		num_value = 0;
		break;
	case JsonKind::STRING:
		str_value = new string();
		break;
	case JsonKind::ARRAY:
		array_value = new JsonArray();
		break;
	case JsonKind::OBJECT:
		object_value = new JsonMap();
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

inline JsonValue::JsonValue(const JsonMap &value) : kind(JsonKind::OBJECT) {
	object_value = new JsonMap(value);
}

inline JsonValue::JsonValue(const JsonArray &value) : kind(JsonKind::ARRAY) {
	array_value = new JsonArray(value);
}

//------------------------------------------------------------------------------
// Accessors
//------------------------------------------------------------------------------

inline JsonKind JsonValue::GetType() const {
	return kind;
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

//------------------------------------------------------------------------------
// Is
//------------------------------------------------------------------------------

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
inline bool JsonValue::Is<JsonArray>() const & {
	return kind == JsonKind::ARRAY;
}

template <>
inline bool JsonValue::Is<JsonMap>() const & {
	return kind == JsonKind::OBJECT;
}

inline bool JsonValue::IsNull() const & {
	return kind == JsonKind::NULLVALUE;
}

inline bool JsonValue::IsBool() const & {
	return Is<bool>();
}

inline bool JsonValue::IsNumber() const & {
	return Is<double>();
}

inline bool JsonValue::IsString() const & {
	return Is<string>();
}

inline bool JsonValue::IsArray() const & {
	return Is<JsonArray>();
}

inline bool JsonValue::IsObject() const & {
	return Is<JsonMap>();
}

//------------------------------------------------------------------------------
// As Methods
//------------------------------------------------------------------------------

template <>
inline double &JsonValue::As() & {
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
inline string &JsonValue::As() & {
	if (kind != JsonKind::STRING) {
		throw InvalidTypeException("Cannot convert JSON value to STRING!");
	}
	return *str_value;
}

template <>
inline JsonArray &JsonValue::As() & {
	if (kind != JsonKind::ARRAY) {
		throw InvalidTypeException("Cannot convert JSON value to ARRAY!");
	}
	return *array_value;
}

template <>
inline JsonMap &JsonValue::As() & {
	if (kind != JsonKind::OBJECT) {
		throw InvalidTypeException("Cannot convert JSON value to OBJECT!");
	}
	return *object_value;
}

inline double &JsonValue::AsNumber() & {
	return As<double>();
}

inline bool &JsonValue::AsBool() & {
	return As<bool>();
}

inline string &JsonValue::AsString() & {
	return As<string>();
}

inline JsonArray &JsonValue::AsArray() & {
	return As<JsonArray>();
}

inline JsonMap &JsonValue::AsObject() & {
	return As<JsonMap>();
}

//------------------------------------------------------------------------------
// As (const) Methods
//------------------------------------------------------------------------------

template <>
inline const bool &JsonValue::As() const & {
	if (kind != JsonKind::BOOLEAN) {
		throw InvalidTypeException("Cannot convert JSON value to BOOLEAN!");
	}
	return bool_value;
}

template <>
inline const double &JsonValue::As() const & {
	if (kind != JsonKind::NUMBER) {
		throw InvalidTypeException("Cannot convert JSON value to DOUBLE!");
	}
	return num_value;
}

template <>
inline const string &JsonValue::As() const & {
	if (kind != JsonKind::STRING) {
		throw InvalidTypeException("Cannot convert JSON value to STRING!");
	}
	return *str_value;
}

template <>
inline const JsonArray &JsonValue::As() const & {
	if (kind != JsonKind::ARRAY) {
		throw InvalidTypeException("Cannot convert JSON value to ARRAY!");
	}
	return *array_value;
}

template <>
inline const JsonMap &JsonValue::As() const & {
	if (kind != JsonKind::OBJECT) {
		throw InvalidTypeException("Cannot convert JSON value to OBJECT!");
	}
	return *object_value;
}

inline const double &JsonValue::AsNumber() const & {
	return As<double>();
}

inline const bool &JsonValue::AsBool() const & {
	return As<bool>();
}

inline const string &JsonValue::AsString() const & {
	return As<string>();
}

inline const JsonArray &JsonValue::AsArray() const & {
	return As<JsonArray>();
}

inline const JsonMap &JsonValue::AsObject() const & {
	return As<JsonMap>();
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
	case JsonKind::BOOLEAN:
		bool_value = value.bool_value;
		break;
	case JsonKind::NUMBER:
		num_value = value.num_value;
		break;
	case JsonKind::STRING:
		str_value = new string(*value.str_value);
		break;
	case JsonKind::ARRAY:
		array_value = new JsonArray(*value.array_value);
		break;
	case JsonKind::OBJECT:
		object_value = new JsonMap(*value.object_value);
		break;
	default:
		break;
	}
}

inline JsonValue::JsonValue(JsonValue &&value) noexcept : kind(value.kind) {
	switch (kind) {
	case JsonKind::BOOLEAN:
		bool_value = value.bool_value;
		break;
	case JsonKind::NUMBER:
		num_value = value.num_value;
		break;
	case JsonKind::STRING:
		str_value = value.str_value;
		value.str_value = nullptr;
		break;
	case JsonKind::ARRAY:
		array_value = value.array_value;
		value.array_value = nullptr;
		break;
	case JsonKind::OBJECT:
		object_value = value.object_value;
		value.object_value = nullptr;
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
	case JsonKind::BOOLEAN:
		bool_value = value.bool_value;
		break;
	case JsonKind::NUMBER:
		num_value = value.num_value;
		break;
	case JsonKind::STRING:
		str_value = new string(*value.str_value);
		break;
	case JsonKind::ARRAY:
		array_value = new JsonArray(*value.array_value);
		break;
	case JsonKind::OBJECT:
		object_value = new JsonMap(*value.object_value);
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
	case JsonKind::BOOLEAN:
		bool_value = value.bool_value;
		break;
	case JsonKind::NUMBER:
		num_value = value.num_value;
		break;
	case JsonKind::STRING:
		str_value = value.str_value;
		value.str_value = nullptr;
		break;
	case JsonKind::ARRAY:
		array_value = value.array_value;
		value.array_value = nullptr;
		break;
	case JsonKind::OBJECT:
		object_value = value.object_value;
		value.object_value = nullptr;
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
