//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/json_document.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/typedefs.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/unique_ptr.hpp"
#include <functional>

//! Forward declaration of the yyjson types - the actual definitions live in the yyjson headers, which are only
//! included in the implementation. This keeps yyjson an internal detail of the wrapper.
namespace duckdb_yyjson { // NOLINT
struct yyjson_doc;
struct yyjson_val;
struct yyjson_mut_doc;
struct yyjson_mut_val;
} // namespace duckdb_yyjson

namespace duckdb {

//! The type of a JSON value
enum class JSONValueType : uint8_t {
	INVALID,          //!< Not a valid value (e.g. the result of a failed lookup)
	JSON_NULL,        //!< JSON null
	BOOLEAN,          //!< true / false
	UNSIGNED_INTEGER, //!< an integer that fits in a uint64_t
	SIGNED_INTEGER,   //!< an integer that fits in an int64_t
	DOUBLE,           //!< a floating point number
	STRING,           //!< a string
	ARRAY,            //!< a JSON array
	OBJECT,           //!< a JSON object
	RAW               //!< raw (unparsed) number, e.g. a bignum
};

//! Flags controlling how JSON is parsed
enum class JSONReadFlags : uint32_t {
	NONE = 0,
	ALLOW_INVALID_UNICODE = 1 << 0,
	ALLOW_INF_AND_NAN = 1 << 1,
	ALLOW_TRAILING_COMMAS = 1 << 2,
	BIGNUM_AS_RAW = 1 << 3
};

constexpr JSONReadFlags operator|(JSONReadFlags a, JSONReadFlags b) {
	return static_cast<JSONReadFlags>(static_cast<uint32_t>(a) | static_cast<uint32_t>(b));
}

//! Flags controlling how JSON is written
enum class JSONWriteFlags : uint32_t { NONE = 0, ALLOW_INVALID_UNICODE = 1 << 0 };

//! A read-only handle to a single value inside a JSONDocument.
//! NOTE: a JSONValue is only valid for as long as the JSONDocument it originates from is alive.
class JSONValue {
	friend class JSONDocument;

public:
	//! Constructs an invalid value
	JSONValue();

	//! Whether or not this refers to an actual value
	bool IsValid() const;
	//! The type of this value
	JSONValueType GetType() const;

	bool IsNull() const;
	bool IsString() const;
	bool IsArray() const;
	bool IsObject() const;

	//! Get the value as a string (only valid if IsString())
	string GetString() const;
	//! Get the value as a boolean
	bool GetBoolean() const;
	//! Get the value as an unsigned integer
	uint64_t GetUnsignedInteger() const;
	//! Get the value as a signed integer
	int64_t GetSignedInteger() const;
	//! Get the value as a double
	double GetDouble() const;

	//! Iterate over the elements of an array
	void IterateArray(const std::function<void(JSONValue)> &callback) const;
	//! Iterate over the key/value pairs of an object
	void IterateObject(const std::function<void(const string &, JSONValue)> &callback) const;

private:
	explicit JSONValue(duckdb_yyjson::yyjson_val *val);

private:
	//! The wrapped yyjson value - owned by the originating JSONDocument
	duckdb_yyjson::yyjson_val *val;
};

//! Error information from parsing a JSON document
struct JSONParseError {
	//! Whether or not an error occurred
	bool HasError() const {
		return has_error;
	}

	bool has_error = false;
	//! Byte position of the error in the input
	idx_t position = 0;
	//! Human-readable error message
	string message;
};

//! Owns an immutable, parsed JSON document. Values obtained from it are valid for as long as the document is alive.
class JSONDocument {
public:
	JSONDocument();
	~JSONDocument();
	// non-copyable, movable
	JSONDocument(const JSONDocument &) = delete;
	JSONDocument &operator=(const JSONDocument &) = delete;
	JSONDocument(JSONDocument &&other) noexcept;
	JSONDocument &operator=(JSONDocument &&other) noexcept;

	//! Parse the given input. Returns nullptr on failure, in which case "error" is populated.
	static unique_ptr<JSONDocument> Parse(const char *data, idx_t len, JSONReadFlags flags, JSONParseError &error);

	//! The root value of the document
	JSONValue GetRoot() const;

private:
	//! The wrapped yyjson document
	duckdb_yyjson::yyjson_doc *doc;
};

//! Builds a JSON document and serializes it to a string. The root is either an object or an array.
class JSONWriter {
public:
	~JSONWriter();
	// non-copyable, movable
	JSONWriter(const JSONWriter &) = delete;
	JSONWriter &operator=(const JSONWriter &) = delete;
	JSONWriter(JSONWriter &&other) noexcept;
	JSONWriter &operator=(JSONWriter &&other) noexcept;

	//! Create a writer with an object as the root value
	static JSONWriter CreateObject();
	//! Create a writer with an array as the root value
	static JSONWriter CreateArray();

	//! Add a string key/value pair to the (object) root
	void AddString(const string &key, const string &value);
	//! Append a string value to the (array) root
	void AppendString(const string &value);

	//! Serialize the document to a string
	string ToString(JSONWriteFlags flags = JSONWriteFlags::NONE) const;

private:
	JSONWriter();

private:
	//! The wrapped mutable yyjson document
	duckdb_yyjson::yyjson_mut_doc *doc;
	//! The root value of the mutable document - owned by "doc"
	duckdb_yyjson::yyjson_mut_val *root;
};

} // namespace duckdb
