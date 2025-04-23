#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/common/file_system.hpp"
#include "duckdb/common/helper.hpp"
#include "duckdb/common/serializer/binary_deserializer.hpp"
#include "duckdb/common/serializer/buffered_file_reader.hpp"
#include "duckdb/main/connection.hpp"
#include "duckdb/parser/parsed_data/parse_info.hpp"

namespace duckdb {

class AttachedDatabase;
class ReplayState;
class WriteAheadLogDeserializer;

//! WALReader is a class that reads and deserializes entries from a Write-Ahead Log file
class WALReader {
public:
	//! Creates a WALReader that reads from the specified database and file handle
	explicit WALReader(AttachedDatabase &database, unique_ptr<BufferedFileReader> reader);

	//! Reads and returns the next entry from the WAL file
	//! Returns nullptr if there are no more entries
	std::unique_ptr<ParseInfo> Next();

	~WALReader();

private:
	unique_ptr<ReplayState> state;
	unique_ptr<BufferedFileReader> reader;
};

} // namespace duckdb
