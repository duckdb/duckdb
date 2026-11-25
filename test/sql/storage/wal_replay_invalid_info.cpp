#include "catch.hpp"
#include "duckdb/common/checksum.hpp"
#include "duckdb/common/local_file_system.hpp"
#include "duckdb/common/serializer/binary_serializer.hpp"
#include "duckdb/common/serializer/memory_stream.hpp"
#include "duckdb/common/enums/wal_type.hpp"
#include "duckdb/main/settings.hpp"
#include "duckdb/parser/parsed_data/alter_table_info.hpp"
#include "duckdb/parser/parsed_data/transaction_info.hpp"
#include "test_helpers.hpp"

using namespace duckdb;

//! Append a framed [size][checksum][payload] entry to the WAL
static void AppendWALEntry(FileSystem &fs, const string &path, MemoryStream &payload) {
	auto handle = fs.OpenFile(path, FileFlags::FILE_FLAGS_WRITE | FileFlags::FILE_FLAGS_APPEND);
	uint64_t size = payload.GetPosition();
	uint64_t checksum = Checksum(payload.GetData(), size);
	handle->Write(QueryContext(), &size, sizeof(uint64_t));
	handle->Write(QueryContext(), &checksum, sizeof(uint64_t));
	handle->Write(QueryContext(), payload.GetData(), size);
}

//! Replay a WAL whose only new entry is written by the callback and followed by a flush, expecting the given error
static void TestReplayInvalidEntry(const string &name, const std::function<void(Serializer &)> &write_entry,
                                   const string &expected_error) {
	auto config = GetTestConfig();
	auto storage_database = TestCreatePath(name);
	auto storage_wal = storage_database + ".wal";

	LocalFileSystem lfs;
	config->options.checkpoint_wal_size = idx_t(-1);
	config->SetOption(CheckpointOnShutdownSetting::SettingIndex, Value::BOOLEAN(false));
	// a deserialization error in an entry is otherwise taken for the torn end of the WAL
	config->SetOption(DebugAbortOnWalFailureSetting::SettingIndex, Value::BOOLEAN(true));
	DeleteDatabase(storage_database);
	{
		DuckDB db(storage_database, config.get());
		Connection con(db);
		REQUIRE_NO_FAIL(con.Query("CREATE TABLE t (i INTEGER);"));
	}
	REQUIRE(lfs.FileExists(storage_wal));

	// the entry, followed by a flush
	{
		MemoryStream entry;
		BinarySerializer serializer(entry);
		serializer.Begin();
		write_entry(serializer);
		serializer.End();
		AppendWALEntry(lfs, storage_wal, entry);
	}
	{
		MemoryStream flush_entry;
		BinarySerializer serializer(flush_entry);
		serializer.Begin();
		serializer.WriteProperty(100, "wal_type", WALType::WAL_FLUSH);
		serializer.End();
		AppendWALEntry(lfs, storage_wal, flush_entry);
	}
	REQUIRE_THROWS_WITH(DuckDB(storage_database, config.get()), Catch::Contains(expected_error));
	DeleteDatabase(storage_database);
}

//! Replay a WAL whose only new entry is an ALTER_INFO with the given info, expecting the given error
static void TestReplayInvalidAlterInfo(const string &name, unique_ptr<ParseInfo> info, const string &expected_error) {
	TestReplayInvalidEntry(
	    name,
	    [&](Serializer &serializer) {
		    serializer.WriteProperty(100, "wal_type", WALType::ALTER_INFO);
		    serializer.WriteProperty(101, "info", info);
	    },
	    expected_error);
}

TEST_CASE("Test WAL alter entry carrying non-alter information", "[storage][.]") {
	TestReplayInvalidAlterInfo("wal_alter_wrong_info", make_uniq<TransactionInfo>(TransactionType::BEGIN_TRANSACTION),
	                           "alter entry does not carry alter information");
}

TEST_CASE("Test WAL add constraint entry without a constraint", "[storage][.]") {
	AlterEntryData data(QualifiedName(Identifier(""), Identifier("main"), Identifier("t")),
	                    OnEntryNotFound::THROW_EXCEPTION);
	TestReplayInvalidAlterInfo("wal_add_constraint_without_constraint", make_uniq<AddConstraintInfo>(data, nullptr),
	                           "add constraint entry is missing its constraint");
}

TEST_CASE("Test WAL add column entry with a type that no column can have", "[storage][.]") {
	AlterEntryData data(QualifiedName(Identifier(""), Identifier("main"), Identifier("t")),
	                    OnEntryNotFound::THROW_EXCEPTION);
	// a type without a physical type, as a column and as the child of a LIST
	TestReplayInvalidAlterInfo(
	    "wal_add_column_literal_type",
	    make_uniq<AddColumnInfo>(data, ColumnDefinition(Identifier("c"), LogicalType(LogicalTypeId::INTEGER_LITERAL)),
	                             false, AddColumnConstraints()),
	    "add column entry with a type that no column can have");
	TestReplayInvalidAlterInfo(
	    "wal_add_column_list_of_literal_type",
	    make_uniq<AddColumnInfo>(
	        data, ColumnDefinition(Identifier("c"), LogicalType::LIST(LogicalType(LogicalTypeId::INTEGER_LITERAL))),
	        false, AddColumnConstraints()),
	    "add column entry with a type that no column can have");
}

//! Replay a WAL whose only new entry is an INSERT_TUPLE with a chunk of one column, whose type the callback writes
static void TestReplayInvalidInsertType(const string &name, const std::function<void(Serializer &)> &write_type,
                                        const string &expected_error) {
	TestReplayInvalidEntry(
	    name,
	    [&](Serializer &serializer) {
		    serializer.WriteProperty(100, "wal_type", WALType::INSERT_TUPLE);
		    serializer.WriteObject(101, "chunk", [&](Serializer &chunk) {
			    chunk.WriteProperty<sel_t>(100, "rows", 1);
			    chunk.WriteList(101, "types", 1, [&](Serializer::List &list, idx_t) { list.WriteObject(write_type); });
			    chunk.WriteList(102, "columns", 0, [&](Serializer::List &, idx_t) {});
		    });
	    },
	    expected_error);
}

TEST_CASE("Test WAL insert entry with a chunk of a type that no vector can hold", "[storage][.]") {
	TestReplayInvalidInsertType(
	    "wal_insert_literal_type",
	    [&](Serializer &type) { LogicalType(LogicalTypeId::INTEGER_LITERAL).Serialize(type); },
	    "a vector cannot hold the type INTEGER_LITERAL");
	// an ENUM without its values: the error names the type by its id
	TestReplayInvalidInsertType(
	    "wal_insert_enum_without_values",
	    [&](Serializer &type) { type.WriteProperty<LogicalTypeId>(100, "id", LogicalTypeId::ENUM); }, "type ENUM");
	// a VARIANT whose child types are not those of a VARIANT
	auto child_types = LogicalType::STRUCT({{"keys", LogicalType(LogicalTypeId::INTEGER_LITERAL)}});
	TestReplayInvalidInsertType(
	    "wal_insert_variant_child_types",
	    [&](Serializer &type) {
		    type.WriteProperty<LogicalTypeId>(100, "id", LogicalTypeId::VARIANT);
		    optional_ptr<const LogicalTypeInfo> type_info = &child_types.GetTypeInfo();
		    type.WritePropertyWithDefault<optional_ptr<const LogicalTypeInfo>>(101, "type_info", type_info);
	    },
	    "a vector cannot hold the type VARIANT");
}

TEST_CASE("Test WAL drop trigger entry without a base table", "[storage][.]") {
	TestReplayInvalidEntry(
	    "wal_drop_trigger_without_table",
	    [&](Serializer &serializer) {
		    serializer.WriteProperty(100, "wal_type", WALType::DROP_TRIGGER);
		    serializer.WritePropertyWithDefault<Identifier>(101, "schema", Identifier("main"));
		    serializer.WritePropertyWithDefault<Identifier>(102, "name", Identifier("trg"));
	    },
	    "drop trigger entry is missing its base table");
}
