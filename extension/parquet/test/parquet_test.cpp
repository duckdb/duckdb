#define CATCH_CONFIG_MAIN
#include "catch.hpp"
#include "parquet-extension.hpp"
#include "duckdb.hpp"
#include "test_helpers.hpp"
#include "duckdb/common/types/timestamp.hpp"

//#include "dbgen.hpp"

using namespace duckdb;
//
//TEST_CASE("Test basic parquet reading", "[parquet]") {
//	DuckDB db(nullptr);
//	db.LoadExtension<ParquetExtension>();
//
//	Connection con(db);
//
//	con.EnableQueryVerification();
//
//	SECTION("Exception on missing file") {
//		REQUIRE_FAIL(con.Query("SELECT * FROM parquet_scan('does_not_exist')"));
//	}
//
//	SECTION("alltypes_plain.parquet") {
//		auto result = con.Query("SELECT * FROM parquet_scan('extension/parquet/test/alltypes_plain.parquet')");
//		REQUIRE(CHECK_COLUMN(result, 0, {4, 5, 6, 7, 2, 3, 0, 1}));
//		REQUIRE(CHECK_COLUMN(result, 1, {true, false, true, false, true, false, true, false}));
//		REQUIRE(CHECK_COLUMN(result, 2, {0, 1, 0, 1, 0, 1, 0, 1}));
//		REQUIRE(CHECK_COLUMN(result, 3, {0, 1, 0, 1, 0, 1, 0, 1}));
//		REQUIRE(CHECK_COLUMN(result, 4, {0, 1, 0, 1, 0, 1, 0, 1}));
//		REQUIRE(CHECK_COLUMN(result, 5, {0, 10, 0, 10, 0, 10, 0, 10}));
//		REQUIRE(CHECK_COLUMN(result, 6, {0.0, 1.1, 0.0, 1.1, 0.0, 1.1, 0.0, 1.1}));
//		REQUIRE(CHECK_COLUMN(result, 7, {0.0, 10.1, 0.0, 10.1, 0.0, 10.1, 0.0, 10.1}));
//		REQUIRE(CHECK_COLUMN(
//		    result, 8,
//		    {"03/01/09", "03/01/09", "04/01/09", "04/01/09", "02/01/09", "02/01/09", "01/01/09", "01/01/09"}));
//		REQUIRE(CHECK_COLUMN(result, 9, {"0", "1", "0", "1", "0", "1", "0", "1"}));
//
//		REQUIRE(CHECK_COLUMN(result, 10,
//		                     {Value::BIGINT(Timestamp::FromString("2009-03-01 00:00:00")),
//		                      Value::BIGINT(Timestamp::FromString("2009-03-01 00:01:00")),
//		                      Value::BIGINT(Timestamp::FromString("2009-04-01 00:00:00")),
//		                      Value::BIGINT(Timestamp::FromString("2009-04-01 00:01:00")),
//		                      Value::BIGINT(Timestamp::FromString("2009-02-01 00:00:00")),
//		                      Value::BIGINT(Timestamp::FromString("2009-02-01 00:01:00")),
//		                      Value::BIGINT(Timestamp::FromString("2009-01-01 00:00:00")),
//		                      Value::BIGINT(Timestamp::FromString("2009-01-01 00:01:00"))}));
//	}
//
//	SECTION("alltypes_plain.snappy.parquet") {
//		auto result = con.Query("SELECT * FROM parquet_scan('extension/parquet/test/alltypes_plain.snappy.parquet')");
//		REQUIRE(CHECK_COLUMN(result, 0, {6, 7}));
//		REQUIRE(CHECK_COLUMN(result, 1, {true, false}));
//		REQUIRE(CHECK_COLUMN(result, 2, {0, 1}));
//		REQUIRE(CHECK_COLUMN(result, 3, {0, 1}));
//		REQUIRE(CHECK_COLUMN(result, 4, {0, 1}));
//		REQUIRE(CHECK_COLUMN(result, 5, {0, 10}));
//		REQUIRE(CHECK_COLUMN(result, 6, {0.0, 1.1}));
//		REQUIRE(CHECK_COLUMN(result, 7, {0.0, 10.1}));
//		REQUIRE(CHECK_COLUMN(result, 8, {"04/01/09", "04/01/09"}));
//		REQUIRE(CHECK_COLUMN(result, 9, {"0", "1"}));
//		REQUIRE(CHECK_COLUMN(result, 10,
//		                     {Value::BIGINT(Timestamp::FromString("2009-04-01 00:00:00")),
//		                      Value::BIGINT(Timestamp::FromString("2009-04-01 00:01:00"))}));
//	}
//
//	SECTION("alltypes_dictionary.parquet") {
//		auto result = con.Query("SELECT * FROM parquet_scan('extension/parquet/test/alltypes_dictionary.parquet')");
//
//		REQUIRE(CHECK_COLUMN(result, 0, {0, 1}));
//		REQUIRE(CHECK_COLUMN(result, 1, {true, false}));
//		REQUIRE(CHECK_COLUMN(result, 2, {0, 1}));
//		REQUIRE(CHECK_COLUMN(result, 3, {0, 1}));
//		REQUIRE(CHECK_COLUMN(result, 4, {0, 1}));
//		REQUIRE(CHECK_COLUMN(result, 5, {0, 10}));
//		REQUIRE(CHECK_COLUMN(result, 6, {0.0, 1.1}));
//		REQUIRE(CHECK_COLUMN(result, 7, {0.0, 10.1}));
//		REQUIRE(CHECK_COLUMN(result, 8, {"01/01/09", "01/01/09"}));
//		REQUIRE(CHECK_COLUMN(result, 9, {"0", "1"}));
//		REQUIRE(CHECK_COLUMN(result, 10,
//		                     {Value::BIGINT(Timestamp::FromString("2009-01-01 00:00:00")),
//		                      Value::BIGINT(Timestamp::FromString("2009-01-01 00:01:00"))}));
//	}
//
//	// this file was created with spark using the data-types.py script
//	SECTION("data-types.parquet") {
//		auto result = con.Query("SELECT * FROM parquet_scan('extension/parquet/test/data-types.parquet')");
//		REQUIRE(CHECK_COLUMN(result, 0, {Value(), 42, -127, 127, Value()}));
//		REQUIRE(CHECK_COLUMN(result, 1, {Value(), 43, -32767, 32767, Value()}));
//		REQUIRE(CHECK_COLUMN(result, 2, {Value(), 44, -2147483647, 2147483647, Value()}));
//		REQUIRE(CHECK_COLUMN(
//		    result, 3,
//		    {Value(), 45, Value::BIGINT(-9223372036854775807), Value::BIGINT(9223372036854775807), Value()}));
//		REQUIRE(CHECK_COLUMN(result, 4, {Value(), 4.6, -4.6, Value(), Value()}));
//		REQUIRE(CHECK_COLUMN(result, 5, {Value(), 4.7, -4.7, Value(), Value()}));
//		// REQUIRE(CHECK_COLUMN(result, 6, {Value(), 4.8, -128, 127, Value()})); // decimal :/
//		REQUIRE(CHECK_COLUMN(result, 7, {Value(), "49", Value(), Value(), Value()}));
//		// REQUIRE(CHECK_COLUMN(result, 8, {Value(), "50", -128, 127, Value()})); // blob :/
//		REQUIRE(CHECK_COLUMN(result, 9, {Value(), true, false, Value(), Value()}));
//		REQUIRE(CHECK_COLUMN(
//		    result, 10,
//		    {Value(), Value::BIGINT(Timestamp::FromString("2019-11-26 20:11:42.501")), Value(), Value(), Value()}));
//		// REQUIRE(CHECK_COLUMN(result, 11, {Value(), false, -128, 127, Value()})); // date :/
//	}
//
//	SECTION("userdata1.parquet") {
//
//		auto result = con.Query("SELECT COUNT(*) FROM  parquet_scan('extension/parquet/test/userdata1.parquet')");
//
//		REQUIRE(CHECK_COLUMN(result, 0, {1000}));
//
//		con.Query("CREATE VIEW userdata1 AS SELECT * FROM parquet_scan('extension/parquet/test/userdata1.parquet')");
//
//		result = con.Query("SELECT COUNT(*) FROM userdata1");
//		REQUIRE(CHECK_COLUMN(result, 0, {1000}));
//
//		con.DisableQueryVerification(); // TOO slow
//
//		result = con.Query("SELECT COUNT(registration_dttm), COUNT(id), COUNT(first_name), COUNT(last_name), "
//		                   "COUNT(email), COUNT(gender), COUNT(ip_address), COUNT(cc), COUNT(country), "
//		                   "COUNT(birthdate), COUNT(salary), COUNT(title), COUNT(comments) FROM userdata1");
//
//		REQUIRE(CHECK_COLUMN(result, 0, {1000}));
//		REQUIRE(CHECK_COLUMN(result, 1, {1000}));
//		REQUIRE(CHECK_COLUMN(result, 2, {1000}));
//		REQUIRE(CHECK_COLUMN(result, 3, {1000}));
//		REQUIRE(CHECK_COLUMN(result, 4, {1000}));
//		REQUIRE(CHECK_COLUMN(result, 5, {1000}));
//		REQUIRE(CHECK_COLUMN(result, 6, {1000}));
//		REQUIRE(CHECK_COLUMN(result, 7, {1000}));
//		REQUIRE(CHECK_COLUMN(result, 8, {1000}));
//		REQUIRE(CHECK_COLUMN(result, 9, {1000}));
//		REQUIRE(CHECK_COLUMN(result, 10, {932}));
//		REQUIRE(CHECK_COLUMN(result, 11, {1000}));
//		REQUIRE(CHECK_COLUMN(result, 12, {994}));
//
//		result = con.Query("SELECT MIN(registration_dttm), MAX(registration_dttm) FROM userdata1");
//		REQUIRE(CHECK_COLUMN(result, 0, {Value::BIGINT(Timestamp::FromString("2016-02-03 00:01:00"))}));
//		REQUIRE(CHECK_COLUMN(result, 1, {Value::BIGINT(Timestamp::FromString("2016-02-03 23:59:55"))}));
//
//		result = con.Query("SELECT MIN(id), MAX(id) FROM userdata1");
//		REQUIRE(CHECK_COLUMN(result, 0, {1}));
//		REQUIRE(CHECK_COLUMN(result, 1, {1000}));
//
//		result = con.Query(
//		    "SELECT FIRST(id) OVER w, LAST(id) OVER w FROM userdata1 WINDOW w AS (ORDER BY id RANGE BETWEEN UNBOUNDED "
//		    "PRECEDING AND UNBOUNDED FOLLOWING) LIMIT 1");
//		REQUIRE(CHECK_COLUMN(result, 0, {1}));
//		REQUIRE(CHECK_COLUMN(result, 1, {1000}));
//
//		result = con.Query("SELECT MIN(first_name), MAX(first_name) FROM userdata1");
//		REQUIRE(CHECK_COLUMN(result, 0, {""}));
//		REQUIRE(CHECK_COLUMN(result, 1, {"Willie"}));
//
//		result = con.Query("SELECT FIRST(first_name) OVER w, LAST(first_name) OVER w FROM userdata1 WINDOW w AS (ORDER "
//		                   "BY id RANGE BETWEEN UNBOUNDED "
//		                   "PRECEDING AND UNBOUNDED FOLLOWING) LIMIT 1");
//		REQUIRE(CHECK_COLUMN(result, 0, {"Amanda"}));
//		REQUIRE(CHECK_COLUMN(result, 1, {"Julie"}));
//
//		result = con.Query("SELECT MIN(last_name), MAX(last_name) FROM userdata1");
//		REQUIRE(CHECK_COLUMN(result, 0, {"Adams"}));
//		REQUIRE(CHECK_COLUMN(result, 1, {"Young"}));
//
//		result = con.Query("SELECT FIRST(last_name) OVER w, LAST(last_name) OVER w FROM userdata1 WINDOW w AS (ORDER "
//		                   "BY id RANGE BETWEEN UNBOUNDED "
//		                   "PRECEDING AND UNBOUNDED FOLLOWING) LIMIT 1");
//		REQUIRE(CHECK_COLUMN(result, 0, {"Jordan"}));
//		REQUIRE(CHECK_COLUMN(result, 1, {"Meyer"}));
//
//		result = con.Query("SELECT MIN(email), MAX(email) FROM userdata1");
//		REQUIRE(CHECK_COLUMN(result, 0, {""}));
//		REQUIRE(CHECK_COLUMN(result, 1, {"wweaver2r@google.de"}));
//
//		result = con.Query("SELECT FIRST(email) OVER w, LAST(email) OVER w FROM userdata1 WINDOW w AS (ORDER "
//		                   "BY id RANGE BETWEEN UNBOUNDED "
//		                   "PRECEDING AND UNBOUNDED FOLLOWING) LIMIT 1");
//		REQUIRE(CHECK_COLUMN(result, 0, {"ajordan0@com.com"}));
//		REQUIRE(CHECK_COLUMN(result, 1, {"jmeyerrr@flavors.me"}));
//
//		result = con.Query("SELECT MIN(gender), MAX(gender) FROM userdata1");
//		REQUIRE(CHECK_COLUMN(result, 0, {""}));
//		REQUIRE(CHECK_COLUMN(result, 1, {"Male"}));
//
//		result = con.Query("SELECT FIRST(gender) OVER w, LAST(gender) OVER w FROM userdata1 WINDOW w AS (ORDER BY id "
//		                   "RANGE BETWEEN UNBOUNDED "
//		                   "PRECEDING AND UNBOUNDED FOLLOWING) LIMIT 1");
//		REQUIRE(CHECK_COLUMN(result, 0, {"Female"}));
//		REQUIRE(CHECK_COLUMN(result, 1, {"Female"}));
//
//		result = con.Query("SELECT MIN(ip_address), MAX(ip_address) FROM userdata1");
//		REQUIRE(CHECK_COLUMN(result, 0, {"0.14.221.162"}));
//		REQUIRE(CHECK_COLUMN(result, 1, {"99.159.168.233"}));
//
//		result = con.Query(
//		    "SELECT FIRST(ip_address) OVER w, LAST(ip_address) OVER w FROM userdata1 WINDOW w AS (ORDER BY id "
//		    "RANGE BETWEEN UNBOUNDED "
//		    "PRECEDING AND UNBOUNDED FOLLOWING) LIMIT 1");
//		REQUIRE(CHECK_COLUMN(result, 0, {"1.197.201.2"}));
//		REQUIRE(CHECK_COLUMN(result, 1, {"217.1.147.132"}));
//
//		result = con.Query("SELECT MIN(cc), MAX(cc) FROM userdata1");
//		REQUIRE(CHECK_COLUMN(result, 0, {""}));
//		REQUIRE(CHECK_COLUMN(result, 1, {"67718647521473678"}));
//
//		result = con.Query("SELECT FIRST(cc) OVER w, LAST(cc) OVER w FROM userdata1 WINDOW w AS (ORDER BY id "
//		                   "RANGE BETWEEN UNBOUNDED "
//		                   "PRECEDING AND UNBOUNDED FOLLOWING) LIMIT 1");
//		REQUIRE(CHECK_COLUMN(result, 0, {"6759521864920116"}));
//		REQUIRE(CHECK_COLUMN(result, 1, {"374288099198540"}));
//
//		result = con.Query("SELECT MIN(country), MAX(country) FROM userdata1");
//		REQUIRE(CHECK_COLUMN(result, 0, {"\"Bonaire"}));
//		REQUIRE(CHECK_COLUMN(result, 1, {"Zimbabwe"}));
//
//		result = con.Query("SELECT FIRST(country) OVER w, LAST(country) OVER w FROM userdata1 WINDOW w AS (ORDER BY id "
//		                   "RANGE BETWEEN UNBOUNDED "
//		                   "PRECEDING AND UNBOUNDED FOLLOWING) LIMIT 1");
//		REQUIRE(CHECK_COLUMN(result, 0, {"Indonesia"}));
//		REQUIRE(CHECK_COLUMN(result, 1, {"China"}));
//
//		result = con.Query("SELECT MIN(birthdate), MAX(birthdate) FROM userdata1");
//		REQUIRE(CHECK_COLUMN(result, 0, {""}));
//		REQUIRE(CHECK_COLUMN(result, 1, {"9/9/1981"}));
//
//		result =
//		    con.Query("SELECT FIRST(birthdate) OVER w, LAST(birthdate) OVER w FROM userdata1 WINDOW w AS (ORDER BY id "
//		              "RANGE BETWEEN UNBOUNDED "
//		              "PRECEDING AND UNBOUNDED FOLLOWING) LIMIT 1");
//		REQUIRE(CHECK_COLUMN(result, 0, {"3/8/1971"}));
//		REQUIRE(CHECK_COLUMN(result, 1, {""}));
//
//		result = con.Query("SELECT MIN(salary), MAX(salary) FROM userdata1");
//		REQUIRE(CHECK_COLUMN(result, 0, {12380.490000}));
//		REQUIRE(CHECK_COLUMN(result, 1, {286592.990000}));
//
//		result = con.Query("SELECT FIRST(salary) OVER w, LAST(salary) OVER w FROM userdata1 WINDOW w AS (ORDER BY id "
//		                   "RANGE BETWEEN UNBOUNDED "
//		                   "PRECEDING AND UNBOUNDED FOLLOWING) LIMIT 1");
//		REQUIRE(CHECK_COLUMN(result, 0, {49756.530000}));
//		REQUIRE(CHECK_COLUMN(result, 1, {222561.130000}));
//
//		result = con.Query("SELECT MIN(title), MAX(title) FROM userdata1");
//		REQUIRE(CHECK_COLUMN(result, 0, {""}));
//		REQUIRE(CHECK_COLUMN(result, 1, {"Web Developer IV"}));
//
//		result = con.Query("SELECT FIRST(title) OVER w, LAST(title) OVER w FROM userdata1 WINDOW w AS (ORDER BY id "
//		                   "RANGE BETWEEN UNBOUNDED "
//		                   "PRECEDING AND UNBOUNDED FOLLOWING) LIMIT 1");
//		REQUIRE(CHECK_COLUMN(result, 0, {"Internal Auditor"}));
//		REQUIRE(CHECK_COLUMN(result, 1, {""}));
//
//		result = con.Query("SELECT MIN(comments), MAX(comments) FROM userdata1");
//		REQUIRE(CHECK_COLUMN(result, 0, {""}));
//		REQUIRE(CHECK_COLUMN(result, 1, {"𠜎𠜱𠝹𠱓𠱸𠲖𠳏"}));
//
//		result = con.Query("SELECT FIRST(comments) OVER w, LAST(comments) OVER w FROM userdata1 WINDOW w AS (ORDER BY "
//		                   "id RANGE BETWEEN UNBOUNDED "
//		                   "PRECEDING AND UNBOUNDED FOLLOWING) LIMIT 1");
//		REQUIRE(CHECK_COLUMN(result, 0, {"1E+02"}));
//		REQUIRE(CHECK_COLUMN(result, 1, {""}));
//	}
//}
//
//TEST_CASE("Parquet file with random NULLs", "[parquet]") {
//	DuckDB db(nullptr);
//	db.LoadExtension<ParquetExtension>();
//	Connection con(db);
//	con.EnableQueryVerification();
//
//	auto result = con.Query("select count(col1) from parquet_scan('extension/parquet/test/bug687_nulls.parquet')");
//	REQUIRE(CHECK_COLUMN(result, 0, {99000}));
//}

#include "duckdb/common/serializer/buffered_file_writer.hpp"
#include "duckdb/common/serializer/buffered_serializer.hpp"

#include "duckdb/main/client_context.hpp"
#include "parquet_types.h"
#include "thrift/protocol/TCompactProtocol.h"
#include "thrift/transport/TBufferTransports.h"
#include "duckdb/common/serializer.hpp"
#include "snappy.h"

using namespace parquet::format;
using namespace apache::thrift::protocol;
using namespace apache::thrift::transport;

class MyTransport : public TTransport {
public:
    MyTransport(Serializer &serializer) : serializer(serializer) {}

    bool isOpen() const override { return true; }

    void open() override {}

    void close() override {}

	void write_virt(const uint8_t* buf, uint32_t len) override {
		serializer.WriteData((const_data_ptr_t) buf, len);
    }

private:
	Serializer &serializer;

};

static Type::type duckdb_type_to_parquet_type(SQLType duckdb_type) {
    switch (duckdb_type.id) {
    case SQLTypeId::BOOLEAN:
        return Type::BOOLEAN;
    case SQLTypeId::TINYINT:
    case SQLTypeId::SMALLINT:
    case SQLTypeId::INTEGER:
        return Type::INT32;
    case SQLTypeId::BIGINT:
        return Type::INT64;
    case SQLTypeId::FLOAT:
        return Type::FLOAT;
    case SQLTypeId::DOUBLE:
        return Type::DOUBLE;
    case SQLTypeId::VARCHAR:
    case SQLTypeId::BLOB:
        return Type::BYTE_ARRAY;
    case SQLTypeId::DATE:
    case SQLTypeId::TIMESTAMP:
        return Type::INT96;
    default: throw NotImplementedException("type");
    }

}

class ParquetWriter {
public:

	void Initialize(ClientContext &context, string filename, vector<SQLType> sql_types, vector<string> names) {
        writer = make_unique<BufferedFileWriter>(context.db.GetFileSystem(), filename.c_str());
        writer->WriteData((const_data_ptr_t)"PAR1", 4);
        TCompactProtocolFactoryT<MyTransport> tproto_factory;
        protocol = tproto_factory.getProtocol(make_shared<MyTransport>(*writer));
        file_meta_data.num_rows = 0;
        file_meta_data.schema.resize(sql_types.size() + 1);

        file_meta_data.schema[0].num_children = sql_types.size();
        file_meta_data.schema[0].__isset.num_children = true;
		file_meta_data.version = 1;

		for (idx_t i = 0; i < sql_types.size(); i++){
           auto& schema_element = file_meta_data.schema[i+1];


		   schema_element.type =duckdb_type_to_parquet_type(sql_types[i]); // TODO switcheroo
		   schema_element.repetition_type = FieldRepetitionType::OPTIONAL;
		   schema_element.num_children = 0;
		   schema_element.__isset.num_children = true;
		   schema_element.__isset.type = true;
		   schema_element.__isset.repetition_type = true;
		   schema_element.name = names[i];
        }
		this->sql_types = sql_types;
	}

	void VarintEncode(uint32_t val, Serializer& ser) {
		do  {
			uint8_t byte = val & 127;
			val >>= 7;
			if (val != 0) {
				byte |= 128;
			}
			ser.Write<uint8_t>(byte);
		} while (val != 0);
	}

    uint8_t GetVarintSize(uint32_t val) {
		uint8_t res = 0;
        do  {
            uint8_t byte = val & 127;
            val >>= 7;
            if (val != 0) {
                byte |= 128;
            }
            res++;
        } while (val != 0);
		return res;
    }

    template <class SRC, class TGT>
    static void _write_plain(Vector& col, idx_t length, nullmask_t& defined, Serializer& ser) {
        auto *ptr = FlatVector::GetData<SRC>(col);
        for (idx_t r = 0; r < length; r++) {
            if (defined[r]) {
                ser.Write<TGT>((TGT) ptr[r]);
            }
        }
    }

	void Sink(ClientContext &context, DataChunk &input)  {
		file_meta_data.row_groups.push_back(RowGroup());
        auto& row_group = file_meta_data.row_groups.back();

        row_group.num_rows = 0;
        row_group.file_offset = writer->GetTotalWritten();
		row_group.__isset.file_offset = true;
		row_group.columns.resize(input.column_count());

        for (idx_t i = 0; i < input.column_count(); i++) {
            auto& input_column = input.data[i];

            BufferedSerializer temp_writer;

            PageHeader hdr;
            hdr.compressed_page_size = 0;
			hdr.uncompressed_page_size = 0;
			hdr.type = PageType::DATA_PAGE;
            hdr.__isset.data_page_header = true;

            hdr.data_page_header.num_values = input.size();
			hdr.data_page_header.encoding = Encoding::PLAIN;
			hdr.data_page_header.definition_level_encoding = Encoding::RLE;
            hdr.data_page_header.repetition_level_encoding = Encoding::BIT_PACKED;

            auto start_offset = writer->GetTotalWritten();

            // varint-encoded, shift count left 1 and set low bit to 1 to indicate literal
            auto define_byte_count = (input.size() + 7)/8;
			uint32_t define_header = (define_byte_count << 1) | 1;
            uint32_t define_size = GetVarintSize(define_header) + define_byte_count;
            // write length of define block

            temp_writer.Write<uint32_t>(define_size);
            VarintEncode(define_header, temp_writer);
			auto defined = FlatVector::Nullmask(input_column);
            defined.flip();
            // bitpack null-ness
            temp_writer.WriteData((const_data_ptr_t) &defined, define_byte_count);

			// write actual payload data
			switch (sql_types[i].id) {
				// TODO bitpack booleans
            case SQLTypeId::TINYINT:
                _write_plain<int8_t, int32_t>(input_column, input.size(), defined, temp_writer);
                break;
            case SQLTypeId::SMALLINT:
                _write_plain<int16_t, int32_t>(input_column, input.size(), defined, temp_writer);
                break;
            case SQLTypeId::INTEGER:
                _write_plain<int32_t, int32_t>(input_column, input.size(), defined, temp_writer);
			    break;
            case SQLTypeId::BIGINT:
                _write_plain<int64_t, int64_t>(input_column, input.size(), defined, temp_writer);
                break;
            case SQLTypeId::FLOAT:
                _write_plain<float, float>(input_column, input.size(), defined, temp_writer);
                break;
            case SQLTypeId::DOUBLE:
                _write_plain<double, double>(input_column, input.size(), defined, temp_writer);
                break;
            case SQLTypeId::VARCHAR: {
                auto *ptr = FlatVector::GetData<string_t>(input_column);
                for (idx_t r = 0; r < input.size(); r++) {
                    if (defined[r]) {
                        temp_writer.Write<uint32_t>(ptr[r].GetSize());
                        temp_writer.WriteData((const_data_ptr_t) ptr[r].GetData(), ptr[r].GetSize());
                    }
                }
				break;
			}
				// TODO date timestamp blob etc.
			default: throw NotImplementedException("type");
			}

           // hdr.compressed_page_size = temp_writer.blob.size;
            hdr.uncompressed_page_size = temp_writer.blob.size;

            size_t compressed_size = snappy::MaxCompressedLength(temp_writer.blob.size);
            auto compressed_buf = unique_ptr<data_t[]>(new data_t[compressed_size]);
			snappy::RawCompress((const char*)temp_writer.blob.data.get(), temp_writer.blob.size, (char*) compressed_buf.get(), &compressed_size);

            hdr.compressed_page_size = compressed_size;

            hdr.write(protocol.get());
            writer->WriteData(compressed_buf.get(), compressed_size);

            auto& column_chunk = row_group.columns[i];
			column_chunk.__isset.meta_data = true;
            column_chunk.meta_data.data_page_offset = start_offset;
            column_chunk.meta_data.total_compressed_size = writer->GetTotalWritten() - start_offset;
            column_chunk.meta_data.codec = CompressionCodec::SNAPPY;
			column_chunk.meta_data.path_in_schema.push_back(file_meta_data.schema[i+1].name);
			column_chunk.meta_data.num_values = input.size();
            column_chunk.meta_data.type = file_meta_data.schema[i+1].type;
        }
        row_group.num_rows += input.size();
        file_meta_data.num_rows += input.size();

        // ?? row_group.total_byte_size / row_group.total_compressed_size

    }
	void Finalize() {
		auto start_offset = writer->GetTotalWritten();
		file_meta_data.write(protocol.get());

//		file_meta_data.printTo(std::cout);
//        std::cout << '\n';

		writer->Write<uint32_t>(writer->GetTotalWritten() - start_offset);

        writer->WriteData((const_data_ptr_t)"PAR1", 4);
		writer->Sync();
		writer.reset();
    }


private:
    unique_ptr<BufferedFileWriter> writer;
    shared_ptr<TProtocol> protocol;
    FileMetaData file_meta_data;
	vector<SQLType> sql_types;
};

TEST_CASE("Parquet writing dummy", "[parquet]") {
    DuckDB db(nullptr);
    db.LoadExtension<ParquetExtension>();
    Connection con(db);
    //con.EnableQueryVerification();

	//auto res = con.Query("VALUES (42::tinyint, 42::smallint, 42::integer, 42::bigint, 42::float,42::double, 42::varchar)");
    auto res = con.Query("SELECT * FROM parquet_scan('/Users/hannes/source/duckdb/extension/parquet/test/lineitem-sf1.snappy.parquet') limit 2000");

    ParquetWriter writer;
    writer.Initialize(*con.context, "/tmp/fuu", res->sql_types, res->names);
    for (auto& chunk : res->collection.chunks) {
		writer.Sink(*con.context, *chunk);
	}
	writer.Finalize();

    auto result = con.Query("SELECT * FROM "
	                        "parquet_scan('/tmp/fuu') LIMIT 10");
	result->Print();
}

//TEST_CASE("Parquet file with gzip compression", "[parquet]") {
//	DuckDB db(nullptr);
//	db.LoadExtension<ParquetExtension>();
//	Connection con(db);
//	con.EnableQueryVerification();
//
//	auto result =
//	    con.Query("select count(*) from parquet_scan('extension/parquet/test/lineitem-top10000.gzip.parquet')");
//	REQUIRE(CHECK_COLUMN(result, 0, {10000}));
//}
//
// TEST_CASE("Test TPCH SF1 from parquet file", "[parquet][.]") {
//	DuckDB db(nullptr);
//	db.LoadExtension<ParquetExtension>();
//	Connection con(db);
//
//	auto result = con.Query("SELECT * FROM "
//	                        "parquet_scan('extension/parquet/test/lineitem-sf1.snappy.parquet') limit 10");
//	result->Print();
//
//	con.Query("CREATE VIEW lineitem AS SELECT * FROM "
//	          "parquet_scan('extension/parquet/test/lineitem-sf1.snappy.parquet')");
//	result = con.Query(tpch::get_query(1));
//	COMPARE_CSV(result, tpch::get_answer(1, 1), true);
//}
