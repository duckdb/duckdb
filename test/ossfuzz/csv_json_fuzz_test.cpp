// Fuzzer for DuckDB CSV and JSON parsing pipelines.
// Exercises read_csv_auto (CSV sniffer, state machine, type detection)
// and JSON functions (json_valid, json_extract, json_type).

#include "duckdb.hpp"
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <string>

static const char *TMPFILE = "/tmp/duckdb_fuzz_data";

extern "C" int LLVMFuzzerTestOneInput(const uint8_t *data, size_t size) {
	if (size < 2) {
		return 0;
	}

	// First byte selects which code path to exercise.
	uint8_t selector = data[0];
	const uint8_t *payload = data + 1;
	size_t payload_size = size - 1;

	// Limit payload to avoid excessive processing time.
	if (payload_size > 8192) {
		payload_size = 8192;
	}

	duckdb::DuckDB db(nullptr);
	duckdb::Connection con(db);

	// Disable progress bar and set memory/row limits to prevent resource exhaustion.
	con.Query("SET enable_progress_bar=false");
	con.Query("SET max_expression_depth=50");

	try {
		switch (selector % 4) {
		case 0: {
			// CSV auto-detection and parsing via read_csv_auto.
			// Exercises: CSVSniffer, CSVStateMachine, StringValueScanner,
			// type detection, dialect detection.
			FILE *f = fopen(TMPFILE, "wb");
			if (!f)
				return 0;
			fwrite(payload, 1, payload_size, f);
			fclose(f);
			con.Query("SELECT * FROM read_csv_auto('" + std::string(TMPFILE) + "', sample_size=1) LIMIT 10");
			break;
		}
		case 1: {
			// CSV with explicit options to exercise different scanner paths.
			FILE *f = fopen(TMPFILE, "wb");
			if (!f)
				return 0;
			fwrite(payload, 1, payload_size, f);
			fclose(f);
			con.Query("SELECT * FROM read_csv('" + std::string(TMPFILE) +
			          "', columns={'c1': 'VARCHAR', 'c2': 'VARCHAR', 'c3': 'VARCHAR'}, "
			          "auto_detect=false, header=false, sample_size=1) LIMIT 10");
			break;
		}
		case 2: {
			// JSON validation and extraction functions.
			// Exercises: yyjson parser, JSONCommon::ReadDocumentUnsafe,
			// json_valid, json_type, json_extract.
			std::string str_payload(reinterpret_cast<const char *>(payload), payload_size);
			// Escape single quotes for SQL safety.
			std::string escaped;
			escaped.reserve(str_payload.size() * 2);
			for (char c : str_payload) {
				if (c == '\'') {
					escaped += "''";
				} else if (c == '\0') {
					// Skip null bytes in SQL strings.
					continue;
				} else {
					escaped += c;
				}
			}
			con.Query("SELECT json_valid('" + escaped + "')");
			con.Query("SELECT json_type('" + escaped + "')");
			con.Query("SELECT json_extract('" + escaped + "', '$')");
			break;
		}
		case 3: {
			// JSON file reading via read_json_auto.
			// Exercises: JSONReader, JSON scanning, auto schema detection.
			FILE *f = fopen(TMPFILE, "wb");
			if (!f)
				return 0;
			fwrite(payload, 1, payload_size, f);
			fclose(f);
			con.Query("SELECT * FROM read_json_auto('" + std::string(TMPFILE) +
			          "', maximum_sample_files=1, sample_size=1) LIMIT 10");
			break;
		}
		}
	} catch (std::exception &e) {
		// Expected: malformed input will throw.
	}

	remove(TMPFILE);
	return 0;
}
