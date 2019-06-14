#include "execution/operator/persistent/physical_copy_from_file.hpp"

#include "catalog/catalog_entry/table_catalog_entry.hpp"
#include "common/file_system.hpp"
#include "common/gzip_stream.hpp"
#include "main/client_context.hpp"
#include "main/database.hpp"

#include <algorithm>
#include <fstream>

using namespace duckdb;
using namespace std;

void PhysicalCopyFromFile::GetChunkInternal(ClientContext &context, DataChunk &chunk, PhysicalOperatorState *state) {
	auto &info = *this->info;
	index_t total = 0;
	index_t nr_elements = 0;

	assert(info.is_from);
	if (!context.db.file_system->FileExists(info.file_path)) {
		throw Exception("File not found");
	}

	unique_ptr<istream> csv_stream;
	if (StringUtil::EndsWith(StringUtil::Lower(info.file_path), ".gz")) {
		csv_stream = make_unique<GzipStream>(info.file_path);
	} else {
		auto csv_local = make_unique<ifstream>();
		csv_local->open(info.file_path);
		csv_stream = move(csv_local);
	}

	BufferedCSVReader reader(*this, *csv_stream);
	// initialize the insert_chunk with the actual to-be-inserted types
	auto types = table->GetTypes();
	reader.insert_chunk.Initialize(types);
	// initialize the parse chunk with VARCHAR data
	for (index_t i = 0; i < types.size(); i++) {
		types[i] = TypeId::VARCHAR;
	}
	reader.parse_chunk.Initialize(types);
	// handle the select list (if any)
	if (info.select_list.size() > 0) {
		reader.set_to_default.resize(types.size(), true);
		for (index_t i = 0; i < info.select_list.size(); i++) {
			auto &column = table->GetColumn(info.select_list[i]);
			reader.column_oids.push_back(column.oid);
			reader.set_to_default[column.oid] = false;
		}
	} else {
		for (index_t i = 0; i < types.size(); i++) {
			reader.column_oids.push_back(i);
		}
	}
	reader.ParseCSV(context);
	total = reader.total + reader.nr_elements;

	chunk.data[0].count = 1;
	chunk.data[0].SetValue(0, Value::BIGINT(total + nr_elements));

	state->finished = true;
}
