#include "duckdb/transaction/cdc_write_state.hpp"

#include "duckdb/catalog/catalog_set.hpp"
#include "duckdb/main/attached_database.hpp"
#include "duckdb/storage/data_table.hpp"
#include "duckdb/storage/table/column_data.hpp"
#include "duckdb/storage/table/update_segment.hpp"
#include "duckdb/transaction/append_info.hpp"
#include "duckdb/transaction/delete_info.hpp"
#include "duckdb/transaction/duck_transaction.hpp"
#include "duckdb/transaction/update_info.hpp"
#include "duckdb/function/change_data_capature_function.hpp"
#include "duckdb/common/unique_ptr.hpp"
#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/config.hpp"
#include "duckdb/common/types/selection_vector.hpp"

namespace duckdb {

void ChangeDataCapture::EmitChange(
			const cdc_event_type type,
			const idx_t transactionId,
			const idx_t column_count,
			const idx_t table_version,
			idx_t *updated_column_index,
			const char *table_name,
			const char **column_names,
			idx_t *column_versions,
			const duckdb_data_chunk values,
			const duckdb_data_chunk previous_values) const {

	if (function != nullptr) {

		function(type, transactionId, column_count, table_version, updated_column_index, table_name, column_names, column_versions, values, previous_values);
	}
}

CDCWriteState::CDCWriteState(DuckTransaction &transaction_p)
	: transaction(transaction_p), update_table_version(0), last_update_info() {
}

void CDCWriteState::EmitDelete(DeleteInfo &info) {
	auto &table = info.table;

	auto table_version = table->GetVersion();
	auto &column_definitions = table->Columns();
	auto columnCount = column_definitions.size();
	auto column_names = vector<const char*>(columnCount);
	auto column_versions = vector<uint64_t>(columnCount);
	for (idx_t i = 0; i < columnCount; i++) {
		column_names[i] = strdup(column_definitions[i].GetName().c_str());
		column_versions[i] = table->GetColumnVersion(i);
	}

	auto number_of_rows = info.count;
	if (!info.is_consecutive) {
		for (idx_t i = 0; i < info.count; i++) {
			const auto row_offset = info.GetRows()[i] + 1;
			if (row_offset > number_of_rows) {
				number_of_rows = row_offset;
			}
		}
	}

	auto ptr = transaction.context.lock();
	auto &config = DBConfig::GetConfig(info.table->db.GetDatabase());
	table->ScanTableSegment(transaction, info.base_row, number_of_rows, [&](DataChunk &chunk) {
		auto delete_chunk = make_uniq<DataChunk>();
		delete_chunk->Initialize(*ptr, chunk.GetTypes(), chunk.size());
		delete_chunk->Append(chunk);

		if (!info.is_consecutive) {
			ManagedSelection sel(info.count);
			auto delete_rows = info.GetRows();
			for (idx_t i = 0; i < info.count; i++) {
				sel.Append(delete_rows[i]);
			}
			delete_chunk->Slice(sel.Selection(), sel.Count());
		}

		delete_chunk->Flatten();

		config.change_data_capture.EmitChange(
			DUCKDB_CDC_EVENT_DELETE,
			transaction.transaction_id,
			columnCount,
			table_version,
			nullptr,
			table->GetTableName().c_str(),
			column_names.data(),
			column_versions.data(),
			nullptr,
			reinterpret_cast<duckdb_data_chunk>(delete_chunk.release())
			);
	});

	if (columnCount > 0) {
		for (idx_t i = 0; i < columnCount; i++) {
			delete[] column_names[i];
		}
	}
}

void CDCWriteState::EmitInsert(AppendInfo &info) {
	auto &table = info.table;
	auto table_version = table->GetVersion();

	auto &column_definitions = table->Columns();
	auto columnCount = column_definitions.size();
	auto column_names = vector<const char*>(columnCount);
	auto column_versions = vector<uint64_t>(columnCount);
	for (idx_t i = 0; i < columnCount; i++) {
		column_names[i] = strdup(column_definitions[i].GetName().c_str());
		column_versions[i] = table->GetColumnVersion(i);
	}
	auto ptr = transaction.context.lock();

	table->ScanTableSegment(transaction, info.start_row, info.count, [&](DataChunk &chunk) {
		auto insert_chunk = make_uniq<DataChunk>();
		insert_chunk->Initialize(*ptr, chunk.GetTypes(), chunk.size());
		insert_chunk->Append(chunk);
		insert_chunk->Flatten();

		auto &config = DBConfig::GetConfig(info.table->db.GetDatabase());
		config.change_data_capture.EmitChange(
			DUCKDB_CDC_EVENT_INSERT,
			transaction.transaction_id,
			columnCount,
			table_version,
			nullptr,
			table->GetTableName().c_str(),
			column_names.data(),
			column_versions.data(),
			reinterpret_cast<duckdb_data_chunk>(insert_chunk.release()),
			nullptr
			);
	});

	if (columnCount > 0) {
		for (idx_t i = 0; i < columnCount; i++) {
			delete[] column_names[i];
		}
	}
}

bool CDCWriteState::CanApplyUpdate(UpdateInfo &info) {
	if (!current_update_chunk || !previous_update_chunk) {
		return false;
	}

	if (info.N != last_update_info.N ||
		info.vector_index != last_update_info.vector_index ||
		info.table->GetTableName() != last_update_info.table->GetTableName()) {

		return false;
	}

	const auto tuples = info.GetTuples();
	const auto last_tuples = last_update_info.cdc_tuples;
	for (auto i = 0; i < info.N; i++) {
		if (tuples[i] != last_tuples[i]) {
			return false;
		}
	}

	return true;
}

void CDCWriteState::EmitUpdate(UpdateInfo &info) {
	auto &table = info.table;

	auto table_types = table->GetTypes();
	auto &column_definitions = table->Columns();
	vector<column_t> column_ids;
	vector<string> column_names;
	vector<uint64_t> column_versions;
	vector<LogicalType> update_types;
	vector<StorageIndex> column_indexes;
	auto did_add_target = false;

	if (transaction.involved_columns.find(table->GetTableName()) != transaction.involved_columns.end()) {
		auto column_map = transaction.involved_columns[table->GetTableName()];
		if (column_map.find(info.column_index) != column_map.end()) {
			column_ids = column_map[info.column_index];
		}
	}

	for (idx_t i = 0; i < column_ids.size(); i++) {
		auto column_index = column_ids[i];
		column_names.push_back(std::move(column_definitions[column_index].GetName()));
		column_versions.push_back(table->GetColumnVersion(column_index));
		update_types.emplace_back(table_types[column_index]);
		column_indexes.push_back(StorageIndex(column_index));
		if (column_index == info.column_index) {
			did_add_target = true;
		}
	}

	if (!did_add_target) {
		column_names.push_back(std::move(column_definitions[info.column_index].GetName()));
		column_versions.push_back(table->GetColumnVersion(info.column_index));
		update_types.emplace_back(table_types[info.column_index]);
		column_indexes.push_back(StorageIndex(info.column_index));
	}

	auto update_offset = info.column_index;
	for (idx_t i = 0; i < column_indexes.size(); i++) {
		if (column_indexes[i].GetPrimaryIndex() == info.column_index) {
			update_offset = i;
			break;
		}
	}


	if (CanApplyUpdate(info)) {
		info.segment->FetchAndApplyUpdate(info, previous_update_chunk->data[update_offset]);
		info.segment->FetchCommitted(info.vector_index, current_update_chunk->data[update_offset]);
	} else {
		Flush();

		update_column_names.clear();
		this->column_versions.clear();
		this->column_indexes.clear();

		last_update_info.cdc_tuples = info.GetTuples();
		last_update_info.vector_index = info.vector_index;
		last_update_info.N = info.N;
		last_update_info.table = info.table;
		update_table_version = table->GetVersion();
		this->column_indexes = column_indexes;
		this->column_versions = column_versions;
		this->update_column_names = column_names;

		auto ptr = transaction.context.lock();

		if (!current_update_chunk || !previous_update_chunk) {
			current_update_chunk = make_uniq<DataChunk>();
			previous_update_chunk = make_uniq<DataChunk>();
			current_update_chunk->Initialize(*ptr, update_types, STANDARD_VECTOR_SIZE);
			previous_update_chunk->Initialize(*ptr, update_types, STANDARD_VECTOR_SIZE);
		}
		else {
			current_update_chunk->Reset();
			previous_update_chunk->Reset();
		}

		table->ScanTableSegment(transaction, info.vector_index * STANDARD_VECTOR_SIZE, STANDARD_VECTOR_SIZE,
			column_indexes, update_types, [&](DataChunk &chunk) {
			current_update_chunk->Append(chunk);
			previous_update_chunk->Append(chunk);
		});

		info.segment->FetchAndApplyUpdate(info, previous_update_chunk->data[update_offset]);
		info.segment->FetchCommitted(info.vector_index, current_update_chunk->data[update_offset]);
	}
}

void CDCWriteState::Flush() {
	if (current_update_chunk && previous_update_chunk) {
		SelectionVector sel(last_update_info.cdc_tuples);
		auto &config = DBConfig::GetConfig(last_update_info.table->db.GetDatabase());

		auto ptr = transaction.context.lock();

		auto current_chunk = make_uniq<DataChunk>();
		auto previous_chunk = make_uniq<DataChunk>();
		current_chunk->Initialize(*ptr, current_update_chunk->GetTypes(), current_update_chunk->size());
		previous_chunk->Initialize(*ptr, previous_update_chunk->GetTypes(), previous_update_chunk->size());

		// We can't reference the chunks here because their life cycle extends beyond the life cycle of this class
		current_chunk->Append(*current_update_chunk);
		previous_chunk->Append(*previous_update_chunk);

		if (current_chunk->size() > last_update_info.N) {
			current_chunk->Slice(sel, last_update_info.N);
			previous_chunk->Slice(sel, last_update_info.N);
		}

		current_chunk->Flatten();
		previous_chunk->Flatten();

		vector<const char*> column_names_cstrings;
		auto updated_columns = update_column_names;
		for (const auto &column_name : updated_columns) {
			column_names_cstrings.push_back(strdup(column_name.c_str()));
		}

		config.change_data_capture.EmitChange(
			DUCKDB_CDC_EVENT_UPDATE,
			transaction.transaction_id,
			column_names_cstrings.size(),
			update_table_version,
			nullptr,
			last_update_info.table->GetTableName().c_str(),
			column_names_cstrings.data(),
			this->column_versions.data(),
			reinterpret_cast<duckdb_data_chunk>(current_chunk.release()),
			reinterpret_cast<duckdb_data_chunk>(previous_chunk.release())
		);

		if (!column_names_cstrings.empty()) {
			for (idx_t i = 0; i < column_names_cstrings.size(); i++) {
				delete[] column_names_cstrings[i];
			}
		}
	}
}

void CDCWriteState::EmitEntry(UndoFlags type, data_ptr_t data) {
	if (type != UndoFlags::UPDATE_TUPLE) {
		Flush(); //Flush existing updates if they exist
	}

	switch (type) {
		case UndoFlags::CATALOG_ENTRY: {
			//Not supported
			break;
		}
		case UndoFlags::INSERT_TUPLE: {
			// append:
			auto info = reinterpret_cast<AppendInfo *>(data);
			if (!info->table->IsTemporary()) {
				EmitInsert(*info);
			}
			break;
		}
		case UndoFlags::DELETE_TUPLE: {
			// deletion:
			auto info = reinterpret_cast<DeleteInfo *>(data);
			if (!info->table->IsTemporary()) {
				EmitDelete(*info);
			}
			break;
		}
		case UndoFlags::UPDATE_TUPLE: {
			// update:
			auto info = reinterpret_cast<UpdateInfo *>(data);
			if (!info->segment->column_data.GetTableInfo().IsTemporary()) {
				EmitUpdate(*info);
			}
			break;
		}
		case UndoFlags::SEQUENCE_VALUE: {
			//Not Supported
			break;
		}
		default:
			throw InternalException("UndoBuffer - don't know how to commit this type!");
	}
}

void CDCWriteState::EmitTransactionEntry(CDC_EVENT_TYPE type){
	if (transaction.context.expired()) {
		return;
	}

	auto context = transaction.context.lock();
	auto &config = DBConfig::GetConfig(*context);
	config.change_data_capture.EmitChange(
		type,
		transaction.transaction_id,
		0,
		0,
		nullptr,
		nullptr,
		nullptr,
		nullptr,
		nullptr,
		nullptr
		);
}
} // namespace duckdb
