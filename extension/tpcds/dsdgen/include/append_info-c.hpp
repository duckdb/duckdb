#pragma once

#include "duckdb/main/appender.hpp"
#include "duckdb/main/connection.hpp"
#include "duckdb/main/client_context.hpp"
#ifndef DUCKDB_AMALGAMATION
#include "duckdb/common/exception.hpp"
#include "duckdb/common/types/date.hpp"
#include "duckdb/parser/column_definition.hpp"
#include "duckdb/storage/data_table.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/planner/parsed_data/bound_create_table_info.hpp"
#include "duckdb/parser/parsed_data/create_table_info.hpp"
#include "duckdb/parser/constraints/not_null_constraint.hpp"
#include "duckdb/catalog/catalog.hpp"
#include "duckdb/planner/binder.hpp"
#include "duckdb/common/operator/cast_operators.hpp"
#include "duckdb/common/operator/string_cast.hpp"
#endif

#include <memory>
#include <cassert>

namespace duckdb {

struct DSDGenAppender {
	DSDGenAppender(ClientContext &context, TableCatalogEntry *tbl) : context(context), tbl(tbl), column(0) {
		vector<LogicalType> types;
		for (idx_t i = 0; i < tbl->columns.size(); i++) {
			types.push_back(tbl->columns[i].type);
		}
		chunk.Initialize(types);
	}

	void BeginRow() {
		column = 0;
	}

	void EndRow() {
		assert(column == chunk.ColumnCount());
		chunk.SetCardinality(chunk.size() + 1);
		if (chunk.size() == STANDARD_VECTOR_SIZE) {
			Flush();
		}
	}

	idx_t CurrentColumn() {
		return column;
	}

	void Flush() {
		if (chunk.size() == 0) {
			return;
		}
		tbl->storage->Append(*tbl, context, chunk);
		chunk.Reset();
	}

	void Close() {
		Flush();
	}

	template <class SRC, class DST>
	void AppendValueInternal(Vector &col, SRC input) {
		FlatVector::GetData<DST>(col)[chunk.size()] = Cast::Operation<SRC, DST>(input);
	}

	template <class T>
	void Append(T input) {
		if (column >= chunk.data.size()) {
			throw InvalidInputException("Too many appends for chunk!");
		}
		auto &col = chunk.data[column];
		switch (col.GetType().InternalType()) {
		case PhysicalType::BOOL:
			AppendValueInternal<T, bool>(col, input);
			break;
		case PhysicalType::UINT8:
			AppendValueInternal<T, uint8_t>(col, input);
			break;
		case PhysicalType::INT8:
			AppendValueInternal<T, int8_t>(col, input);
			break;
		case PhysicalType::UINT16:
			AppendValueInternal<T, uint16_t>(col, input);
			break;
		case PhysicalType::INT16:
			AppendValueInternal<T, int16_t>(col, input);
			break;
		case PhysicalType::UINT32:
			AppendValueInternal<T, uint32_t>(col, input);
			break;
		case PhysicalType::INT32:
			AppendValueInternal<T, int32_t>(col, input);
			break;
		case PhysicalType::UINT64:
			AppendValueInternal<T, uint64_t>(col, input);
			break;
		case PhysicalType::INT64:
			AppendValueInternal<T, int64_t>(col, input);
			break;
		case PhysicalType::INT128:
			AppendValueInternal<T, hugeint_t>(col, input);
			break;
		case PhysicalType::FLOAT:
			AppendValueInternal<T, float>(col, input);
			break;
		case PhysicalType::DOUBLE:
			AppendValueInternal<T, double>(col, input);
			break;
		case PhysicalType::VARCHAR:
			FlatVector::GetData<string_t>(col)[chunk.size()] = StringCast::Operation<T>(input, col);
			break;
		default:
			AppendValue(Value::CreateValue<T>(input));
			return;
		}
		column++;
	}

	void AppendValue(const Value &value) {
		chunk.SetValue(column, chunk.size(), value);
		column++;
	}
	void AppendString(const char *value) {
		Append<string_t>(StringVector::AddString(chunk.data[column], value));
	}

private:
	ClientContext &context;
	TableCatalogEntry *tbl;
	DataChunk chunk;
	idx_t column;
};

} // namespace duckdb

namespace tpcds {

struct tpcds_table_def {
	const char *name;
	int fl_small;
	int fl_child;
};

#define CALL_CENTER   0
#define DBGEN_VERSION 24

struct tpcds_append_information {
	tpcds_append_information(duckdb::ClientContext &context_p, duckdb::TableCatalogEntry *table)
	    : context(context_p), appender(context_p, table) {
	}

	duckdb::ClientContext &context;
	duckdb::DSDGenAppender appender;

	tpcds_table_def table_def;
};

} // namespace tpcds
