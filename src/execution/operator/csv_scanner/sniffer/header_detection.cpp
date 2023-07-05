

// void BufferedCSVReader::DetectHeader(const vector<vector<LogicalType>> &best_sql_types_candidates,
//                                     const DataChunk &best_header_row) {
//	// information for header detection
//	bool first_row_consistent = true;
//	bool first_row_nulls = false;
//
//	// check if header row is all null and/or consistent with detected column data types
//	first_row_nulls = true;
//	for (idx_t col = 0; col < best_sql_types_candidates.size(); col++) {
//		auto dummy_val = best_header_row.GetValue(col, 0);
//		if (!dummy_val.IsNull()) {
//			first_row_nulls = false;
//		}
//
//		// try cast to sql_type of column
//		const auto &sql_type = best_sql_types_candidates[col].back();
//		if (!TryCastValue(dummy_val, sql_type)) {
//			first_row_consistent = false;
//		}
//	}
//
//	// update parser info, and read, generate & set col_names based on previous findings
//	if (((!first_row_consistent || first_row_nulls) && !options.has_header) || (options.has_header && options.header)) {
//		options.header = true;
//		case_insensitive_map_t<idx_t> name_collision_count;
//		// get header names from CSV
//		for (idx_t col = 0; col < options.num_cols; col++) {
//			const auto &val = best_header_row.GetValue(col, 0);
//			string col_name = val.ToString();
//
//			// generate name if field is empty
//			if (col_name.empty() || val.IsNull()) {
//				col_name = GenerateColumnName(options.num_cols, col);
//			}
//
//			// normalize names or at least trim whitespace
//			if (options.normalize_names) {
//				col_name = NormalizeColumnName(col_name);
//			} else {
//				col_name = TrimWhitespace(col_name);
//			}
//
//			// avoid duplicate header names
//			const string col_name_raw = col_name;
//			while (name_collision_count.find(col_name) != name_collision_count.end()) {
//				name_collision_count[col_name] += 1;
//				col_name = col_name + "_" + to_string(name_collision_count[col_name]);
//			}
//
//			names.push_back(col_name);
//			name_collision_count[col_name] = 0;
//		}
//
//	} else {
//		options.header = false;
//		for (idx_t col = 0; col < options.num_cols; col++) {
//			string column_name = GenerateColumnName(options.num_cols, col);
//			names.push_back(column_name);
//		}
//	}
//	for (idx_t i = 0; i < MinValue<idx_t>(names.size(), options.name_list.size()); i++) {
//		names[i] = options.name_list[i];
//	}
//}
