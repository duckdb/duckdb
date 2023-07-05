//
// bool TryCastDecimalVectorCommaSeparated(CSVReaderOptions &options, Vector &input_vector, Vector &result_vector,
//                                        idx_t count, string &error_message, const LogicalType &result_type) {
//	auto width = DecimalType::GetWidth(result_type);
//	auto scale = DecimalType::GetScale(result_type);
//	switch (result_type.InternalType()) {
//	case PhysicalType::INT16:
//		return TemplatedTryCastDecimalVector<TryCastToDecimalCommaSeparated, int16_t>(
//		    options, input_vector, result_vector, count, error_message, width, scale);
//	case PhysicalType::INT32:
//		return TemplatedTryCastDecimalVector<TryCastToDecimalCommaSeparated, int32_t>(
//		    options, input_vector, result_vector, count, error_message, width, scale);
//	case PhysicalType::INT64:
//		return TemplatedTryCastDecimalVector<TryCastToDecimalCommaSeparated, int64_t>(
//		    options, input_vector, result_vector, count, error_message, width, scale);
//	case PhysicalType::INT128:
//		return TemplatedTryCastDecimalVector<TryCastToDecimalCommaSeparated, hugeint_t>(
//		    options, input_vector, result_vector, count, error_message, width, scale);
//	default:
//		throw InternalException("Unimplemented physical type for decimal");
//	}
//}
//
// bool TryCastFloatingVectorCommaSeparated(CSVReaderOptions &options, Vector &input_vector, Vector &result_vector,
//                                         idx_t count, string &error_message, const LogicalType &result_type,
//                                         idx_t &line_error) {
//	switch (result_type.InternalType()) {
//	case PhysicalType::DOUBLE:
//		return TemplatedTryCastFloatingVector<TryCastErrorMessageCommaSeparated, double>(
//		    options, input_vector, result_vector, count, error_message, line_error);
//	case PhysicalType::FLOAT:
//		return TemplatedTryCastFloatingVector<TryCastErrorMessageCommaSeparated, float>(
//		    options, input_vector, result_vector, count, error_message, line_error);
//	default:
//		throw InternalException("Unimplemented physical type for floating");
//	}
//}

//
// vector<LogicalType> BufferedCSVReader::RefineTypeDetection(const vector<LogicalType> &type_candidates,
//                                                           const vector<LogicalType> &requested_types,
//                                                           vector<vector<LogicalType>> &best_sql_types_candidates,
//                                                           map<LogicalTypeId, vector<string>> &best_format_candidates)
//                                                           {
//	// for the type refine we set the SQL types to VARCHAR for all columns
//	return_types.clear();
//	return_types.assign(options.num_cols, LogicalType::VARCHAR);
//
//	vector<LogicalType> detected_types;
//
//	// if data types were provided, exit here if number of columns does not match
//	if (!requested_types.empty()) {
//		if (requested_types.size() != options.num_cols) {
//			throw InvalidInputException(
//			    "Error while determining column types: found %lld columns but expected %d. (%s)", options.num_cols,
//			    requested_types.size(), options.ToString());
//		} else {
//			detected_types = requested_types;
//		}
//	} else if (options.all_varchar) {
//		// return all types varchar
//		detected_types = return_types;
//	} else {
//		// jump through the rest of the file and continue to refine the sql type guess
//		while (JumpToNextSample()) {
//			InitParseChunk(return_types.size());
//			// if jump ends up a bad line, we just skip this chunk
//			if (!TryParseCSV(ParserMode::SNIFFING_DATATYPES)) {
//				continue;
//			}
//			for (idx_t col = 0; col < parse_chunk.ColumnCount(); col++) {
//				vector<LogicalType> &col_type_candidates = best_sql_types_candidates[col];
//				while (col_type_candidates.size() > 1) {
//					const auto &sql_type = col_type_candidates.back();
//					//	narrow down the date formats
//					if (best_format_candidates.count(sql_type.id())) {
//						auto &best_type_format_candidates = best_format_candidates[sql_type.id()];
//						auto save_format_candidates = best_type_format_candidates;
//						while (!best_type_format_candidates.empty()) {
//							if (TryCastVector(parse_chunk.data[col], parse_chunk.size(), sql_type)) {
//								break;
//							}
//							//	doesn't work - move to the next one
//							best_type_format_candidates.pop_back();
//							options.has_format[sql_type.id()] = (!best_type_format_candidates.empty());
//							if (!best_type_format_candidates.empty()) {
//								SetDateFormat(best_type_format_candidates.back(), sql_type.id());
//							}
//						}
//						//	if none match, then this is not a column of type sql_type,
//						if (best_type_format_candidates.empty()) {
//							//	so restore the candidates that did work.
//							best_type_format_candidates.swap(save_format_candidates);
//							if (!best_type_format_candidates.empty()) {
//								SetDateFormat(best_type_format_candidates.back(), sql_type.id());
//							}
//						}
//					}
//
//					if (TryCastVector(parse_chunk.data[col], parse_chunk.size(), sql_type)) {
//						break;
//					} else {
//						col_type_candidates.pop_back();
//					}
//				}
//			}
//		}
//
//		// set sql types
//		for (auto &best_sql_types_candidate : best_sql_types_candidates) {
//			LogicalType d_type = best_sql_types_candidate.back();
//			if (best_sql_types_candidate.size() == type_candidates.size()) {
//				d_type = LogicalType::VARCHAR;
//			}
//			detected_types.push_back(d_type);
//		}
//	}
//
//	return detected_types;
//}
