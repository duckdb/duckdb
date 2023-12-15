
void CSVIterator::Reset() {
	buffer_idx = start_buffer_idx;
	buffer_pos = start_buffer_pos;
	bytes_to_read = NumericLimits<idx_t>::Maximum();
}

bool CSVIterator::Next(CSVBufferManager &buffer_manager) {
	if (file_idx >= buffer_manager.FileCount()) {
		// We are done
		return false;
	}
	iterator_id++;
	// This is our start buffer
	auto buffer = buffer_manager.GetBuffer(file_idx, buffer_idx);
	// 1) We are done with the current file, we must move to the next file
	if (buffer->is_last_buffer && buffer_pos + bytes_to_read > buffer->actual_size) {
		// We are done with this file, we need to reset everything for the next file
		file_idx++;
		start_buffer_idx = 0;
		start_buffer_pos = buffer_manager.GetStartPos();
		buffer_idx = 0;
		buffer_pos = buffer_manager.GetStartPos();
		if (file_idx >= buffer_manager.FileCount()) {
			// We are done
			return false;
		}
		return true;
	}
	// 2) We still have data to scan in this file, we set the iterator accordingly.
	else if (buffer_pos + bytes_to_read > buffer->actual_size) {
		// We must move the buffer
		start_buffer_idx++;
		buffer_idx++;
		start_buffer_pos = 0;
		buffer_pos = 0;
		return true;
	}
	// 3) We are not done with the current buffer, hence we just move where we start within the buffer
	start_buffer_pos += bytes_to_read;
	buffer_pos = start_buffer_pos;
	return true;
}