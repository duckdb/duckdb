import pytest
import duckdb

pa = pytest.importorskip('pyarrow')

ipc = pytest.importorskip('pyarrow.ipc')


def get_record_batch():
    data = [pa.array([1, 2, 3, 4]), pa.array(['foo', 'bar', 'baz', None]), pa.array([True, None, False, True])]
    return pa.record_batch(data, names=['f0', 'f1', 'f2'])


class TestArrowIPCExtension(object):
    # Only thing we can test in core is that it suggests the
    # instalation and loading of the extension
    def test_single_buffer(self, duckdb_cursor):
        batch = get_record_batch()
        sink = pa.BufferOutputStream()

        with ipc.new_stream(sink, batch.schema) as writer:
            for _ in range(5):  # Write 5 batches into one stream
                writer.write_batch(batch)

        buffer = sink.getvalue()

        buffers = []
        with pa.BufferReader(buffer) as buf_reader:  # Use pyarrow.BufferReader
            stream = ipc.MessageReader.open_stream(buf_reader)
            # This fails
            with pytest.raises(
                duckdb.Error, match="The nanoarrow community extension is needed to read the Arrow IPC protocol."
            ):
                result = duckdb_cursor.from_arrow(stream).fetchall()
