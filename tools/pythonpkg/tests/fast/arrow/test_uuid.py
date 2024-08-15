import pyarrow as pa
import uuid
import duckdb

class UuidType(pa.ExtensionType):
    def __init__(self):
        pa.ExtensionType.__init__(self, pa.binary(16), "arrow.uuid")

    def __arrow_ext_serialize__(self):
        # since we don't have a parameterized type, we don't need extra
        # metadata to be deserialized
        return b''

    @classmethod
    def __arrow_ext_deserialize__(self, storage_type, serialized):
        # return an instance of this subclass given the serialized
        # metadata.
        return UuidType()


pa.register_extension_type(UuidType())

storage_array = pa.array([uuid.uuid4().bytes for _ in range(4)], pa.binary(16))
uuid_type = UuidType()
uuid_type.wrap_array(storage_array)

# arrow_table = pa.Table.from_arrays([storage_array], names=['uuid_col'])
# Create a RecordBatch directly from the array
record_batch = pa.RecordBatch.from_arrays([storage_array], ['uuid_col'])

# Create a RecordBatchReader from the RecordBatch
record_batch_reader = pa.RecordBatchReader.from_batches(record_batch.schema, [record_batch])

print (record_batch_reader)

con = duckdb.connect()
res = con.execute('FROM record_batch_reader').fetchall()
print(res)