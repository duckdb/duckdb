import pytest

pa = pytest.importorskip("pyarrow")


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


class JSONType(pa.ExtensionType):
    def __init__(self):
        pa.ExtensionType.__init__(self, pa.string(), "arrow.json")

    def __arrow_ext_serialize__(self):
        # since we don't have a parameterized type, we don't need extra
        # metadata to be deserialized
        return b''

    @classmethod
    def __arrow_ext_deserialize__(self, storage_type, serialized):
        # return an instance of this subclass given the serialized
        # metadata.
        return JSONType()


class UHugeIntType(pa.ExtensionType):
    def __init__(self):
        pa.ExtensionType.__init__(self, pa.binary(16), "duckdb.uhugeint")

    def __arrow_ext_serialize__(self):
        return b''

    @classmethod
    def __arrow_ext_deserialize__(self, storage_type, serialized):
        return UHugeIntType()


class HugeIntType(pa.ExtensionType):
    def __init__(self):
        pa.ExtensionType.__init__(self, pa.binary(16), "duckdb.hugeint")

    def __arrow_ext_serialize__(self):
        return b''

    @classmethod
    def __arrow_ext_deserialize__(self, storage_type, serialized):
        return HugeIntType()


class VarIntType(pa.ExtensionType):
    def __init__(self):
        pa.ExtensionType.__init__(self, pa.binary(), "duckdb.varint")

    def __arrow_ext_serialize__(self):
        return b''

    @classmethod
    def __arrow_ext_deserialize__(self, storage_type, serialized):
        return VarIntType()
