#pragma once

#include "thrift/protocol/TCompactProtocol.h"
#include "thrift/transport/TBufferTransports.h"
#include "duckdb/common/file_system.hpp"
#include "duckdb/common/assert.hpp"

#include "resizable_buffer.hpp"

namespace duckdb {


// some wrappers so we dont need to copy stuff to unpack the damn thrift
class DuckdbFileTransport : public apache::thrift::transport::TVirtualTransport<DuckdbFileTransport> {
public:
    DuckdbFileTransport(unique_ptr<FileHandle> handle_p)
        : handle(move(handle_p)), location(0) {
    }

    uint32_t read(uint8_t* buf, uint32_t len) {
        handle->Read( buf, len, location);
        location += len;
        return len;
    }

    void SetLocation(idx_t location_p) {
        location = location_p;
    }

private:
    unique_ptr<duckdb::FileHandle> handle;
    duckdb::idx_t location;

};

class DuckdbBufferTransport : public apache::thrift::transport::TVirtualTransport<DuckdbBufferTransport> {
public:
    DuckdbBufferTransport(duckdb::ResizeableBuffer &buffer_p)
        : buffer(buffer_p) {
    }

    uint32_t read(uint8_t* buf, uint32_t len) {
        buf = (uint8_t*) buffer.ptr;
        buffer.inc(len);
        return len;
    }

private:
    duckdb::ResizeableBuffer &buffer;
};


template <class T> static void thrift_unpack_file(apache::thrift::protocol::TProtocol& protocol, idx_t starting_pos, T *deserialized_msg) {
    ((DuckdbFileTransport*)protocol.getTransport().get())->SetLocation(starting_pos);

    try {
        deserialized_msg->read(&protocol);
    } catch (std::exception &e) {
        std::stringstream ss;
        ss << "Couldn't deserialize thrift: " << e.what() << "\n";
        throw std::runtime_error(ss.str());
    }
}

template <class T> static void thrift_unpack(const uint8_t *buf, uint32_t *len, T *deserialized_msg) {
    shared_ptr<apache::thrift::transport::TMemoryBuffer> tmem_transport(new apache::thrift::transport::TMemoryBuffer());
    shared_ptr<apache::thrift::protocol::TProtocol> tproto(new apache::thrift::protocol::TCompactProtocolT<apache::thrift::transport::TMemoryBuffer>(tmem_transport));

    ((apache::thrift::transport::TMemoryBuffer*)tproto->getTransport().get())->resetBuffer(const_cast<uint8_t *>(buf), *len);

    try {
        deserialized_msg->read(tproto.get());
    } catch (std::exception &e) {
        std::stringstream ss;
        ss << "Couldn't deserialize thrift: " << e.what() << "\n";
        throw std::runtime_error(ss.str());
    }
    uint32_t bytes_left = tmem_transport->available_read();
    *len = *len - bytes_left;
}




}