//===----------------------------------------------------------------------===//
//                         DuckDB
//
// crypto_wrapper.hpp
//
//
//===----------------------------------------------------------------------===/

#pragma once

#include "thrift/protocol/TCompactProtocol.h"

namespace duckdb {

using duckdb_apache::thrift::protocol::TProtocol;
using duckdb_apache::thrift::transport::TTransport;
using GCMContext = duckdb_mbedtls::MbedTlsWrapper::AESGCMState;
using duckdb_apache::thrift::protocol::TCompactProtocolFactoryT;

class ParquetCryptoWrapper {
private:
	// Encrypt/decrypt wrapper for a transport protocol
	class CryptoTransport : public TTransport {
	public:
		explicit CryptoTransport(TProtocol &prot_p, const string &key)
		    : prot(prot_p), trans(*prot.getTransport()), aes(key), rw_buffer_remaining(0) {
		}

		void InitializeEncryption(const uint8_t *buf, uint32_t len) {
			aes.InitializeEncryption(buf, len);
		}

		void InitializeDecryption(const uint8_t *buf, uint32_t len) {
			aes.InitializeDecryption(buf, len);
		}

		bool isOpen() const override {
			return trans.isOpen();
		}

		void open() override {
			trans.open();
		}

		void close() override {
			trans.close();
		}

		void write_virt(const uint8_t *buf, uint32_t len) override {
			// Fill up write buffer (if there is anything in there)
			if (rw_buffer_remaining != 0) {
				const auto copy_bytes = MinValue<idx_t>(GCMContext::BLOCK_SIZE - rw_buffer_remaining, len);

				memcpy(rw_buffer + rw_buffer_remaining, buf, copy_bytes);
				buf += copy_bytes;
				len -= copy_bytes;

				rw_buffer_remaining += copy_bytes;
				if (rw_buffer_remaining == GCMContext::BLOCK_SIZE) {
					WriteBlock(rw_buffer);
					rw_buffer_remaining = 0;
				}
			}

			// Process one block at a time
			while (len >= GCMContext::BLOCK_SIZE) {
				WriteBlock(buf);
				buf += GCMContext::BLOCK_SIZE;
				len -= GCMContext::BLOCK_SIZE;
			}

			// Copy any remaining bytes to write buffer
			D_ASSERT(rw_buffer_remaining + len < GCMContext::BLOCK_SIZE);
			memcpy(rw_buffer + rw_buffer_remaining, buf, len);
			rw_buffer_remaining += len;
		}

		uint32_t read_virt(uint8_t *buf, uint32_t len) override {
			const uint32_t result = len;

			// Get from read buffer first (if there is anything in there)
			if (rw_buffer_remaining != 0) {
				const auto copy_bytes = MinValue<idx_t>(rw_buffer_remaining, len);
				const auto read_buffer_offset = GCMContext::BLOCK_SIZE - rw_buffer_remaining;

				memcpy(buf, rw_buffer + read_buffer_offset, rw_buffer_remaining);
				buf += copy_bytes;
				len -= copy_bytes;

				rw_buffer_remaining -= copy_bytes;
			}

			// Process one block at a time
			while (len >= GCMContext::BLOCK_SIZE) {
				ReadBlock(buf);
				buf += GCMContext::BLOCK_SIZE;
				len -= GCMContext::BLOCK_SIZE;
			}

			// Read last block into read buffer and copy just the bytes we need
			if (len != 0) {
				ReadBlock(rw_buffer);
				memcpy(buf, rw_buffer, len);
				rw_buffer_remaining = GCMContext::BLOCK_SIZE - len;
			}

			return result;
		}

	private:
		void WriteBlock(const uint8_t *buf) {
			// Encrypt into aes_buffer
#ifdef DEBUG
			auto size = aes.Process(buf, GCMContext::BLOCK_SIZE, aes_buffer, GCMContext::BLOCK_SIZE);
			D_ASSERT(size == GCMContext::BLOCK_SIZE);
#else
			aes.Process(buf, GCMContext::BLOCK_SIZE, aes_buffer, GCMContext::BLOCK_SIZE);
#endif
			// Write to transport
			trans.write(aes_buffer, GCMContext::BLOCK_SIZE);
		}

		void ReadBlock(uint8_t *buf) {
#ifdef DEBUG
			// Read from transport into aes_buffer
			auto size = trans.read(aes_buffer, GCMContext::BLOCK_SIZE);
			D_ASSERT(size == GCMContext::BLOCK_SIZE);
			// Decrypt from aes_buffer into buf
			size = aes.Process(aes_buffer, GCMContext::BLOCK_SIZE, buf, GCMContext::BLOCK_SIZE);
			D_ASSERT(size == GCMContext::BLOCK_SIZE);
#else
			// Read from transport into aes_buffer
			trans.read(aes_buffer, GCMContext::BLOCK_SIZE);
			// Decrypt from aes_buffer into buf
			aes.Process(aes_buffer, GCMContext::BLOCK_SIZE, buf, GCMContext::BLOCK_SIZE);
#endif
		}

	private:
		//! Protocol and corresponding transport that we're wrapping
		TProtocol &prot;
		TTransport &trans;

		//! AES context and buffers
		GCMContext aes;

		//! For buffering small AES reads/writes
		data_t rw_buffer[GCMContext::BLOCK_SIZE];
		idx_t rw_buffer_remaining;

		//! For encrypting/decrypting one block at a time
		data_t aes_buffer[GCMContext::BLOCK_SIZE];
	};

public:
	template <class T>
	static unique_ptr<T> Read(TProtocol &iprot, const string &key) {
		// Read IV
		data_t iv_buf[GCMContext::BLOCK_SIZE];
		const auto iv_read_size = iprot.getTransport()->read(iv_buf, GCMContext::BLOCK_SIZE);
		if (iv_read_size != GCMContext::BLOCK_SIZE) {
			// TODO throw
		}

		// Create crypto protocol
		TCompactProtocolFactoryT<CryptoTransport> tproto_factory;
		auto dprot = tproto_factory.getProtocol(make_shared<CryptoTransport>(iprot, key));

		// Read encoded ciphertext length and initialize decryption using IV
		const uint32_t ciphertext_length = Load<uint32_t>(iv_buf);
		reinterpret_cast<CryptoTransport &>(*dprot->getTransport())
		    .InitializeDecryption(iv_buf + sizeof(uint32_t), GCMContext::BLOCK_SIZE - sizeof(uint32_t));

		// Finally read the object
		auto object = make_uniq<T>();
		const auto object_read_size = object->read(dprot.get());
		if (object_read_size != ciphertext_length) {
			// TODO throw
		}

		return object;
	}

	template <class T>
	static uint32_t Write(const T &object, TProtocol &oprot, const string &key) {
		// Due to the impeccable design of Parquet, we cannot stream encrypted writes
		// The length of the ciphertext must be known before we write anything to file
	}

private:
	static unique_ptr<duckdb_apache::thrift::protocol::TProtocol> CreateGenericReadProtocol(const char *const ptr,
	                                                                                        const idx_t len) {
		auto transport = make_shared<ThriftGenericTransport>(ptr, len);
		return make_uniq<duckdb_apache::thrift::protocol::TCompactProtocolT<ThriftGenericTransport>>(
		    std::move(transport));
	}
};

} // namespace duckdb
