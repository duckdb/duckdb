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
using AESGCMState = duckdb_mbedtls::MbedTlsWrapper::AESGCMState;
using duckdb_apache::thrift::protocol::TCompactProtocolFactoryT;

class ParquetCryptoWrapper {
public:
	//! Encrypted modules
	static constexpr idx_t LENGTH_BYTES = 4;
	static constexpr idx_t NONCE_BYTES = 12;
	static constexpr idx_t TAG_BYTES = 16;

private:
	//! Encrypt wrapper for a transport protocol
	class EncryptionTransport : public TTransport {
	private:
		//! We encrypt this or less blocks per call
		static constexpr idx_t ENCRYPTION_BLOCK_SIZE = 2048;

	public:
		EncryptionTransport(TProtocol &prot_p, const string &key, Allocator &allocator_p)
		    : prot(prot_p), trans(*prot.getTransport()), aes(key), allocator(allocator_p, ENCRYPTION_BLOCK_SIZE) {
			allocator.Allocate(0);
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
			const auto &tail = *allocator.GetTail();

			// Make sure we fill up the last block
			const auto copy_bytes = MinValue<idx_t>(tail.maximum_size - tail.current_position, len);
			memcpy(allocator.Allocate(copy_bytes), buf, copy_bytes);
			buf += copy_bytes;
			len -= copy_bytes;

			if (len != 0) {
				memcpy(allocator.Allocate(copy_bytes), buf, len);
			}
		}

		void Finalize() {
			const auto ciphertext_length = allocator.SizeInBytes();

			// Write length
			const uint32_t total_length = NONCE_BYTES + ciphertext_length + TAG_BYTES;
			trans.write(reinterpret_cast<const uint8_t *>(&total_length), LENGTH_BYTES);

			// Generate and write nonce TODO actual nonce
			data_t nonce[NONCE_BYTES];
			for (idx_t i = 0; i < NONCE_BYTES; i++) {
				nonce[i] = i;
			}
			trans.write(nonce, NONCE_BYTES);

			// Encrypt and write data
			data_t aes_buffer[ENCRYPTION_BLOCK_SIZE];
			auto current = allocator.GetHead();
			while (current != nullptr) {
				const auto write_size =
				    aes.Process(current->data.get(), current->current_position, aes_buffer, ENCRYPTION_BLOCK_SIZE);
				trans.write(aes_buffer, write_size);
				current = current->next.get();
			}

			// Finalize the last encrypted data and write tag
			data_t tag[TAG_BYTES];
			const auto write_size = aes.Finalize(aes_buffer, ENCRYPTION_BLOCK_SIZE, tag, TAG_BYTES);
			trans.write(aes_buffer, write_size);
			trans.write(tag, TAG_BYTES);
		}

	private:
		//! Protocol and corresponding transport that we're wrapping
		TProtocol &prot;
		TTransport &trans;

		//! AES context
		AESGCMState aes;

		//! Arena Allocator to fully materialize in memory before encrypting
		ArenaAllocator allocator;
	};

	//! Decrypt wrapper for a transport protocol TODO: implement a Finalize()
	class DecryptionTransport : public TTransport {
	public:
		DecryptionTransport(TProtocol &prot_p, const string &key)
		    : prot(prot_p), trans(*prot.getTransport()), aes(key), read_buffer_remaining(0) {
		}

		void Initialize(const uint8_t *buf, uint32_t len) {
			aes.InitializeDecryption(buf, len);
		}

		uint32_t read_virt(uint8_t *buf, uint32_t len) override {
			const uint32_t result = len;

			// Get from read buffer first (if there is anything in there)
			if (read_buffer_remaining != 0) {
				const auto copy_bytes = MinValue<idx_t>(read_buffer_remaining, len);
				const auto read_buffer_offset = AESGCMState::BLOCK_SIZE - read_buffer_remaining;

				memcpy(buf, read_buffer + read_buffer_offset, read_buffer_remaining);
				buf += copy_bytes;
				len -= copy_bytes;

				read_buffer_remaining -= copy_bytes;
			}

			// Process one block at a time
			while (len >= AESGCMState::BLOCK_SIZE) {
				ReadBlock(buf);
				buf += AESGCMState::BLOCK_SIZE;
				len -= AESGCMState::BLOCK_SIZE;
			}

			// Read last block into read buffer and copy just the bytes we need
			if (len != 0) {
				ReadBlock(read_buffer);
				memcpy(buf, read_buffer, len);
				read_buffer_remaining = AESGCMState::BLOCK_SIZE - len;
			}

			return result;
		}

		void Finalize() {
			if (read_buffer_remaining != 0) {
				// TODO throw
			}

			data_t computed_tag[TAG_BYTES];
			const auto read_size = aes.Finalize(read_buffer, AESGCMState::BLOCK_SIZE, computed_tag, TAG_BYTES);
			if (read_size != 0) {
				// TODO throw
			}

			data_t read_tag[TAG_BYTES];
			trans.read(read_tag, TAG_BYTES);
			if (memcmp(computed_tag, read_tag, TAG_BYTES) != 0) {
				// TODO throw
			}
		}

	private:
		void ReadBlock(uint8_t *buf) {
#ifdef DEBUG
			// Read from transport into aes_buffer
			auto size = trans.read(aes_buffer, AESGCMState::BLOCK_SIZE);
			D_ASSERT(size == AESGCMState::BLOCK_SIZE);
			// Decrypt from aes_buffer into buf
			size = aes.Process(aes_buffer, AESGCMState::BLOCK_SIZE, buf, AESGCMState::BLOCK_SIZE);
			D_ASSERT(size == AESGCMState::BLOCK_SIZE);
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
		AESGCMState aes;

		//! For buffering small reads
		data_t read_buffer[AESGCMState::BLOCK_SIZE];
		idx_t read_buffer_remaining;

		//! For decrypting one block at a time
		data_t aes_buffer[AESGCMState::BLOCK_SIZE];
	};

public:
	template <class T>
	static unique_ptr<T> Read(TProtocol &iprot, const string &key) {
		// Read IV
		data_t iv_buf[AESGCMState::BLOCK_SIZE];
		const auto iv_read_size = iprot.getTransport()->read(iv_buf, AESGCMState::BLOCK_SIZE);
		if (iv_read_size != AESGCMState::BLOCK_SIZE) {
			// TODO throw
		}

		// Create crypto protocol
		TCompactProtocolFactoryT<DecryptionTransport> tproto_factory;
		auto dprot = tproto_factory.getProtocol(make_shared<DecryptionTransport>(iprot, key));

		// Read encoded ciphertext length and initialize decryption using IV
		const uint32_t ciphertext_length = Load<uint32_t>(iv_buf);
		reinterpret_cast<DecryptionTransport &>(*dprot->getTransport())
		    .Initialize(iv_buf + sizeof(uint32_t), AESGCMState::BLOCK_SIZE - sizeof(uint32_t));

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
		// The length of the ciphertext must be known before we write anything else to file
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
