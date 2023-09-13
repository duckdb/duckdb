//===----------------------------------------------------------------------===//
//                         DuckDB
//
// crypto_wrapper.hpp
//
//
//===----------------------------------------------------------------------===/

#pragma once

#include "mbedtls_wrapper.hpp"
#include "thrift/protocol/TCompactProtocol.h"

namespace duckdb {

using duckdb_apache::thrift::protocol::TProtocol;
using duckdb_apache::thrift::transport::TTransport;
using AESGCMState = duckdb_mbedtls::MbedTlsWrapper::AESGCMState;
using duckdb_apache::thrift::protocol::TCompactProtocolFactoryT;

class ParquetCryptoWrapper {
private:
	//! Encrypted modules
	static constexpr idx_t LENGTH_BYTES = 4;
	static constexpr idx_t NONCE_BYTES = 12;
	static constexpr idx_t TAG_BYTES = 16;

private:
	//! Encryption wrapper for a transport protocol
	class EncryptionTransport : public TTransport {
	private:
		//! We encrypt this or less blocks per call
		static constexpr idx_t ENCRYPTION_BLOCK_SIZE = 2048;

	public:
		EncryptionTransport(TProtocol &prot_p, const string &key)
		    : prot(prot_p), trans(*prot.getTransport()), aes(key),
		      allocator(Allocator::DefaultAllocator(), ENCRYPTION_BLOCK_SIZE), finalized(false) {
			Initialize();
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
			finalized = true;

			// Write length
			const auto ciphertext_length = allocator.SizeInBytes();
			const uint32_t total_length = NONCE_BYTES + ciphertext_length + TAG_BYTES;
			trans.write(reinterpret_cast<const uint8_t *>(&total_length), LENGTH_BYTES);

			// Write nonce TODO actual nonce
			trans.write(nonce, NONCE_BYTES);

			// Encrypt and write data
			data_t aes_buffer[ENCRYPTION_BLOCK_SIZE];
			auto current = allocator.GetHead();
			while (current != nullptr) {
				for (idx_t pos = 0; pos < current->current_position; pos += ENCRYPTION_BLOCK_SIZE) {
					auto next = MinValue<idx_t>(current->current_position - pos, ENCRYPTION_BLOCK_SIZE);
					auto write_size = aes.Process(current->data.get() + pos, next, aes_buffer, ENCRYPTION_BLOCK_SIZE);
					trans.write(aes_buffer, write_size);
				}
				current = current->next.get();
			}

			// Finalize the last encrypted data and write tag
			data_t tag[TAG_BYTES];
			auto write_size = aes.Finalize(aes_buffer, ENCRYPTION_BLOCK_SIZE, tag, TAG_BYTES);
			trans.write(aes_buffer, write_size);
			trans.write(tag, TAG_BYTES);
		}

	private:
		void Initialize() {
			// Initialize allocator so we have at least 1 block
			allocator.Allocate(0);

			// Generate nonce and initialize AES TODO actual nonce
			for (idx_t i = 0; i < NONCE_BYTES; i++) {
				nonce[i] = i;
			}
			aes.InitializeEncryption(nonce, NONCE_BYTES);
		}

	private:
		//! Protocol and corresponding transport that we're wrapping
		TProtocol &prot;
		TTransport &trans;

		//! AES context
		AESGCMState aes;

		//! Nonce created by Initialize()
		data_t nonce[NONCE_BYTES];

		//! Arena Allocator to fully materialize in memory before encrypting
		ArenaAllocator allocator;

		//! Whether Finalize() has been called
		bool finalized;
	};

	//! Decryption wrapper for a transport protocol
	class DecryptionTransport : public TTransport {
	public:
		DecryptionTransport(TProtocol &prot_p, const string &key)
		    : prot(prot_p), trans(*prot.getTransport()), aes(key), read_buffer_remaining(0), read_bytes(0),
		      finalized(false) {
			Initialize();
		}

		~DecryptionTransport() override {
			if (!finalized) {
				// TODO throw
			}
		}

		uint32_t read_virt(uint8_t *buf, uint32_t len) override {
			const uint32_t result = len;

			// Get from read buffer first (if there is anything in there)
			if (read_buffer_remaining != 0) {
				const auto copy_bytes = MinValue<idx_t>(read_buffer_remaining, len);
				const auto read_buffer_offset = AESGCMState::BLOCK_SIZE - read_buffer_remaining;

				memcpy(buf, read_buffer + read_buffer_offset, copy_bytes);
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
			finalized = true;

			if (read_buffer_remaining != 0) {
				// TODO throw
			}

			data_t computed_tag[TAG_BYTES];
			const auto read_size = aes.Finalize(read_buffer, AESGCMState::BLOCK_SIZE, computed_tag, TAG_BYTES);
			if (read_size != 0) {
				// TODO throw
			}

			data_t read_tag[TAG_BYTES];
			read_bytes += trans.read(read_tag, TAG_BYTES);
			if (memcmp(computed_tag, read_tag, TAG_BYTES) != 0) {
				// TODO throw
			}

			if (read_bytes != length) {
				// TODO throw
			}
		}

	private:
		void Initialize() {
			// Read encoded length (don't add to read_bytes)
			data_t length_buf[LENGTH_BYTES];
			trans.read(length_buf, LENGTH_BYTES);
			length = Load<uint32_t>(length_buf);

			// Read nonce and initialize AES
			read_bytes += trans.read(nonce, NONCE_BYTES);
			aes.InitializeDecryption(nonce, NONCE_BYTES);
		}

		void ReadBlock(uint8_t *buf) {
			// Read from transport into aes_buffer
			read_bytes += trans.read(aes_buffer, AESGCMState::BLOCK_SIZE);
			// Decrypt from aes_buffer into buf
#ifdef DEBUG
			auto size = aes.Process(aes_buffer, AESGCMState::BLOCK_SIZE, buf, AESGCMState::BLOCK_SIZE);
			D_ASSERT(size == AESGCMState::BLOCK_SIZE);
#else
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

		//! Encoded length read by Initialize()
		uint32_t length;
		//! Nonce read by Initialize()
		data_t nonce[NONCE_BYTES];

		//! How many bytes were requested from trans
		idx_t read_bytes;
		//! Whether Finalize() has been called
		bool finalized;
	};

public:
	template <class T>
	static unique_ptr<T> Read(TProtocol &iprot, const string &key) {
		// Create decryption protocol
		TCompactProtocolFactoryT<DecryptionTransport> tproto_factory;
		auto dprot = tproto_factory.getProtocol(make_shared<DecryptionTransport>(iprot, key));
		auto &dtrans = reinterpret_cast<DecryptionTransport &>(*dprot->getTransport());

		// Read the object
		auto object = make_uniq<T>();
		object->read(dprot.get());

		// Verify AES tag and read length
		dtrans.Finalize();

		return object;
	}

	template <class T>
	static void Write(const T &object, TProtocol &oprot, const string &key) {
		// Create encryption protocol
		TCompactProtocolFactoryT<EncryptionTransport> tproto_factory;
		auto eprot = tproto_factory.getProtocol(make_shared<EncryptionTransport>(oprot, key));
		auto &etrans = reinterpret_cast<EncryptionTransport &>(*eprot->getTransport());

		// Write the object in memory
		object.write(eprot.get());

		// Encrypt and write to oprot
		etrans.Finalize();
	}
};

} // namespace duckdb
