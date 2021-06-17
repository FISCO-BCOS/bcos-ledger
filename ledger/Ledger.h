/**
 *  Copyright (C) 2021 FISCO BCOS.
 *  SPDX-License-Identifier: Apache-2.0
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 * @file Ledger.h
 * @author: kyonRay
 * @date 2021-04-13
 */
#pragma once
#include "bcos-framework/interfaces/ledger/LedgerInterface.h"
#include "bcos-framework/interfaces/ledger/LedgerTypeDef.h"
#include "bcos-framework/interfaces/protocol/BlockFactory.h"
#include "bcos-framework/interfaces/protocol/BlockHeaderFactory.h"
#include "bcos-framework/interfaces/storage/Common.h"
#include "bcos-framework/interfaces/storage/StorageInterface.h"
#include "bcos-framework/libtable/TableFactory.h"
#include "bcos-framework/libutilities/Common.h"
#include "bcos-framework/libutilities/Exceptions.h"
#include "bcos-framework/libutilities/ThreadPool.h"
#include "storage/StorageGetter.h"
#include "storage/StorageSetter.h"
#include "utilities/Common.h"
#include "utilities/FIFOCache.h"
#include "utilities/MerkleProofUtility.h"

#include <utility>

#define LEDGER_LOG(LEVEL) LOG(LEVEL) << LOG_BADGE("LEDGER")

namespace bcos::ledger
{
class Ledger : public LedgerInterface, public std::enable_shared_from_this<Ledger>
{
public:
    Ledger(bcos::protocol::BlockFactory::Ptr _blockFactory,
        bcos::storage::StorageInterface::Ptr _storage)
      : m_blockFactory(_blockFactory),
        m_storage(_storage),
        m_storageGetter(StorageGetter::storageGetterFactory()),
        m_storageSetter(StorageSetter::storageSetterFactory()),
        m_merkleProofUtility(std::make_shared<MerkleProofUtility>())
    {
        assert(m_blockFactory);
        assert(m_storage);

        auto blockDestructorThread = std::make_shared<ThreadPool>("blockCache", 1);
        auto headerDestructorThread = std::make_shared<ThreadPool>("headerCache", 1);
        auto txsDestructorThread = std::make_shared<ThreadPool>("txsCache", 1);
        auto rcptDestructorThread = std::make_shared<ThreadPool>("receiptsCache", 1);
        m_blockCache.setDestructorThread(blockDestructorThread);
        m_blockHeaderCache.setDestructorThread(headerDestructorThread);
        m_transactionsCache.setDestructorThread(txsDestructorThread);
        m_receiptCache.setDestructorThread(rcptDestructorThread);
    };
    void asyncCommitBlock(bcos::protocol::BlockHeader::Ptr _header,
        std::function<void(Error::Ptr, LedgerConfig::Ptr)> _onCommitBlock) override;

    void asyncStoreTransactions(std::shared_ptr<std::vector<bytesPointer>> _txToStore,
        crypto::HashListPtr _txHashList, std::function<void(Error::Ptr)> _onTxStored) override;

    void asyncStoreReceipts(storage::TableFactoryInterface::Ptr _tableFactory,
        protocol::Block::Ptr _block, std::function<void(Error::Ptr)> _onReceiptStored) override;

    void asyncGetBlockDataByNumber(bcos::protocol::BlockNumber _blockNumber, int32_t _blockFlag,
        std::function<void(Error::Ptr, bcos::protocol::Block::Ptr)> _onGetBlock) override;

    void asyncGetBlockNumber(
        std::function<void(Error::Ptr, bcos::protocol::BlockNumber)> _onGetBlock) override;

    void asyncGetBlockHashByNumber(bcos::protocol::BlockNumber _blockNumber,
        std::function<void(Error::Ptr, const bcos::crypto::HashType&)> _onGetBlock) override;

    void asyncGetBlockNumberByHash(const crypto::HashType& _blockHash,
        std::function<void(Error::Ptr, bcos::protocol::BlockNumber)> _onGetBlock) override;

    void asyncGetBatchTxsByHashList(crypto::HashListPtr _txHashList, bool _withProof,
        std::function<void(Error::Ptr, bcos::protocol::TransactionsPtr,
            std::shared_ptr<std::map<std::string, MerkleProofPtr>>)>
            _onGetTx) override;

    void asyncGetTransactionReceiptByHash(bcos::crypto::HashType const& _txHash, bool _withProof,
        std::function<void(
            Error::Ptr, bcos::protocol::TransactionReceipt::ConstPtr, MerkleProofPtr)>
            _onGetTx) override;

    void asyncGetTotalTransactionCount(
        std::function<void(Error::Ptr, int64_t, int64_t, bcos::protocol::BlockNumber)> _callback)
        override;

    void asyncGetSystemConfigByKey(const std::string& _key,
        std::function<void(Error::Ptr, std::string, bcos::protocol::BlockNumber)> _onGetConfig)
        override;

    void asyncGetNonceList(bcos::protocol::BlockNumber _startNumber, int64_t _offset,
        std::function<void(
            Error::Ptr, std::shared_ptr<std::map<protocol::BlockNumber, protocol::NonceListPtr>>)>
            _onGetList) override;

    void asyncGetNodeListByType(const std::string& _type,
        std::function<void(Error::Ptr, consensus::ConsensusNodeListPtr)> _onGetConfig) override;

    void setBlockFactory(const protocol::BlockFactory::Ptr& _blockFactory)
    {
        m_blockFactory = _blockFactory;
    }
    void setStorage(const storage::StorageInterface::Ptr& _storage) { m_storage = _storage; }

    /****** init ledger ******/
    bool buildGenesisBlock(
        LedgerConfig::Ptr _ledgerConfig, size_t _gasLimit, std::string _genesisData);

private:
    /****** base block data getter ******/
    void getBlock(const protocol::BlockNumber& _blockNumber, int32_t _blockFlag,
        std::function<void(Error::Ptr, protocol::Block::Ptr)>);
    void getLatestBlockNumber(std::function<void(protocol::BlockNumber)> _onGetNumber);
    void getBlockHeader(const bcos::protocol::BlockNumber& _blockNumber,
        std::function<void(Error::Ptr, protocol::BlockHeader::Ptr)> _onGetHeader);
    void getTxs(const bcos::protocol::BlockNumber& _blockNumber,
        std::function<void(Error::Ptr, bcos::protocol::TransactionsPtr)> _onGetTxs);
    void getReceipts(const bcos::protocol::BlockNumber& _blockNumber,
        std::function<void(Error::Ptr, bcos::protocol::ReceiptsPtr)> _onGetReceipts);
    void getTxProof(const crypto::HashType& _txHash,
        std::function<void(Error::Ptr, MerkleProofPtr)> _onGetProof);
    void getBatchTxProof(crypto::HashListPtr _txHashList,
        std::function<void(Error::Ptr, std::shared_ptr<std::map<std::string, MerkleProofPtr>>)>
            _onGetProof);
    void getReceiptProof(protocol::TransactionReceipt::Ptr _receipt,
        std::function<void(Error::Ptr, MerkleProofPtr)> _onGetProof);
    LedgerConfig::Ptr getLedgerConfig(protocol::BlockNumber _number, const crypto::HashType& _hash);

    /****** Ledger attribute getter ******/

    // TODO: if storage merge table factory, it can use new tableFactory
    inline storage::TableFactoryInterface::Ptr getMemoryTableFactory(
        const protocol::BlockNumber& _number)
    {
        // if you use this tableFactory to write data, _number should be latest block number;
        // if you use it read data, _number can be -1
        return std::make_shared<storage::TableFactory>(
            m_storage, m_blockFactory->cryptoSuite()->hashImpl(), _number);
    }

    inline protocol::TransactionFactory::Ptr getTransactionFactory()
    {
        return m_blockFactory->transactionFactory();
    }

    inline protocol::BlockHeaderFactory::Ptr getBlockHeaderFactory()
    {
        return m_blockFactory->blockHeaderFactory();
    }

    inline protocol::TransactionReceiptFactory::Ptr getReceiptFactory()
    {
        return m_blockFactory->receiptFactory();
    }

    inline StorageGetter::Ptr getStorageGetter() { return m_storageGetter; }
    inline StorageSetter::Ptr getStorageSetter() { return m_storageSetter; }
    inline bcos::storage::StorageInterface::Ptr getStorage() { return m_storage; }

    bool isBlockShouldCommit(
        const protocol::BlockNumber& _blockNumber, const std::string& _parentHash);


    /****** data writer ******/
    void writeNumber(const protocol::BlockNumber& _blockNumber,
        const bcos::storage::TableFactoryInterface::Ptr& _tableFactory);
    void writeTotalTransactionCount(const bcos::protocol::Block::Ptr& block,
        const bcos::storage::TableFactoryInterface::Ptr& _tableFactory);
    void writeNumber2Nonces(const bcos::protocol::Block::Ptr& block,
        const bcos::storage::TableFactoryInterface::Ptr& _tableFactory);
    void writeHash2Number(const bcos::protocol::BlockHeader::Ptr& header,
        const bcos::storage::TableFactoryInterface::Ptr& _tableFactory);
    void writeNumber2BlockHeader(const bcos::protocol::BlockHeader::Ptr& _header,
        const bcos::storage::TableFactoryInterface::Ptr& _tableFactory);
    // transaction encoded in block
    void writeNumber2Transactions(const bcos::protocol::Block::Ptr& _block,
        const storage::TableFactoryInterface::Ptr& _tableFactory);
    // receipts encoded in block
    void writeHash2Receipt(const bcos::protocol::Block::Ptr& _block,
        const storage::TableFactoryInterface::Ptr& _tableFactory);

    /****** runtime cache ******/
    FIFOCache<protocol::Block::Ptr, protocol::Block> m_blockCache;
    FIFOCache<protocol::BlockHeader::Ptr, protocol::BlockHeader> m_blockHeaderCache;
    FIFOCache<protocol::TransactionsPtr, protocol::Transactions> m_transactionsCache;
    FIFOCache<protocol::ReceiptsPtr, protocol::Receipts> m_receiptCache;

    boost::condition_variable m_signalled;
    boost::mutex x_signalled;

    size_t m_timeout = 10000;
    bcos::protocol::BlockFactory::Ptr m_blockFactory;
    bcos::storage::StorageInterface::Ptr m_storage;
    StorageGetter::Ptr m_storageGetter;
    StorageSetter::Ptr m_storageSetter;
    ledger::MerkleProofUtility::Ptr m_merkleProofUtility;
};
}  // namespace bcos::ledger