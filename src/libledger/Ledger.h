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
#include "bcos-ledger/libledger/storage/BlockStorage.h"
#include "bcos-ledger/libledger/storage/ConfigStorage.h"
#include "bcos-ledger/libledger/storage/StorageUtilities.h"
#include "bcos-ledger/libledger/storage/TransactionStorage.h"
#include "bcos-ledger/libledger/utilities/Common.h"
#include "bcos-ledger/libledger/utilities/FIFOCache.h"
#include "bcos-ledger/libledger/utilities/MerkleProofUtility.h"

#include <utility>

#define LEDGER_LOG(LEVEL) BCOS_LOG(LEVEL) << LOG_BADGE("LEDGER")

namespace bcos::ledger
{
class Ledger : public LedgerInterface, public std::enable_shared_from_this<Ledger>
{
public:
    Ledger(bcos::protocol::BlockFactory::Ptr _blockFactory,
        bcos::storage::StorageInterface::Ptr _storage)
      : m_blockFactory(_blockFactory),
        m_storage(_storage),
        m_blockStorage(std::make_shared<BlockStorage>(_blockFactory)),
        m_txStorage(std::make_shared<TransactionStorage>(_blockFactory)),
        m_configStorage(std::make_shared<ConfigStorage>(_blockFactory->cryptoSuite()->keyFactory()))
    {
        assert(m_blockFactory);
        assert(m_storage);

        auto blockDestructorThread = std::make_shared<ThreadPool>("blockCache", 1);
        m_blockCache.setDestructorThread(blockDestructorThread);
    };

    virtual void stop()
    {
        m_blockCache.stop();
        m_txStorage->stop();
        m_blockStorage->stop();
    }
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

    void registerCommittedBlockNotifier(
        std::function<void(bcos::protocol::BlockNumber, std::function<void(Error::Ptr)>)>
            _committedBlockNotifier)
    {
        m_committedBlockNotifier = _committedBlockNotifier;
    }

    /****** init ledger ******/
    bool buildGenesisBlock(
        LedgerConfig::Ptr _ledgerConfig, size_t _gasLimit, std::string _genesisData);

private:
    /****** base block data getter ******/
    void getBlock(const protocol::BlockNumber& _blockNumber, int32_t _blockFlag,
        std::function<void(Error::Ptr, protocol::Block::Ptr)>);
    void getLatestBlockNumber(std::function<void(protocol::BlockNumber)> _onGetNumber);

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

    inline protocol::BlockHeaderFactory::Ptr getBlockHeaderFactory()
    {
        return m_blockFactory->blockHeaderFactory();
    }

    inline protocol::TransactionReceiptFactory::Ptr getReceiptFactory()
    {
        return m_blockFactory->receiptFactory();
    }
    inline bcos::storage::StorageInterface::Ptr getStorage() { return m_storage; }

    bool isBlockShouldCommit(
        const protocol::BlockNumber& _blockNumber, const std::string& _parentHash);

    // notify block commit
    void notifyCommittedBlockNumber(protocol::BlockNumber _blockNumber);

    /****** runtime cache ******/
    FIFOCache<protocol::Block::Ptr, protocol::Block> m_blockCache;
    std::function<void(bcos::protocol::BlockNumber, std::function<void(Error::Ptr)>)>
        m_committedBlockNotifier;

    mutable SharedMutex m_blockNumberMutex;
    protocol::BlockNumber m_blockNumber = -1;

    size_t m_timeout = 10000;
    bcos::protocol::BlockFactory::Ptr m_blockFactory;
    bcos::storage::StorageInterface::Ptr m_storage;
    BlockStorage::Ptr m_blockStorage;
    TransactionStorage::Ptr m_txStorage;
    ConfigStorage::Ptr m_configStorage;
};
}  // namespace bcos::ledger