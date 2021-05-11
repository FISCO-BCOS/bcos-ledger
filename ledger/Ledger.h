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
#include "utilities/Common.h"
#include "utilities/FIFOCache.h"
#include "storage/StorageGetter.h"
#include "storage/StorageSetter.h"

#include <utility>

#define LEDGER_LOG(LEVEL) LOG(LEVEL) << LOG_BADGE("LEDGER")

namespace bcos::ledger
{

class Ledger: public LedgerInterface, public std::enable_shared_from_this<Ledger>
{
public:
    Ledger(bcos::protocol::BlockFactory::Ptr _blockFactory,
        bcos::protocol::BlockHeaderFactory::Ptr _headerFactory,
        bcos::storage::StorageInterface::Ptr _storage)
      : m_blockFactory(_blockFactory),
        m_headerFactory(_headerFactory),
        m_storage(_storage),
        m_storageGetter(StorageGetter::storageGetterFactory()),
        m_storageSetter(StorageSetter::storageSetterFactory())
    {
        assert(m_blockFactory);
        assert(m_headerFactory);
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
    void asyncCommitBlock(bcos::protocol::BlockNumber _blockNumber,
        const gsl::span<const protocol::Signature>& _signList,
        std::function<void(Error::Ptr,  LedgerConfig::Ptr)> _onCommitBlock) override;

    void asyncPreStoreTransactions(
        bcos::protocol::Block::Ptr _txsToStore,
        protocol::BlockNumber _number, std::function<void(Error::Ptr)> _onTxsStored) override;

    void asyncGetBlockDataByNumber(bcos::protocol::BlockNumber _blockNumber, int32_t _blockFlag,
        std::function<void(Error::Ptr, bcos::protocol::Block::Ptr)> _onGetBlock) override;

    void asyncGetBlockNumber(
        std::function<void(Error::Ptr, bcos::protocol::BlockNumber)> _onGetBlock) override;

    void asyncGetBlockHashByNumber(bcos::protocol::BlockNumber _blockNumber,
        std::function<void(Error::Ptr, const bcos::crypto::HashType)> _onGetBlock) override;

    void asyncGetBlockNumberByHash(const crypto::HashType& _blockHash,
        std::function<void(Error::Ptr, bcos::protocol::BlockNumber)> _onGetBlock) override;

    void asyncGetTransactionByHash(crypto::HashType const& _txHash, bool _withProof,
        std::function<void(Error::Ptr, protocol::Transaction::ConstPtr, MerkleProofPtr)> _onGetTx)
        override;

    void asyncGetTransactionReceiptByHash(bcos::crypto::HashType const& _txHash, bool _withProof,
        std::function<void(Error::Ptr, bcos::protocol::TransactionReceipt::ConstPtr, MerkleProofPtr)> _onGetTx)
        override;

    void asyncGetTransactionByBlockNumberAndIndex(protocol::BlockNumber _blockNumber,
        int64_t _index, bool _withProof,
        std::function<void(Error::Ptr, protocol::Transaction::ConstPtr, MerkleProofPtr)> _onGetTx)
        override;

    void asyncGetReceiptByBlockNumberAndIndex(protocol::BlockNumber _blockNumber, int64_t _index,
        bool _withProof,
        std::function<void(Error::Ptr, protocol::TransactionReceipt::ConstPtr, MerkleProofPtr)>
            _onGetTx) override;

    void asyncGetTotalTransactionCount(
        std::function<void(Error::Ptr, int64_t, int64_t, bcos::protocol::BlockNumber)> _callback)
        override;

    void asyncGetSystemConfigByKey(const std::string& _key,
        std::function<void(Error::Ptr, std::string, bcos::protocol::BlockNumber)> _onGetConfig)
        override;

    void asyncGetNonceList(bcos::protocol::BlockNumber _blockNumber,
        std::function<void(Error::Ptr, bcos::protocol::NonceListPtr)> _onGetList) override;

    void asyncGetNodeListByType(const std::string& _type,
        std::function<void(Error::Ptr, consensus::ConsensusNodeListPtr)> _onGetConfig) override;

    void setBlockFactory(const protocol::BlockFactory::Ptr& mBlockFactory)
    {
        m_blockFactory = mBlockFactory;
    }
    void setHeaderFactory(const protocol::BlockHeaderFactory::Ptr& mHeaderFactory)
    {
        m_headerFactory = mHeaderFactory;
    }
    void setStorage(const storage::StorageInterface::Ptr& mStorage)
    {
        m_storage = mStorage;
    }

    /****** init ledger ******/
    bool buildGenesisBlock(LedgerConfig::Ptr _ledgerConfig);

private:
    /****** base block data getter ******/
    bcos::protocol::Block::Ptr getBlock(bcos::protocol::BlockNumber const& _blockNumber,  int32_t _blockFlag);

    /****** block attribute getter ******/
    bcos::protocol::BlockNumber getBlockNumberByHash(bcos::crypto::HashType const& _hash);
    std::shared_ptr<std::pair<bcos::protocol::BlockNumber, int64_t>> getBlockNumberAndIndexByTxHash(bcos::crypto::HashType const& _txHash);
    bcos::protocol::BlockNumber getLatestBlockNumber();
    bcos::protocol::BlockNumber getNumberFromStorage();
    std::string getLatestBlockHash();
    std::string getHashFromStorage();
    bcos::protocol::BlockHeader::Ptr getBlockHeader(
        const bcos::protocol::BlockNumber& _blockNumber);
    std::shared_ptr<Child2ParentMap> getChild2ParentCacheByReceipt(
        std::shared_ptr<Parent2ChildListMap> _parent2ChildList, protocol::BlockNumber _blockNumber);
    std::shared_ptr<Child2ParentMap> getChild2ParentCacheByTransaction(
        std::shared_ptr<Parent2ChildListMap> _parent2Child, protocol::BlockNumber _blockNumber);

    std::shared_ptr<Child2ParentMap> getChild2ParentCache(SharedMutex& _mutex,
        std::pair<bcos::protocol::BlockNumber, std::shared_ptr<Child2ParentMap>>& _cache,
        std::shared_ptr<Parent2ChildListMap> _parent2Child, protocol::BlockNumber _blockNumber);

    std::shared_ptr<Parent2ChildListMap> getParent2ChildListByReceiptProofCache(
        protocol::BlockNumber _blockNumber, protocol::ReceiptsPtr _receipts);

    std::shared_ptr<Parent2ChildListMap> getParent2ChildListByTxsProofCache(
        protocol::BlockNumber _blockNumber, protocol::TransactionsPtr _txs);

    inline storage::TableFactoryInterface::Ptr getMemoryTableFactory(const protocol::BlockNumber& _number)
    {
        // if you use this tableFactory to write data, _number should be latest block number;
        // if you use it read data, _number can be -1
        return std::make_shared<storage::TableFactory>(
            m_storage, m_blockFactory->cryptoSuite()->hashImpl(), _number);
    }


    inline StorageGetter::Ptr getStorageGetter(){
        return m_storageGetter;
    }
    inline StorageSetter::Ptr getStorageSetter(){
        return m_storageSetter;
    }

    /****** base tx data getter ******/
    bcos::protocol::TransactionsPtr getTxs(bcos::protocol::BlockNumber const& _blockNumber);
    bcos::protocol::ReceiptsPtr getReceipts(bcos::protocol::BlockNumber const& _blockNumber);

    bcos::protocol::Block::Ptr decodeBlock(const std::string& _blockStr);
    bcos::protocol::BlockHeader::Ptr decodeBlockHeader(const std::string& _headerStr);
    bool isBlockShouldCommit(bcos::protocol::BlockNumber const& _blockNumber);


    /****** data writer ******/
    void writeNumber(const protocol::BlockNumber & _blockNumber,
        const bcos::storage::TableFactoryInterface::Ptr& _tableFactory);
    void writeTotalTransactionCount(const bcos::protocol::Block::Ptr& block,
        const bcos::storage::TableFactoryInterface::Ptr& _tableFactory);
    void writeTxToBlock(const bcos::protocol::Block::Ptr& block,
        const bcos::storage::TableFactoryInterface::Ptr& _tableFactory);
    void writeNoncesToBlock(const bcos::protocol::Block::Ptr& _block,
        const bcos::storage::TableFactoryInterface::Ptr& _tableFactory);
    void writeHash2Number(const bcos::protocol::Block::Ptr& block,
        const bcos::storage::TableFactoryInterface::Ptr& _tableFactory);
    // TODO: is it necessary?
    void writeNumber2Block(const bcos::protocol::Block::Ptr& block,
        const bcos::storage::TableFactoryInterface::Ptr& _tableFactory);
    void writeNumber2BlockHeader(const bcos::protocol::Block::Ptr& _block,
        const bcos::storage::TableFactoryInterface::Ptr& _tableFactory);
    // transaction encoded in block
    void writeNumber2Transactions(const bcos::protocol::Block::Ptr& _block,
        const bcos::protocol::BlockNumber& _number,
        const bcos::storage::TableFactoryInterface::Ptr& _tableFactory);
    // receipts encoded in block
    void writeNumber2Receipts(const bcos::protocol::Block::Ptr& _block,
        const bcos::protocol::BlockNumber& _number,
        const storage::TableFactoryInterface::Ptr& _tableFactory);

    inline bcos::storage::StorageInterface::Ptr getState(){
        return m_storage;
    }

    /****** runtime cache ******/
    FIFOCache<protocol::Block::Ptr, protocol::Block> m_blockCache;
    FIFOCache<protocol::BlockHeader::Ptr, protocol::BlockHeader> m_blockHeaderCache;
    FIFOCache<protocol::TransactionsPtr, protocol::Transactions>
        m_transactionsCache;
    FIFOCache<protocol::ReceiptsPtr, protocol::Receipts> m_receiptCache;

    mutable SharedMutex m_blockNumberMutex;
    bcos::protocol::BlockNumber m_blockNumber = -1;

    mutable SharedMutex m_blockHashMutex;
    std::string m_blockHash;

    std::map<std::string, SystemConfigRecordCache> m_systemConfigRecordMap;
    mutable SharedMutex m_systemConfigMutex;

    std::pair<bcos::protocol::BlockNumber,
        std::shared_ptr<Parent2ChildListMap>>
        m_transactionWithProof = std::make_pair(0, nullptr);
    mutable SharedMutex m_transactionWithProofMutex;

    std::pair<bcos::protocol::BlockNumber,
        std::shared_ptr<Parent2ChildListMap>>
        m_receiptWithProof = std::make_pair(0, nullptr);
    mutable SharedMutex m_receiptWithProofMutex;

    std::pair<bcos::protocol::BlockNumber, std::shared_ptr<Child2ParentMap>> m_receiptChild2ParentCache;
    mutable SharedMutex x_receiptChild2ParentCache;

    std::pair<bcos::protocol::BlockNumber, std::shared_ptr<Child2ParentMap>> m_txsChild2ParentCache;
    mutable SharedMutex x_txsChild2ParentCache;

    bcos::protocol::BlockFactory::Ptr m_blockFactory;
    bcos::protocol::BlockHeaderFactory::Ptr m_headerFactory;
    bcos::storage::StorageInterface::Ptr m_storage;
    StorageGetter::Ptr m_storageGetter;
    StorageSetter::Ptr m_storageSetter;
};
} // namespace bcos