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
#include "interfaces/ledger/LedgerInterface.h"
#include "interfaces/protocol/BlockFactory.h"
#include "interfaces/protocol/BlockHeaderFactory.h"
#include "interfaces/storage/Common.h"
#include "interfaces/storage/StorageInterface.h"
#include "libtable/TableFactory.h"
#include "libutilities/Common.h"
#include "libutilities/Exceptions.h"
#include "libutilities/ThreadPool.h"
#include "utilities/Common.h"
#include "utilities/FIFOCache.h"
#include "storage/StorageGetter.h"
#include "storage/StorageSetter.h"

#include <utility>

#define LEDGER_LOG(LEVEL) LOG(LEVEL) << LOG_BADGE("LEDGER")

namespace bcos::ledger
{

class Ledger: public LedgerInterface {
public:
    Ledger(bcos::storage::TableFactoryInterface::Ptr _tableFactory,
        bcos::protocol::BlockFactory::Ptr _blockFactory,
        bcos::protocol::BlockHeaderFactory::Ptr _headerFactory,
        bcos::storage::StorageInterface::Ptr _state)
      : m_blockFactory(_blockFactory),
        m_headerFactory(_headerFactory),
        m_tableFactory(_tableFactory),
        m_state(_state),
        m_storageGetter(StorageGetter::storageGetterFactory()),
        m_storageSetter(StorageSetter::storageSetterFactory())
    {
        assert(m_blockFactory);
        assert(m_headerFactory);
        assert(m_tableFactory);
        assert(m_state);

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
        bcos::protocol::SignatureListPtr _signList,
        std::function<void(Error::Ptr)> _onCommitBlock) override;
    void asyncGetTransactionByHash(const bcos::crypto::HashType& _txHash,
        std::function<void(Error::Ptr, bcos::protocol::Transaction::ConstPtr)> _onGetTx) override;
    void asyncGetTransactionReceiptByHash(const bcos::crypto::HashType& _txHash,
        std::function<void(Error::Ptr, bcos::protocol::TransactionReceipt::ConstPtr)> _onGetTx)
        override;
    void asyncGetReceiptsByBlockNumber(bcos::protocol::BlockNumber _blockNumber,
                                       std::function<void(Error::Ptr, bcos::protocol::ReceiptsConstPtr)> _onGetReceipt) override;
    void asyncPreStoreTransactions(
        const protocol::Blocks& _txsToStore, std::function<void(Error::Ptr)> _onTxsStored) override;
    void asyncGetTransactionsByBlockNumber(bcos::protocol::BlockNumber _blockNumber,
                                           std::function<void(Error::Ptr, bcos::protocol::TransactionsConstPtr)> _onGetTx) override;
    void asyncGetTransactionByBlockHashAndIndex(const crypto::HashType& _blockHash, int64_t _index,
                                                std::function<void(Error::Ptr, bcos::protocol::Transaction::ConstPtr)> _onGetTx) override;
    void asyncGetTransactionByBlockNumberAndIndex(bcos::protocol::BlockNumber _blockNumber,
                                                  int64_t _index,
                                                  std::function<void(Error::Ptr, bcos::protocol::Transaction::ConstPtr)> _onGetTx) override;
    void asyncGetTotalTransactionCount(
        std::function<void(Error::Ptr, int64_t, int64_t, bcos::protocol::BlockNumber)> _callback)
        override;
    void asyncGetTransactionReceiptProof(const crypto::HashType& _blockHash, const int64_t& _index,
        std::function<void(Error::Ptr, MerkleProofPtr)> _onGetProof) override;
    void asyncGetTransactionProof(const crypto::HashType& _blockHash, const int64_t& _index,
        std::function<void(Error::Ptr, MerkleProofPtr)> _onGetProof) override;
    void asyncGetTransactionProofByHash(
        const bcos::crypto::HashType& _txHash, std::function<void(Error::Ptr, MerkleProofPtr)> _onGetProof) override;
    void asyncGetBlockNumber(
        std::function<void(Error::Ptr, bcos::protocol::BlockNumber)> _onGetBlock) override;
    void asyncGetBlockHashByNumber(bcos::protocol::BlockNumber number,
        std::function<void(Error::Ptr, std::shared_ptr<const bcos::crypto::HashType>)> _onGetBlock) override;
    void asyncGetBlockByHash(const bcos::crypto::HashType& _blockHash,
        std::function<void(Error::Ptr, bcos::protocol::Block::Ptr)> _onGetBlock) override;
    void asyncGetBlockNumberByHash(const crypto::HashType& _blockHash,
                                   std::function<void(Error::Ptr, std::shared_ptr<const bcos::crypto::HashType>)> _onGetBlock) override;
    void asyncGetBlockByNumber(bcos::protocol::BlockNumber _blockNumber,
        std::function<void(Error::Ptr, bcos::protocol::Block::Ptr)> _onGetBlock) override;
    void asyncGetBlockEncodedByNumber(bcos::protocol::BlockNumber _blockNumber,
        std::function<void(Error::Ptr, bytesPointer)> _onGetBlock) override;
    void asyncGetBlockHeaderByNumber(bcos::protocol::BlockNumber _blockNumber,
        std::function<void(
            Error::Ptr, std::shared_ptr<const std::pair<bcos::protocol::BlockHeader::Ptr,
                            bcos::protocol::SignatureListPtr>>)>
            _onGetBlock) override;
    void asyncGetBlockHeaderByHash(const bcos::crypto::HashType& _blockHash,
        std::function<void(
            Error::Ptr, std::shared_ptr<const std::pair<bcos::protocol::BlockHeader::Ptr,
                            bcos::protocol::SignatureListPtr>>)>
            _onGetBlock) override;
    void asyncGetCode(const std::string& _tableID, bcos::Address _codeAddress,
        std::function<void(Error::Ptr, std::shared_ptr<const bytes>)> _onGetCode) override;
    void asyncGetSystemConfigByKey(const std::string& _key,
        std::function<void(
            Error::Ptr, std::shared_ptr<const std::pair<std::string, bcos::protocol::BlockNumber>>)>
            _onGetConfig) override;
    void asyncGetNonceList(bcos::protocol::BlockNumber _blockNumber,
        std::function<void(Error::Ptr, bcos::protocol::NonceListPtr)> _onGetList) override;

private:
    /****** init ledger ******/
    bool buildGenesisBlock();

    /****** base block data getter ******/
    bcos::protocol::Block::Ptr getFullBlock(bcos::protocol::BlockNumber const& _blockNumber);
    bcos::protocol::Block::Ptr getBlock(bcos::protocol::BlockNumber const& _blockNumber);
    bcos::protocol::Block::Ptr getBlock(bcos::crypto::HashType const& _blockHash);
    bytesPointer getEncodeBlock(const bcos::protocol::BlockNumber& _blockNumber);

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
        std::shared_ptr<Parent2ChildListMap> _parent2ChildList, long long int _blockNumber);
    std::shared_ptr<Child2ParentMap> getChild2ParentCacheByTransaction(
        std::shared_ptr<Parent2ChildListMap> _parent2Child, long long int _blockNumber);

    std::shared_ptr<Child2ParentMap> getChild2ParentCache(SharedMutex& _mutex,
        std::pair<bcos::protocol::BlockNumber, std::shared_ptr<Child2ParentMap>>& _cache,
        std::shared_ptr<Parent2ChildListMap> _parent2Child, long long int _blockNumber);

    std::shared_ptr<Parent2ChildListMap> getParent2ChildListByReceiptProofCache(
        protocol::BlockNumber _blockNumber, protocol::ReceiptsPtr _receipts);

    std::shared_ptr<Parent2ChildListMap> getParent2ChildListByTxsProofCache(
        protocol::BlockNumber _blockNumber, protocol::TransactionsPtr _txs);

    void getMerkleProof(const crypto::HashType& _txHash,
                        const Parent2ChildListMap & parent2ChildList,
                        const Child2ParentMap& child2Parent,
                        MerkleProof& merkleProof);

    inline storage::TableFactoryInterface::Ptr getMemoryTableFactory()
    {
        return m_tableFactory;
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
        return m_state;
    }

    /****** runtime cache ******/
    FIFOCache<bcos::protocol::Block::Ptr, bcos::protocol::Block> m_blockCache;
    FIFOCache<bcos::protocol::BlockHeader::Ptr, bcos::protocol::BlockHeader> m_blockHeaderCache;
    FIFOCache<bcos::protocol::TransactionsPtr, bcos::protocol::Transaction>
        m_transactionsCache;
    FIFOCache<bcos::protocol::ReceiptsPtr, bcos::protocol::TransactionReceipt> m_receiptCache;

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
    bcos::storage::TableFactoryInterface::Ptr m_tableFactory;
    bcos::storage::StorageInterface::Ptr m_state;
    StorageGetter::Ptr m_storageGetter;
    StorageSetter::Ptr m_storageSetter;
};
} // namespace bcos