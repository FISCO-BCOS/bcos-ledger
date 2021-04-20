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
#include "interfaces/ledger/ledgerInterface.h"
#include "interfaces/storage/TableInterface.h"
#include "libutilities/Common.h"
#include "libutilities/ThreadPool.h"
#include "utilities/BlockCache.h"
#include "utilities/Common.h"

#include <utility>

#define LEDGER_LOG(LEVEL) LOG(LEVEL) << LOG_BADGE("LEDGER")

namespace bcos::ledger
{
class Ledger: public LedgerInterface {
public:
    Ledger(){
        m_destructorThread = std::make_shared<ThreadPool>("blockCache", 1);
        m_blockCache.setDestructorThread(m_destructorThread);
    };
    void asyncCommitBlock(bcos::protocol::BlockNumber _blockNumber,
        bcos::protocol::SignatureListPtr _signList,
        std::function<void(Error::Ptr)> _onCommitBlock) override;
    void asyncGetTxByHash(const bcos::crypto::HashType& _txHash,
        std::function<void(Error::Ptr, bcos::protocol::Transaction::ConstPtr)> _onGetTx) override;
    void asyncGetTransactionReceiptByHash(const bcos::crypto::HashType& _txHash,
        std::function<void(Error::Ptr, bcos::protocol::TransactionReceipt::ConstPtr)> _onGetTx)
        override;
    void asyncPreStoreTxs(
        const protocol::Blocks& _txsToStore, std::function<void(Error::Ptr)> _onTxsStored) override;
    void asyncGetTotalTransactionCount(
        std::function<void(Error::Ptr, int64_t, int64_t, bcos::protocol::BlockNumber)> _callback)
        override;
    void asyncGetTransactionReceiptProof(const crypto::HashType& _blockHash, const int64_t& _index,
        std::function<void(Error::Ptr, MerkleProofPtr)> _onGetProof) override;
    void getTransactionProof(const crypto::HashType& _blockHash, const int64_t& _index,
        std::function<void(Error::Ptr, MerkleProofPtr)> _onGetProof) override;
    void asyncGetTransactionProofByHash(
        const bcos::crypto::HashType& _txHash, std::function<void(Error::Ptr, MerkleProofPtr)> _onGetProof) override;
    void asyncGetBlockNumber(
        std::function<void(Error::Ptr, bcos::protocol::BlockNumber)> _onGetBlock) override;
    void asyncGetBlockHashByNumber(bcos::protocol::BlockNumber number,
        std::function<void(Error::Ptr, std::shared_ptr<const bcos::crypto::HashType>)> _onGetBlock) override;
    void asyncGetBlockByHash(const bcos::crypto::HashType& _blockHash,
        std::function<void(Error::Ptr, bcos::protocol::Block::Ptr)> _onGetBlock) override;
    void asyncGetBlockByNumber(bcos::protocol::BlockNumber _blockNumber,
        std::function<void(Error::Ptr, bcos::protocol::Block::Ptr)> _onGetBlock) override;
    void asyncGetBlockEncodedByNumber(bcos::protocol::BlockNumber _blockNumber,
        std::function<void(Error::Ptr, std::shared_ptr<const bytes>)> _onGetBlock) override;
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
    bcos::protocol::Block::Ptr getBlock(bcos::protocol::BlockNumber const& _blockNumber);
    bcos::protocol::Block::Ptr getBlock(bcos::crypto::HashType const& _blockHash, bcos::protocol::BlockNumber const& _blockNumber);
    std::shared_ptr<bcos::crypto::HashType>  getBlockHashByNumber(bcos::protocol::BlockNumber _blockNumber) const;
    bcos::protocol::BlockNumber getBlockNumberByHash(bcos::crypto::HashType const& _hash);
    bcos::protocol::BlockNumber getLatestBlockNumber();
    bcos::protocol::BlockHeader::Ptr getBlockHeaderFromBlock(bcos::protocol::Block::Ptr _block);
    std::shared_ptr<Child2ParentMap> getChild2ParentCacheByReceipt(
        std::shared_ptr<Parent2ChildListMap> _parent2ChildList, bcos::protocol::Block::Ptr _block);
    std::shared_ptr<Child2ParentMap> getChild2ParentCacheByTransaction(
        std::shared_ptr<Parent2ChildListMap> _parent2Child, bcos::protocol::Block::Ptr _block);

    std::shared_ptr<Child2ParentMap> getChild2ParentCache(SharedMutex& _mutex,
                                                          std::pair<bcos::protocol::BlockNumber, std::shared_ptr<Child2ParentMap>>& _cache,
                                                          std::shared_ptr<Parent2ChildListMap> _parent2Child, bcos::protocol::Block::Ptr _block);
    void getMerkleProof(bytes const& _txHash,
                        const std::map<std::string, std::vector<std::string>>& parent2ChildList,
                        const Child2ParentMap& child2Parent,
                        std::vector<std::pair<std::vector<std::string>, std::vector<std::string>>>& merkleProof);


    /****** data writer ******/
    void writeBytesToField(std::shared_ptr<bytes> _data, bcos::storage::EntryInterface::Ptr _entry,
                           std::string const& _fieldName);
    void writeBlockToField(bcos::protocol::Block const& _block, bcos::storage::EntryInterface::Ptr _entry);
    void writeNumber(const bcos::protocol::Block& _block,
                     bcos::storage::TableFactory::Ptr _tableFactory);
    void writeTotalTransactionCount(const bcos::protocol::Block& block,
                                    bcos::storage::TableFactory::Ptr _tableFactory);
    void writeTxToBlock(const bcos::protocol::Block& block,
                        bcos::storage::TableFactory::Ptr _tableFactory);
    void writeNumber2Hash(const bcos::protocol::Block& block,
                          bcos::storage::TableFactory::Ptr _tableFactory);
    void writeHash2Block(
        bcos::protocol::Block& block, bcos::storage::TableFactory::Ptr _tableFactory);
    void writeHash2BlockHeader(
        bcos::protocol::Block& _block, bcos::storage::TableFactory::Ptr _tableFactory);


    /****** runtime cache ******/
    BlockCache m_blockCache;
    bcos::ThreadPool::Ptr m_destructorThread;

    mutable SharedMutex m_blockNumberMutex;
    bcos::protocol::BlockNumber m_blockNumber = -1;

    std::map<std::string, SystemConfigRecordCache> m_systemConfigRecord;
    mutable SharedMutex m_systemConfigMutex;

    std::pair<bcos::protocol::BlockNumber,
        std::shared_ptr<std::map<std::string, std::vector<std::string>>>>
        m_transactionWithProof = std::make_pair(0, nullptr);
    mutable SharedMutex m_transactionWithProofMutex;

    std::pair<bcos::protocol::BlockNumber,
        std::shared_ptr<std::map<std::string, std::vector<std::string>>>>
        m_receiptWithProof = std::make_pair(0, nullptr);
    mutable SharedMutex m_receiptWithProofMutex;

    std::pair<bcos::protocol::BlockNumber, std::shared_ptr<Child2ParentMap>> m_receiptChild2ParentCache;
    mutable SharedMutex x_receiptChild2ParentCache;

    std::pair<bcos::protocol::BlockNumber, std::shared_ptr<Child2ParentMap>> m_txsChild2ParentCache;
    mutable SharedMutex x_txsChild2ParentCache;

    bcos::storage::TableFactory::Ptr m_tableFactory;

    long long int getBlockNumberByHash();
};
} // namespace bcos