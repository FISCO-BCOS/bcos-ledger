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
#include "bcos-framework/libutilities/Common.h"
#include "bcos-framework/libutilities/Exceptions.h"
#include "bcos-framework/libutilities/ThreadPool.h"
#include "interfaces/protocol/ProtocolTypeDef.h"
#include "utilities/Common.h"
#include "utilities/MerkleProofUtility.h"
#include <utility>

#define LEDGER_LOG(LEVEL) BCOS_LOG(LEVEL) << LOG_BADGE("LEDGER")

namespace bcos::ledger
{
class Ledger : public LedgerInterface, public std::enable_shared_from_this<Ledger>
{
public:
    Ledger(bcos::protocol::BlockFactory::Ptr _blockFactory,
        bcos::storage::StorageInterface::Ptr _storage)
      : m_blockFactory(std::move(_blockFactory)), m_storage(std::move(_storage))
    {
        assert(m_blockFactory);
        assert(m_storage);
    };

    virtual ~Ledger() = default;

    void asyncPrewriteBlock(bcos::storage::StorageInterface::Ptr storage,
        bcos::protocol::Block::ConstPtr block, std::function<void(Error::Ptr&&)> callback) override;

    void asyncStoreTransactions(std::shared_ptr<std::vector<bytesConstPtr>> _txToStore,
        crypto::HashListPtr _txHashList, std::function<void(Error::Ptr)> _onTxStored) override;

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
    bool buildGenesisBlock(LedgerConfig::Ptr _ledgerConfig, const std::string& _groupId,
        size_t _gasLimit, const std::string& _genesisData);

private:
    Error::Ptr checkTableValid(Error::UniquePtr&& error,
        const std::optional<bcos::storage::Table>& table, const std::string_view& tableName);

    Error::Ptr checkEntryValid(Error::UniquePtr&& error,
        const std::optional<bcos::storage::Entry>& entry, const std::string_view& key);

    void asyncGetBlockHeader(bcos::protocol::Block::Ptr block,
        bcos::protocol::BlockNumber blockNumber, std::function<void(Error::Ptr&&)> callback);

    void asyncGetBlockTransactionHashes(bcos::protocol::BlockNumber blockNumber,
        std::function<void(Error::Ptr&&, std::vector<std::string>&&)> callback);

    void asyncBatchGetTransactions(std::shared_ptr<std::vector<std::string>> hashes,
        std::function<void(Error::Ptr&&, std::vector<protocol::Transaction::Ptr>&&)> callback);

    void asyncBatchGetReceipts(std::shared_ptr<std::vector<std::string>> hashes,
        std::function<void(Error::Ptr&&, std::vector<protocol::TransactionReceipt::Ptr>&&)>
            callback);

    void asyncGetSystemTableEntry(const std::string_view& table, const std::string_view& key,
        std::function<void(Error::Ptr&&, std::optional<bcos::storage::Entry>&&)> callback);

    std::vector<std::string_view> asView(const std::vector<std::string>& list);

    void getTxProof(const crypto::HashType& _txHash,
        std::function<void(Error::Ptr, MerkleProofPtr)> _onGetProof);
    void getReceiptProof(protocol::TransactionReceipt::Ptr _receipt,
        std::function<void(Error::Ptr, MerkleProofPtr)> _onGetProof);

    // notify block commit
    void notifyCommittedBlockNumber(protocol::BlockNumber _blockNumber);

    void createFileSystemTables(const std::string& _groupId);
    void recursiveBuildDir(const std::string& _absoluteDir);

    std::function<void(bcos::protocol::BlockNumber, std::function<void(Error::Ptr)>)>
        m_committedBlockNotifier;

    bcos::protocol::BlockFactory::Ptr m_blockFactory;
    bcos::storage::StorageInterface::Ptr m_storage;
    MerkleProofUtility m_merkleProofUtility;
};
}  // namespace bcos::ledger