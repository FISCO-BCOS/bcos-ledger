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
 * @file TransactionStorage.h
 * @author: kyonRay
 * @date 2021-07-09
 */

#pragma once
#include "bcos-framework/interfaces/consensus/ConsensusNode.h"
#include "bcos-framework/interfaces/ledger/LedgerTypeDef.h"
#include "bcos-framework/interfaces/protocol/Block.h"
#include "bcos-framework/interfaces/protocol/BlockFactory.h"
#include "bcos-framework/interfaces/protocol/BlockHeader.h"
#include "bcos-framework/interfaces/protocol/BlockHeaderFactory.h"
#include "bcos-framework/interfaces/protocol/Transaction.h"
#include "bcos-framework/interfaces/protocol/TransactionReceipt.h"
#include "bcos-framework/interfaces/storage/Common.h"
#include "bcos-framework/interfaces/storage/TableInterface.h"
#include "bcos-ledger/libledger/utilities/FIFOCache.h"
#include "bcos-ledger/libledger/utilities/MerkleProofUtility.h"

namespace bcos::ledger
{
class TransactionStorage
{
public:
    using Ptr = std::shared_ptr<TransactionStorage>;
    explicit TransactionStorage(bcos::protocol::BlockFactory::Ptr _blockFactory)
      : m_blockFactory(_blockFactory), m_merkleProofUtility(std::make_shared<MerkleProofUtility>())
    {
        assert(m_blockFactory);

        auto txsDestructorThread = std::make_shared<ThreadPool>("txsCache", 1);
        auto rcptDestructorThread = std::make_shared<ThreadPool>("receiptsCache", 1);
        m_transactionsCache.setDestructorThread(txsDestructorThread);
        m_receiptCache.setDestructorThread(rcptDestructorThread);
    }

    void stop()
    {
        m_receiptCache.stop();
        m_transactionsCache.stop();
    }

    inline void receiptsCacheAdd(
        const protocol::BlockNumber& _num, const protocol::ReceiptsPtr& _receipts)
    {
        m_receiptCache.add(_num, _receipts);
    }

    inline void txsCacheAdd(
        const protocol::BlockNumber& _num, const protocol::TransactionsPtr& _txs)
    {
        m_transactionsCache.add(_num, _txs);
    }

    void getTxs(const bcos::protocol::BlockNumber& _blockNumber,
        const bcos::storage::TableFactoryInterface::Ptr& _tableFactory,
        std::function<void(Error::Ptr, bcos::protocol::TransactionsPtr)> _onGetTxs);
    void getReceipts(const bcos::protocol::BlockNumber& _blockNumber,
        const bcos::storage::TableFactoryInterface::Ptr& _tableFactory,
        std::function<void(Error::Ptr, bcos::protocol::ReceiptsPtr)> _onGetReceipts);
    void getTxProof(const crypto::HashType& _txHash,
        const bcos::storage::TableFactoryInterface::Ptr& _tableFactory,
        std::function<void(Error::Ptr, MerkleProofPtr)> _onGetProof);
    void getReceiptProof(protocol::TransactionReceipt::Ptr _receipt,
        const bcos::storage::TableFactoryInterface::Ptr& _tableFactory,
        std::function<void(Error::Ptr, MerkleProofPtr)> _onGetProof);
    void getBatchTxByHashList(std::shared_ptr<std::vector<std::string>> _hashList,
        const bcos::storage::TableFactoryInterface::Ptr& _tableFactory,
        std::function<void(Error::Ptr, protocol::TransactionsPtr)> _onGetTx);

    void getReceiptByTxHash(std::string _txHash,
        const bcos::storage::TableFactoryInterface::Ptr& _tableFactory,
        std::function<void(Error::Ptr, bcos::storage::Entry::Ptr)> _onGetReceipt);

    void getBatchReceiptsByHashList(std::shared_ptr<std::vector<std::string>> _hashList,
        const bcos::storage::TableFactoryInterface::Ptr& _tableFactory,
        std::function<void(Error::Ptr, protocol::ReceiptsPtr)> _onGetReceipt);

public:
    // transaction encoded in block
    void setNumberToTransactions(const bcos::protocol::Block::Ptr& _block,
        const storage::TableFactoryInterface::Ptr& _tableFactory);
    // receipts encoded in block
    void setBatchHashToReceipt(const bcos::protocol::Block::Ptr& _block,
        const storage::TableFactoryInterface::Ptr& _tableFactory);

    bool setHashToTx(const std::string& _txHash, const std::string& _encodeTx,
        const bcos::storage::TableFactoryInterface::Ptr& _tableFactory);

    bool setHashToReceipt(const std::string& _txHash, const std::string& _encodeReceipt,
        const bcos::storage::TableFactoryInterface::Ptr& _tableFactory);

private:
    inline protocol::TransactionFactory::Ptr getTransactionFactory()
    {
        return m_blockFactory->transactionFactory();
    }

    inline protocol::TransactionReceiptFactory::Ptr getReceiptFactory()
    {
        return m_blockFactory->receiptFactory();
    }

private:
    FIFOCache<protocol::TransactionsPtr, protocol::Transactions> m_transactionsCache;
    FIFOCache<protocol::ReceiptsPtr, protocol::Receipts> m_receiptCache;
    bcos::protocol::BlockFactory::Ptr m_blockFactory;
    ledger::MerkleProofUtility::Ptr m_merkleProofUtility;
};
}  // namespace bcos::ledger
