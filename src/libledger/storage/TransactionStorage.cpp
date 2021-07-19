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
 * @file TransactionStorage.cpp
 * @author: kyonRay
 * @date 2021-07-09
 */

#include "TransactionStorage.h"
#include "StorageUtilities.h"
#include "bcos-ledger/libledger/utilities/BlockUtilities.h"
#include "bcos-ledger/libledger/utilities/Common.h"
#include <bcos-framework/interfaces/protocol/CommonError.h>

using namespace bcos;
using namespace bcos::protocol;
using namespace bcos::storage;
using namespace bcos::consensus;

namespace bcos::ledger
{
void TransactionStorage::getTxs(const BlockNumber& _blockNumber,
    const TableFactoryInterface::Ptr& _tableFactory,
    std::function<void(Error::Ptr, bcos::protocol::TransactionsPtr)> _onGetTxs)
{
    auto cachedTransactions = m_transactionsCache.get(_blockNumber);
    if (cachedTransactions.second)
    {
        LEDGER_LOG(TRACE) << LOG_BADGE("getTxs") << LOG_DESC("CacheTxs hit, read from cache")
                          << LOG_KV("blockNumber", _blockNumber);
        _onGetTxs(nullptr, cachedTransactions.second);
        return;
    }
    LEDGER_LOG(TRACE) << LOG_BADGE("getTxs") << LOG_DESC("Cache missed, read from storage")
                      << LOG_KV("blockNumber", _blockNumber);

    auto table = _tableFactory->openTable(SYS_NUMBER_2_TXS);
    if (!table)
    {
        LEDGER_LOG(DEBUG) << LOG_DESC("Open table error from db")
                          << LOG_KV("openTable", SYS_NUMBER_2_TXS);
        // TODO: add error code and msg
        auto error = std::make_shared<Error>(-1, "");
        _onGetTxs(error, nullptr);
        return;
    }
    table->asyncGetRow(boost::lexical_cast<std::string>(_blockNumber),
        [_onGetTxs, _blockNumber, _tableFactory, this](
            const Error::Ptr& _error, Entry::Ptr _blockEntry) {
            if (_error && _error->errorCode() != CommonError::SUCCESS)
            {
                auto error = std::make_shared<Error>(
                    _error->errorCode(), "asyncGetRow callback error" + _error->errorMessage());
                _onGetTxs(error, nullptr);
                return;
            }
            if (!_blockEntry)
            {
                LEDGER_LOG(ERROR) << LOG_BADGE("getTxs")
                                  << LOG_DESC("Get txHashList from storage callback null entry")
                                  << LOG_KV("blockNumber", _blockNumber);
                _onGetTxs(nullptr, nullptr);
                return;
            }
            auto block = decodeBlock(m_blockFactory, _blockEntry->getField(SYS_VALUE));
            if (!block)
            {
                LEDGER_LOG(ERROR) << LOG_BADGE("getTxs")
                                  << LOG_DESC("getTxsFromStorage get error block")
                                  << LOG_KV("blockNumber", _blockNumber);
                // TODO: add error code
                auto error = std::make_shared<Error>(-1, "getTxsFromStorage get error block");
                _onGetTxs(error, nullptr);
                return;
            }
            auto txHashList = blockTxHashListGetter(block);

            getBatchTxByHashList(txHashList, _tableFactory,
                [this, _onGetTxs, _blockNumber, txHashList](
                    Error::Ptr _error, protocol::TransactionsPtr _txs) {
                    if (_error && _error->errorCode() != CommonError::SUCCESS)
                    {
                        LEDGER_LOG(ERROR)
                            << LOG_BADGE("getTxs") << LOG_DESC("Get txs from storage error")
                            << LOG_KV("errorCode", _error->errorCode())
                            << LOG_KV("errorMsg", _error->errorMessage())
                            << LOG_KV("txsSize", _txs->size());
                        auto error = std::make_shared<Error>(_error->errorCode(),
                            "getBatchTxByHashList callback error" + _error->errorMessage());
                        _onGetTxs(error, nullptr);
                        return;
                    }
                    LEDGER_LOG(TRACE) << LOG_BADGE("getTxs") << LOG_DESC("Get txs from storage");
                    if (_txs && !_txs->empty())
                    {
                        LEDGER_LOG(TRACE) << LOG_BADGE("getTxs") << LOG_DESC("Write to cache");
                        m_transactionsCache.add(_blockNumber, _txs);
                    }
                    _onGetTxs(nullptr, _txs);
                });
        });
}

void TransactionStorage::getReceipts(const BlockNumber& _blockNumber,
    const TableFactoryInterface::Ptr& _tableFactory,
    std::function<void(Error::Ptr, bcos::protocol::ReceiptsPtr)> _onGetReceipts)
{
    auto cachedReceipts = m_receiptCache.get(_blockNumber);
    if (cachedReceipts.second)
    {
        LEDGER_LOG(TRACE) << LOG_BADGE("getReceipts")
                          << LOG_DESC("Cache Receipts hit, read from cache")
                          << LOG_KV("blockNumber", _blockNumber);
        _onGetReceipts(nullptr, cachedReceipts.second);
        return;
    }
    LEDGER_LOG(TRACE) << LOG_BADGE("getReceipts") << LOG_DESC("Cache missed, read from storage")
                      << LOG_KV("blockNumber", _blockNumber);

    auto table = _tableFactory->openTable(SYS_NUMBER_2_TXS);
    if (!table)
    {
        LEDGER_LOG(DEBUG) << LOG_DESC("Open table error from db")
                          << LOG_KV("openTable", SYS_NUMBER_2_TXS);
        // TODO: add error code and msg
        auto error = std::make_shared<Error>(-1, "");
        _onGetReceipts(error, nullptr);
        return;
    }
    table->asyncGetRow(boost::lexical_cast<std::string>(_blockNumber),
        [_onGetReceipts, _blockNumber, _tableFactory, this](
            const Error::Ptr& _error, Entry::Ptr _blockEntry) {
            if (_error && _error->errorCode() != CommonError::SUCCESS)
            {
                auto error = std::make_shared<Error>(
                    _error->errorCode(), "asyncGetRow callback error" + _error->errorMessage());
                _onGetReceipts(error, nullptr);
                return;
            }
            if (!_blockEntry)
            {
                _onGetReceipts(nullptr, nullptr);
                return;
            }
            auto block = decodeBlock(m_blockFactory, _blockEntry->getField(SYS_VALUE));
            if (!block)
            {
                LEDGER_LOG(ERROR) << LOG_BADGE("getReceipts")
                                  << LOG_DESC("getTxsFromStorage get txHashList error")
                                  << LOG_KV("blockNumber", _blockNumber);
                // TODO: add error code
                auto error = std::make_shared<Error>(-1, "getTxsFromStorage get empty block");
                _onGetReceipts(error, nullptr);
                return;
            }
            auto txHashList = blockTxHashListGetter(block);
            getBatchReceiptsByHashList(
                txHashList, _tableFactory, [=](Error::Ptr _error, ReceiptsPtr _receipts) {
                    if (_error && _error->errorCode() != CommonError::SUCCESS)
                    {
                        LEDGER_LOG(ERROR) << LOG_BADGE("getReceipts")
                                          << LOG_DESC("Get receipts from storage error")
                                          << LOG_KV("errorCode", _error->errorCode())
                                          << LOG_KV("errorMsg", _error->errorMessage())
                                          << LOG_KV("blockNumber", _blockNumber);
                        auto error = std::make_shared<Error>(_error->errorCode(),
                            "getBatchReceiptsByHashList callback error" + _error->errorMessage());
                        _onGetReceipts(error, nullptr);
                        return;
                    }
                    LEDGER_LOG(TRACE)
                        << LOG_BADGE("getReceipts") << LOG_DESC("Get receipts from storage");
                    if (_receipts && _receipts->size() > 0)
                        m_receiptCache.add(_blockNumber, _receipts);
                    _onGetReceipts(nullptr, _receipts);
                });
        });
}

void TransactionStorage::getTxProof(const crypto::HashType& _txHash,
    const bcos::storage::TableFactoryInterface::Ptr& _tableFactory,
    std::function<void(Error::Ptr, MerkleProofPtr)> _onGetProof)
{
    // get receipt to get block number
    getReceiptByTxHash(_txHash.hex(), _tableFactory,
        [_onGetProof, _txHash, _tableFactory, this](
            const Error::Ptr& _error, Entry::Ptr _receiptEntry) {
            if (_error && _error->errorCode() != CommonError::SUCCESS)
            {
                auto error = std::make_shared<Error>(
                    _error->errorCode(), "asyncGetRow callback error" + _error->errorMessage());
                _onGetProof(error, nullptr);
                return;
            }
            if (!_receiptEntry)
            {
                LEDGER_LOG(ERROR) << LOG_BADGE("getTxProof")
                                  << LOG_DESC("getReceiptByTxHash from storage callback null entry")
                                  << LOG_KV("txHash", _txHash.hex());
                _onGetProof(nullptr, nullptr);
                return;
            }
            auto receipt = decodeReceipt(getReceiptFactory(), _receiptEntry->getField(SYS_VALUE));
            if (!receipt)
            {
                LEDGER_LOG(TRACE) << LOG_BADGE("getTxProof") << LOG_DESC("receipt is null or empty")
                                  << LOG_KV("txHash", _txHash.hex());
                // TODO: add error code
                auto error =
                    std::make_shared<Error>(-1, "getReceiptByTxHash callback empty receipt");
                _onGetProof(error, nullptr);
                return;
            }
            auto blockNumber = receipt->blockNumber();
            getTxs(blockNumber, _tableFactory,
                [this, blockNumber, _onGetProof, _txHash](Error::Ptr _error, TransactionsPtr _txs) {
                    if (_error && _error->errorCode() != CommonError::SUCCESS)
                    {
                        // TODO: add error msg
                        LEDGER_LOG(ERROR)
                            << LOG_BADGE("getTxProof") << LOG_DESC("getTxs callback error")
                            << LOG_KV("errorCode", _error->errorCode())
                            << LOG_KV("errorMsg", _error->errorMessage());
                        auto error = std::make_shared<Error>(
                            _error->errorCode(), "getTxs callback error" + _error->errorMessage());
                        _onGetProof(error, nullptr);
                        return;
                    }
                    if (!_txs)
                    {
                        LEDGER_LOG(ERROR) << LOG_BADGE("getTxProof") << LOG_DESC("get txs error")
                                          << LOG_KV("blockNumber", blockNumber)
                                          << LOG_KV("txHash", _txHash.hex());
                        // TODO: add error code
                        auto error = std::make_shared<Error>(-1, "getTxs callback empty txs");
                        _onGetProof(error, nullptr);
                        return;
                    }
                    auto merkleProofPtr = std::make_shared<MerkleProof>();
                    auto parent2ChildList =
                        m_merkleProofUtility->getParent2ChildListByTxsProofCache(
                            blockNumber, _txs, m_blockFactory->cryptoSuite());
                    auto child2Parent = m_merkleProofUtility->getChild2ParentCacheByTransaction(
                        parent2ChildList, blockNumber);
                    m_merkleProofUtility->getMerkleProof(
                        _txHash, *parent2ChildList, *child2Parent, *merkleProofPtr);
                    LEDGER_LOG(TRACE)
                        << LOG_BADGE("getTxProof") << LOG_DESC("get merkle proof success")
                        << LOG_KV("blockNumber", blockNumber) << LOG_KV("txHash", _txHash.hex());
                    _onGetProof(nullptr, merkleProofPtr);
                });
        });
}

void TransactionStorage::getReceiptProof(protocol::TransactionReceipt::Ptr _receipt,
    const bcos::storage::TableFactoryInterface::Ptr& _tableFactory,
    std::function<void(Error::Ptr, MerkleProofPtr)> _onGetProof)
{
    auto table = _tableFactory->openTable(SYS_NUMBER_2_TXS);
    if (!table)
    {
        LEDGER_LOG(DEBUG) << LOG_DESC("Open table error from db")
                          << LOG_KV("openTable", SYS_NUMBER_2_TXS);
        // TODO: add error code and msg
        auto error = std::make_shared<Error>(-1, "");
        _onGetProof(error, nullptr);
        return;
    }
    table->asyncGetRow(boost::lexical_cast<std::string>(_receipt->blockNumber()),
        [this, _onGetProof, _receipt, _tableFactory](
            const Error::Ptr& _error, Entry::Ptr _blockEntry) {
            if (_error && _error->errorCode() != CommonError::SUCCESS)
            {
                auto error = std::make_shared<Error>(
                    _error->errorCode(), "asyncGetRow callback error" + _error->errorMessage());
                _onGetProof(error, nullptr);
                return;
            }
            if (!_blockEntry)
            {
                LEDGER_LOG(ERROR) << LOG_BADGE("getReceiptProof")
                                  << LOG_DESC("getTxsFromStorage callback null entry")
                                  << LOG_KV("blockNumber", _receipt->blockNumber());
                _onGetProof(nullptr, nullptr);
                return;
            }
            auto block = decodeBlock(m_blockFactory, _blockEntry->getField(SYS_VALUE));
            if (!block)
            {
                // TODO: add error code
                LEDGER_LOG(ERROR) << LOG_BADGE("getReceiptProof")
                                  << LOG_DESC("getTxsFromStorage callback empty block txs");
                auto error = std::make_shared<Error>(-1, "empty txs");
                _onGetProof(error, nullptr);
                return;
            }
            auto txHashList = blockTxHashListGetter(block);
            getBatchReceiptsByHashList(txHashList, _tableFactory,
                [this, _onGetProof, _receipt](Error::Ptr _error, ReceiptsPtr receipts) {
                    if (_error && _error->errorCode() != CommonError::SUCCESS)
                    {
                        LEDGER_LOG(ERROR) << LOG_BADGE("getReceiptProof")
                                          << LOG_DESC("getBatchReceiptsByHashList callback error")
                                          << LOG_KV("errorCode", _error->errorCode())
                                          << LOG_KV("errorMsg", _error->errorMessage());
                        // TODO: add error code and message
                        auto error = std::make_shared<Error>(_error->errorCode(),
                            "getBatchReceiptsByHashList callback error" + _error->errorMessage());
                        _onGetProof(error, nullptr);
                        return;
                    }
                    if (!receipts || receipts->empty())
                    {
                        // TODO: add error code
                        LEDGER_LOG(ERROR) << LOG_BADGE("getReceiptProof")
                                          << LOG_DESC(
                                                 "getBatchReceiptsByHashList callback empty "
                                                 "receipts");
                        auto error = std::make_shared<Error>(-1, "empty receipts");
                        _onGetProof(error, nullptr);
                        return;
                    }
                    auto merkleProof = std::make_shared<MerkleProof>();
                    auto parent2ChildList =
                        m_merkleProofUtility->getParent2ChildListByReceiptProofCache(
                            _receipt->blockNumber(), receipts, m_blockFactory->cryptoSuite());
                    auto child2Parent = m_merkleProofUtility->getChild2ParentCacheByReceipt(
                        parent2ChildList, _receipt->blockNumber());
                    m_merkleProofUtility->getMerkleProof(
                        _receipt->hash(), *parent2ChildList, *child2Parent, *merkleProof);
                    LEDGER_LOG(INFO)
                        << LOG_BADGE("getReceiptProof") << LOG_DESC("call back receipt and proof");
                    _onGetProof(nullptr, merkleProof);
                });
        });
}

void TransactionStorage::getBatchTxByHashList(std::shared_ptr<std::vector<std::string>> _hashList,
    const TableFactoryInterface::Ptr& _tableFactory,
    std::function<void(Error::Ptr, protocol::TransactionsPtr)> _onGetTx)
{
    auto table = _tableFactory->openTable(SYS_HASH_2_TX);

    if (!table)
    {
        LEDGER_LOG(DEBUG) << LOG_DESC("Open SYS_HASH_2_TX table error from db");
        auto error = std::make_shared<Error>(-1, "open table SYS_HASH_2_TX error");
        _onGetTx(error, nullptr);
        return;
    }
    table->asyncGetRows(_hashList, [_onGetTx, _hashList, this](const Error::Ptr& _error,
                                       const std::map<std::string, Entry::Ptr>& _hashEntryMap) {
        if (_error && _error->errorCode() != CommonError::SUCCESS)
        {
            LEDGER_LOG(DEBUG) << LOG_DESC("Open SYS_HASH_2_TX table error from db");
            auto error = std::make_shared<Error>(_error->errorCode(), _error->errorMessage());
            _onGetTx(error, nullptr);
            return;
        }
        auto txList = std::make_shared<Transactions>();
        if (_hashEntryMap.empty())
        {
            _onGetTx(nullptr, txList);
            return;
        }
        // get tx list in hashList sequence
        for (const auto& hash : *_hashList)
        {
            try
            {
                auto entry = _hashEntryMap.at(hash);
                if (!entry)
                {
                    continue;
                }
                auto tx = decodeTransaction(
                    m_blockFactory->transactionFactory(), entry->getField(SYS_VALUE));
                if (tx)
                    txList->emplace_back(tx);
            }
            catch (std::out_of_range const& e)
            {
                continue;
            }
        }
        _onGetTx(nullptr, txList);
    });
}

void TransactionStorage::getReceiptByTxHash(std::string _txHash,
    const bcos::storage::TableFactoryInterface::Ptr& _tableFactory,
    std::function<void(Error::Ptr, bcos::storage::Entry::Ptr)> _onGetReceipt)
{
    StorageUtilities::asyncTableGetter(_tableFactory, SYS_HASH_2_RECEIPT, _txHash, _onGetReceipt);
}

void TransactionStorage::getBatchReceiptsByHashList(
    std::shared_ptr<std::vector<std::string>> _hashList,
    const TableFactoryInterface::Ptr& _tableFactory,
    std::function<void(Error::Ptr, protocol::ReceiptsPtr)> _onGetReceipt)

{
    auto table = _tableFactory->openTable(SYS_HASH_2_RECEIPT);

    if (!table)
    {
        LEDGER_LOG(DEBUG) << LOG_DESC("Open SYS_HASH_2_RECEIPT table error from db");
        // TODO: add error code
        auto error = std::make_shared<Error>(-1, "open table SYS_HASH_2_RECEIPT error");
        _onGetReceipt(error, nullptr);
        return;
    }
    table->asyncGetRows(_hashList, [_onGetReceipt, _hashList, this](const Error::Ptr& _error,
                                       const std::map<std::string, Entry::Ptr>& _hashEntryMap) {
        if (_error && _error->errorCode() != CommonError::SUCCESS)
        {
            auto error = std::make_shared<Error>(_error->errorCode(), _error->errorMessage());
            _onGetReceipt(error, nullptr);
            return;
        }
        auto receiptList = std::make_shared<Receipts>();
        if (_hashEntryMap.empty())
        {
            _onGetReceipt(nullptr, receiptList);
            return;
        }
        for (const auto& hash : *_hashList)
        {
            try
            {
                auto entry = _hashEntryMap.at(hash);
                if (!entry)
                {
                    continue;
                }
                auto receipt =
                    decodeReceipt(m_blockFactory->receiptFactory(), entry->getField(SYS_VALUE));
                if (receipt)
                    receiptList->emplace_back(receipt);
            }
            catch (std::out_of_range const& e)
            {
                continue;
            }
        }
        _onGetReceipt(nullptr, receiptList);
    });
}

void TransactionStorage::setNumberToTransactions(
    const Block::Ptr& _block, const TableFactoryInterface::Ptr& _tableFactory)
{
    if (_block->transactionsSize() == 0)
    {
        LEDGER_LOG(TRACE) << LOG_BADGE("WriteNumber2Txs") << LOG_DESC("empty txs in block")
                          << LOG_KV("blockNumber", _block->blockHeader()->number());
        return;
    }
    auto encodeBlock = std::make_shared<bytes>();
    auto emptyBlock = m_blockFactory->createBlock();
    auto number = _block->blockHeader()->number();
    for (size_t i = 0; i < _block->transactionsSize(); i++)
    {
        emptyBlock->appendTransactionHash(_block->transactionHash(i));
    }

    emptyBlock->encode(*encodeBlock);

    StorageUtilities::syncTableSetter(_tableFactory, SYS_NUMBER_2_TXS,
        boost::lexical_cast<std::string>(number), SYS_VALUE, asString(*encodeBlock));
}

void TransactionStorage::setBatchHashToReceipt(
    const Block::Ptr& _block, const TableFactoryInterface::Ptr& _tableFactory)
{
    tbb::parallel_for(tbb::blocked_range<size_t>(0, _block->transactionsHashSize()),
        [&](const tbb::blocked_range<size_t>& range) {
            for (size_t i = range.begin(); i < range.end(); ++i)
            {
                auto encodeReceipt = _block->receipt(i)->encode();
                setHashToReceipt(
                    _block->transactionHash(i).hex(), encodeReceipt.toString(), _tableFactory);
            }
        });
}

bool TransactionStorage::setHashToTx(const std::string& _txHash, const std::string& _encodeTx,
    const bcos::storage::TableFactoryInterface::Ptr& _tableFactory)
{
    return StorageUtilities::syncTableSetter(
        _tableFactory, SYS_HASH_2_TX, _txHash, SYS_VALUE, _encodeTx);
}

bool TransactionStorage::setHashToReceipt(const std::string& _txHash,
    const std::string& _encodeReceipt,
    const bcos::storage::TableFactoryInterface::Ptr& _tableFactory)
{
    return StorageUtilities::syncTableSetter(
        _tableFactory, SYS_HASH_2_RECEIPT, _txHash, SYS_VALUE, _encodeReceipt);
}
}  // namespace bcos::ledger
