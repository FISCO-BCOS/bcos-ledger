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
 * @file Ledger.cpp
 * @author: kyonRay
 * @date 2021-04-13
 * @file Ledger.cpp
 * @author: ancelmo
 * @date 2021-09-06
 */

#include "Ledger.h"
#include "bcos-framework/interfaces/consensus/ConsensusNode.h"
#include "interfaces/crypto/CommonType.h"
#include "interfaces/ledger/LedgerTypeDef.h"
#include "interfaces/protocol/ProtocolTypeDef.h"
#include "interfaces/protocol/TransactionMetaData.h"
#include "libutilities/BoostLog.h"
#include "libutilities/Common.h"
#include "libutilities/DataConvertUtility.h"
#include <bcos-framework/interfaces/protocol/CommonError.h>
#include <bcos-framework/interfaces/storage/Table.h>
#include <bcos-framework/libprotocol/ParallelMerkleProof.h>
#include <tbb/parallel_for.h>
#include <tbb/parallel_invoke.h>
#include <boost/exception/diagnostic_information.hpp>
#include <boost/lexical_cast.hpp>
#include <boost/throw_exception.hpp>
#include <future>
#include <memory>

using namespace bcos;
using namespace bcos::ledger;
using namespace bcos::protocol;
using namespace bcos::storage;
using namespace bcos::crypto;

void Ledger::asyncPrewriteBlock(bcos::storage::StorageInterface::Ptr storage,
    bcos::protocol::Block::ConstPtr block, std::function<void(Error::Ptr&&)> callback)
{
    if (!block)
    {
        callback(
            BCOS_ERROR_PTR(LedgerError::ErrorArgument, "asyncPrewriteBlock failed, empty block"));
        return;
    }

    auto mutableBlock = std::const_pointer_cast<bcos::protocol::Block>(block);
    auto header = mutableBlock->blockHeader();

    auto blockNumberStr = boost::lexical_cast<std::string>(header->number());

    // 8 storage callbacks and write hash=>receipt
    size_t TOTAL_CALLBACK = 8 + mutableBlock->receiptsSize();
    auto setRowCallback = [total = std::make_shared<std::atomic<size_t>>(TOTAL_CALLBACK),
                              failed = std::make_shared<bool>(false),
                              callback = std::move(callback)](
                              Error::UniquePtr&& error, size_t count = 1) {
        *total -= count;
        if (error)
        {
            LEDGER_LOG(ERROR) << "Prewrite block error!" << boost::diagnostic_information(*error);
            *failed = true;
        }

        if (*total == 0)
        {
            // all finished
            if (*failed)
            {
                LEDGER_LOG(ERROR) << "PrewriteBlock error";
                callback(
                    BCOS_ERROR_PTR(LedgerError::CollectAsyncCallbackError, "PrewriteBlock error"));
                return;
            }

            LEDGER_LOG(INFO) << "PrewriteBlock success";
            callback(nullptr);
        }
    };

    // number 2 entry
    Entry numberEntry;
    numberEntry.importFields({blockNumberStr});
    storage->asyncSetRow(SYS_CURRENT_STATE, SYS_KEY_CURRENT_NUMBER, std::move(numberEntry),
        [setRowCallback](auto&& error) { setRowCallback(std::move(error)); });

    // number 2 hash
    Entry hashEntry;
    hashEntry.importFields({header->hash().hex()});
    storage->asyncSetRow(SYS_NUMBER_2_HASH, blockNumberStr, std::move(hashEntry),
        [setRowCallback](auto&& error) { setRowCallback(std::move(error)); });

    // hash 2 number
    Entry hash2NumberEntry;
    hash2NumberEntry.importFields({blockNumberStr});
    storage->asyncSetRow(SYS_HASH_2_NUMBER, header->hash().hex(), std::move(hash2NumberEntry),
        [setRowCallback](auto&& error) { setRowCallback(std::move(error)); });

    // number 2 header
    bytes headerBuffer;
    header->encode(headerBuffer);

    Entry number2HeaderEntry;
    number2HeaderEntry.importFields({std::move(headerBuffer)});
    storage->asyncSetRow(SYS_NUMBER_2_BLOCK_HEADER, blockNumberStr, std::move(number2HeaderEntry),
        [setRowCallback](auto&& error) { setRowCallback(std::move(error)); });

    // number 2 nonce
    auto nonceBlock = m_blockFactory->createBlock();
    nonceBlock->setNonceList(mutableBlock->nonceList());
    bytes nonceBuffer;
    nonceBlock->encode(nonceBuffer);

    Entry number2NonceEntry;
    number2NonceEntry.importFields({std::move(nonceBuffer)});
    storage->asyncSetRow(SYS_BLOCK_NUMBER_2_NONCES, blockNumberStr, std::move(number2NonceEntry),
        [setRowCallback](auto&& error) { setRowCallback(std::move(error)); });

    // number 2 transactions
    auto transactionsBlock = m_blockFactory->createBlock();
    for (size_t i = 0; i < block->transactionsHashSize(); ++i)
    {
        auto originTransactionMetaData = block->transactionMetaData(i);
        auto transactionMetaData = m_blockFactory->createTransactionMetaData(
            originTransactionMetaData->hash(), std::string(originTransactionMetaData->to()));
        transactionMetaData->setSource("");
        transactionsBlock->appendTransactionMetaData(transactionMetaData);
    }
    bytes transactionsBuffer;
    transactionsBlock->encode(transactionsBuffer);

    Entry number2TransactionHashesEntry;
    number2TransactionHashesEntry.importFields({std::move(transactionsBuffer)});
    storage->asyncSetRow(SYS_NUMBER_2_TXS, blockNumberStr, std::move(number2TransactionHashesEntry),
        [setRowCallback](auto&& error) { setRowCallback(std::move(error)); });

    // hash 2 receipts
    bytes receiptBuffer;
    int64_t totalCount = 0;
    int64_t failedCount = 0;
    for (size_t i = 0; i < mutableBlock->receiptsSize(); ++i)
    {
        auto hash = mutableBlock->transactionHash(i);
        auto receipt = mutableBlock->receipt(i);
        if (receipt->status() != 0)
        {
            failedCount++;
        }
        totalCount++;

        receiptBuffer.clear();
        receipt->encode(receiptBuffer);

        Entry receiptEntry;
        receiptEntry.importFields({std::move(receiptBuffer)});
        storage->asyncSetRow(SYS_HASH_2_RECEIPT, hash.hex(), std::move(receiptEntry),
            [setRowCallback](auto&& error) { setRowCallback(std::move(error)); });
    }

    // total transaction count
    asyncGetTotalTransactionCount(
        [storage, mutableBlock, setRowCallback, totalCount = std::move(totalCount),
            failedCount = std::move(failedCount)](
            Error::Ptr error, int64_t total, int64_t failed, bcos::protocol::BlockNumber) {
            if (error)
            {
                LEDGER_LOG(INFO) << "No total transaction count entry, add new one";
                setRowCallback(std::make_unique<Error>(*error), 2);
                return;
            }

            Entry totalEntry;
            totalEntry.importFields({boost::lexical_cast<std::string>(total + totalCount)});
            storage->asyncSetRow(SYS_CURRENT_STATE, SYS_KEY_TOTAL_TRANSACTION_COUNT,
                std::move(totalEntry),
                [setRowCallback](auto&& error) { setRowCallback(std::move(error)); });

            if (failedCount != 0)
            {
                Entry failedEntry;
                failedEntry.importFields({boost::lexical_cast<std::string>(failed + failedCount)});
                storage->asyncSetRow(SYS_CURRENT_STATE, SYS_KEY_TOTAL_TRANSACTION_COUNT,
                    std::move(failedEntry),
                    [setRowCallback](auto&& error) { setRowCallback(std::move(error)); });
            }
            else
            {
                setRowCallback({}, true);
            }
        });
}

void Ledger::asyncStoreTransactions(std::shared_ptr<std::vector<bytesConstPtr>> _txToStore,
    crypto::HashListPtr _txHashList, std::function<void(Error::Ptr)> _onTxStored)
{
    if (!_txToStore || !_txHashList || _txToStore->size() != _txHashList->size())
    {
        LEDGER_LOG(ERROR) << "StoreTransactions error";
        _onTxStored(
            BCOS_ERROR_PTR(LedgerError::ErrorArgument, "asyncStoreTransactions argument error!"));
        return;
    }

    LEDGER_LOG(TRACE) << "StoreTransactions request" << LOG_KV("tx count", _txToStore->size())
                      << LOG_KV("hash count", _txHashList->size());

    m_storage->asyncOpenTable(SYS_HASH_2_TX,
        [this, storage = m_storage, hashList = std::move(_txHashList),
            txList = std::move(_txToStore),
            callback = std::move(_onTxStored)](auto&& error, std::optional<Table>&& table) {
            auto validError = checkTableValid(std::move(error), table, SYS_HASH_2_TX);
            if (validError)
            {
                LEDGER_LOG(ERROR) << "StoreTransactions error"
                                  << boost::diagnostic_information(validError);
                callback(std::move(validError));
                return;
            }

            auto total = txList->size();
            auto count =
                std::make_shared<std::tuple<std::atomic<size_t>, std::atomic<size_t>>>(0, 0);
            for (size_t i = 0; i < txList->size(); ++i)
            {
                auto entry = table->newEntry();
                entry.setField(SYS_VALUE, *((*txList)[i]));  // copy the bytes entry

                LEDGER_LOG(TRACE) << "Write transaction" << LOG_KV("hash", (*hashList)[i].hex());
                table->asyncSetRow(
                    (*hashList)[i].hex(), std::move(entry), [total, count, callback](auto&& error) {
                        if (error)
                        {
                            ++std::get<1>(*count);
                            LEDGER_LOG(ERROR) << "Set row failed!" << error->what();
                        }
                        else
                        {
                            ++std::get<0>(*count);
                        }

                        if (std::get<0>(*count) + std::get<1>(*count) == total)
                        {
                            // All finished
                            LEDGER_LOG(TRACE) << "StoreTransactions success";
                            callback(nullptr);
                        }
                    });
            }
        });
}

void Ledger::asyncGetBlockDataByNumber(bcos::protocol::BlockNumber _blockNumber, int32_t _blockFlag,
    std::function<void(Error::Ptr, bcos::protocol::Block::Ptr)> _onGetBlock)
{
    LEDGER_LOG(INFO) << "GetBlockDataByNumber request" << LOG_KV("blockNumber", _blockNumber)
                     << LOG_KV("blockFlag", _blockFlag);
    if (_blockNumber < 0 || _blockFlag < 0)
    {
        LEDGER_LOG(INFO) << "GetBlockDataByNumber error, wrong argument";
        _onGetBlock(BCOS_ERROR_PTR(LedgerError::ErrorArgument, "Wrong argument"), nullptr);
        return;
    }

    std::list<std::function<void()>> fetchers;
    auto block = m_blockFactory->createBlock();
    auto total = std::make_shared<size_t>(0);
    auto result = std::make_shared<std::tuple<std::atomic<size_t>, std::atomic<size_t>>>(0, 0);

    auto finally = [total, result, block, _onGetBlock](Error::Ptr&& error) {
        if (error)
            ++std::get<1>(*result);
        else
            ++std::get<0>(*result);

        if (std::get<0>(*result) + std::get<1>(*result) == *total)
        {
            // All finished
            if (std::get<0>(*result) != *total)
            {
                LEDGER_LOG(ERROR) << "GetBlockDataByNumber request error, with errors!";
                _onGetBlock(BCOS_ERROR_PTR(LedgerError::CollectAsyncCallbackError,
                                "Get block failed with errors!"),
                    nullptr);
                return;
            }

            LEDGER_LOG(INFO) << "GetBlockDataByNumber success";
            _onGetBlock(nullptr, std::move(block));
        }
    };

    if (_blockFlag & HEADER)
    {
        ++(*total);

        fetchers.push_back([this, _blockNumber, block, finally]() {
            asyncGetBlockHeader(
                block, _blockNumber, [finally](Error::Ptr&& error) { finally(std::move(error)); });
        });
    }
    if (_blockFlag & TRANSACTIONS)
        ++(*total);
    if (_blockFlag & RECEIPTS)
        ++(*total);

    if ((_blockFlag & TRANSACTIONS) || (_blockFlag & RECEIPTS))
    {
        fetchers.push_back([this, block, _blockNumber, finally, _blockFlag]() {
            asyncGetBlockTransactionHashes(
                _blockNumber, [this, _blockFlag, block, finally](
                                  Error::Ptr&& error, std::vector<std::string>&& hashes) {
                    if (error)
                    {
                        if (_blockFlag & TRANSACTIONS)
                            finally(std::move(error));
                        if (_blockFlag & RECEIPTS)
                            finally(std::move(error));
                        return;
                    }

                    auto hashesPtr = std::make_shared<std::vector<std::string>>(std::move(hashes));
                    if (_blockFlag & TRANSACTIONS)
                    {
                        asyncBatchGetTransactions(
                            hashesPtr, [block, finally](Error::Ptr&& error,
                                           std::vector<protocol::Transaction::Ptr>&& transactions) {
                                for (auto& it : transactions)
                                {
                                    block->appendTransaction(it);
                                }
                                finally(std::move(error));
                            });
                    }
                    if (_blockFlag & RECEIPTS)
                    {
                        asyncBatchGetReceipts(hashesPtr,
                            [block, finally](Error::Ptr&& error,
                                std::vector<protocol::TransactionReceipt::Ptr>&& receipts) {
                                for (auto& it : receipts)
                                {
                                    block->appendReceipt(it);
                                }
                                finally(std::move(error));
                            });
                    }
                });
        });
    }

    for (auto& it : fetchers)
    {
        it();
    }
}

void Ledger::asyncGetBlockNumber(
    std::function<void(Error::Ptr, bcos::protocol::BlockNumber)> _onGetBlock)
{
    LEDGER_LOG(INFO) << "GetBlockNumber request";
    asyncGetSystemTableEntry(SYS_CURRENT_STATE, SYS_KEY_CURRENT_NUMBER,
        [callback = std::move(_onGetBlock)](
            Error::Ptr&& error, std::optional<bcos::storage::Entry>&& entry) {
            if (error)
            {
                LEDGER_LOG(ERROR) << "GetBlockNumber error" << boost::diagnostic_information(error);
                callback(BCOS_ERROR_WITH_PREV_PTR(LedgerError::GetStorageError,
                             "Get block number storage error", *error),
                    -1);
                return;
            }

            bcos::protocol::BlockNumber blockNumber = -1;
            try
            {
                blockNumber =
                    boost::lexical_cast<bcos::protocol::BlockNumber>(entry->getField(SYS_VALUE));
            }
            catch (boost::bad_lexical_cast& e)
            {
                // Ignore the exception
                LEDGER_LOG(INFO) << "Cast blocknumber failed, may be empty, set to default value -1"
                                 << LOG_KV("blocknumber str", entry->getField(SYS_VALUE));
            }

            LEDGER_LOG(INFO) << "GetBlockNumber success" << LOG_KV("number", blockNumber);
            callback(nullptr, blockNumber);
        });
}

void Ledger::asyncGetBlockHashByNumber(bcos::protocol::BlockNumber _blockNumber,
    std::function<void(Error::Ptr, const bcos::crypto::HashType&)> _onGetBlock)
{
    LEDGER_LOG(INFO) << "GetBlockHashByNumber request" << LOG_KV("blockNumber", _blockNumber);
    if (_blockNumber < 0)
    {
        _onGetBlock(BCOS_ERROR_PTR(
                        LedgerError::ErrorArgument, "GetBlockHashByNumber error, error argument"),
            bcos::crypto::HashType());
        return;
    }

    auto key = boost::lexical_cast<std::string>(_blockNumber);
    asyncGetSystemTableEntry(SYS_NUMBER_2_HASH, key,
        [callback = std::move(_onGetBlock)](
            Error::Ptr&& error, std::optional<bcos::storage::Entry>&& entry) {
            try
            {
                if (error)
                {
                    LEDGER_LOG(ERROR)
                        << "GetBlockHashByNumber error" << boost::diagnostic_information(error);
                    callback(BCOS_ERROR_WITH_PREV_PTR(LedgerError::GetStorageError,
                                 "GetBlockHashByNumber error", *error),
                        bcos::crypto::HashType());
                    return;
                }

                auto hashStr = entry->getField(SYS_VALUE);
                bcos::crypto::HashType hash(std::string(hashStr), bcos::crypto::HashType::FromHex);

                LEDGER_LOG(INFO) << "GetBlockHashByNumber success" << LOG_KV("hash", hashStr);
                callback(nullptr, std::move(hash));
            }
            catch (std::exception& e)
            {
                callback(BCOS_ERROR_WITH_PREV_PTR(LedgerError::UnknownError, "Unknown error", e),
                    bcos::crypto::HashType());
                return;
            }
        });
}

void Ledger::asyncGetBlockNumberByHash(const crypto::HashType& _blockHash,
    std::function<void(Error::Ptr, bcos::protocol::BlockNumber)> _onGetBlock)
{
    auto key = _blockHash.hex();
    LEDGER_LOG(INFO) << "GetBlockNumberByHash request" << LOG_KV("hash", key);

    asyncGetSystemTableEntry(SYS_HASH_2_NUMBER, key,
        [callback = std::move(_onGetBlock)](
            Error::Ptr&& error, std::optional<bcos::storage::Entry>&& entry) {
            try
            {
                if (error)
                {
                    LEDGER_LOG(ERROR)
                        << "GetBlockNumberByHash error " << boost::diagnostic_information(*error);
                    callback(BCOS_ERROR_WITH_PREV_PTR(LedgerError::GetStorageError,
                                 "GetBlockHashByNumber error", *error),
                        -1);
                    return;
                }

                bcos::protocol::BlockNumber blockNumber = -1;
                try
                {
                    blockNumber = boost::lexical_cast<bcos::protocol::BlockNumber>(
                        entry->getField(SYS_VALUE));
                }
                catch (boost::bad_lexical_cast& e)
                {
                    // Ignore the exception
                    LEDGER_LOG(INFO)
                        << "Cast blocknumber failed, may be empty, set to default value -1"
                        << LOG_KV("blocknumber str", entry->getField(SYS_VALUE));
                }
                LEDGER_LOG(INFO) << "GetBlockNumberByHash success" << LOG_KV("number", blockNumber);
                callback(nullptr, blockNumber);
            }
            catch (std::exception& e)
            {
                LEDGER_LOG(INFO) << "GetBlockNumberByHash error"
                                 << boost::diagnostic_information(e);
                callback(BCOS_ERROR_WITH_PREV_PTR(LedgerError::GetStorageError,
                             "GetBlockNumberByHash error", std::move(e)),
                    -1);
            }
        });
}

void Ledger::asyncGetBatchTxsByHashList(crypto::HashListPtr _txHashList, bool _withProof,
    std::function<void(Error::Ptr, bcos::protocol::TransactionsPtr,
        std::shared_ptr<std::map<std::string, MerkleProofPtr>>)>
        _onGetTx)
{
    if (!_txHashList)
    {
        LEDGER_LOG(ERROR) << "GetBatchTxsByHashList error, wrong argument";
        _onGetTx(BCOS_ERROR_PTR(LedgerError::ErrorArgument, "Wrong argument"), nullptr, nullptr);
        return;
    }

    LEDGER_LOG(TRACE) << "GetBatchTxsByHashList request" << LOG_KV("hashes", _txHashList->size())
                     << LOG_KV("withProof", _withProof);

    auto hexList = std::make_shared<std::vector<std::string>>();
    hexList->reserve(_txHashList->size());

    for (auto& it : *_txHashList)
    {
        hexList->push_back(it.hex());
    }

    asyncBatchGetTransactions(
        hexList, [this, callback = std::move(_onGetTx), _txHashList, _withProof](
                     Error::Ptr&& error, std::vector<protocol::Transaction::Ptr>&& transactions) {
            if (error)
            {
                LEDGER_LOG(ERROR) << "GetBatchTxsByHashList error: "
                                  << boost::diagnostic_information(error);
                callback(BCOS_ERROR_WITH_PREV_PTR(
                             LedgerError::GetStorageError, "GetBatchTxsByHashList error", *error),
                    nullptr, nullptr);
                return;
            }

            bcos::protocol::TransactionsPtr results =
                std::make_shared<bcos::protocol::Transactions>(std::move(transactions));

            // if (_withProof)
            if (false)
            {
                auto con_proofMap =
                    std::make_shared<tbb::concurrent_unordered_map<std::string, MerkleProofPtr>>();
                auto count = std::make_shared<std::atomic_uint64_t>(0);
                auto counter = [_txList = results, _txHashList, count, con_proofMap,
                                   callback = std::move(callback)]() {
                    count->fetch_add(1);
                    if (count->load() == _txHashList->size())
                    {
                        auto proofMap = std::make_shared<std::map<std::string, MerkleProofPtr>>(
                            con_proofMap->begin(), con_proofMap->end());
                        LEDGER_LOG(INFO) << LOG_BADGE("GetBatchTxsByHashList success")
                                         << LOG_KV("txHashListSize", _txHashList->size())
                                         << LOG_KV("proofMapSize", proofMap->size());
                        callback(nullptr, _txList, proofMap);
                    }
                };

                tbb::parallel_for(tbb::blocked_range<size_t>(0, _txHashList->size()),
                    [this, _txHashList, counter, con_proofMap](
                        const tbb::blocked_range<size_t>& range) {
                        for (size_t i = range.begin(); i < range.end(); ++i)
                        {
                            auto txHash = _txHashList->at(i);
                            getTxProof(txHash, [con_proofMap, txHash, counter](
                                                   Error::Ptr _error, MerkleProofPtr _proof) {
                                if (!_error && _proof)
                                {
                                    con_proofMap->insert(std::make_pair(txHash.hex(), _proof));
                                }
                                counter();
                            });
                        }
                    });
            }
            else
            {
                LEDGER_LOG(TRACE) << LOG_BADGE("GetBatchTxsByHashList success")
                                 << LOG_KV("txHashListSize", _txHashList->size())
                                 << LOG_KV("withProof", _withProof);
                callback(nullptr, results, nullptr);
            }
        });
}

void Ledger::asyncGetTransactionReceiptByHash(bcos::crypto::HashType const& _txHash, bool,
    std::function<void(Error::Ptr, bcos::protocol::TransactionReceipt::ConstPtr, MerkleProofPtr)>
        _onGetTx)
{
    auto key = _txHash.hex();

    LEDGER_LOG(TRACE) << "GetTransactionReceiptByHash" << LOG_KV("hash", key);

    asyncGetSystemTableEntry(SYS_HASH_2_RECEIPT, key,
        [this, callback = std::move(_onGetTx), key](
            Error::Ptr&& error, std::optional<bcos::storage::Entry>&& entry) {
            if (error)
            {
                LEDGER_LOG(ERROR) << "GetTransactionReceiptByHash error"
                                  << boost::diagnostic_information(error);
                callback(BCOS_ERROR_WITH_PREV_PTR(
                             LedgerError::GetStorageError, "GetTransactionReceiptByHash", *error),
                    nullptr, nullptr);

                return;
            }

            auto value = entry->getField(SYS_VALUE);
            auto receipt = m_blockFactory->receiptFactory()->createReceipt(
                bcos::bytesConstRef((bcos::byte*)value.data(), value.size()));

            // if (_withProof)
            if (false)
            {
                getReceiptProof(receipt, [receipt, _onGetTx = std::move(callback)](
                                             Error::Ptr _error, MerkleProofPtr _proof) {
                    if (_error && _error->errorCode() != CommonError::SUCCESS)
                    {
                        LEDGER_LOG(ERROR) << "GetTransactionReceiptByHash error"
                                          << LOG_KV("errorCode", _error->errorCode())
                                          << LOG_KV("errorMsg", _error->errorMessage())
                                          << boost::diagnostic_information(_error);
                        _onGetTx(std::move(_error), receipt, nullptr);
                        return;
                    }

                    _onGetTx(nullptr, receipt, _proof);
                });
            }
            else
            {
                LEDGER_LOG(TRACE) << "GetTransactionReceiptByHash success" << LOG_KV("hash", key);
                callback(nullptr, receipt, nullptr);
            }
        });
}

void Ledger::asyncGetTotalTransactionCount(
    std::function<void(Error::Ptr, int64_t, int64_t, bcos::protocol::BlockNumber)> _callback)
{
    static std::string_view keys[] = {
        SYS_KEY_TOTAL_TRANSACTION_COUNT, SYS_KEY_TOTAL_FAILED_TRANSACTION, SYS_KEY_CURRENT_NUMBER};

    LEDGER_LOG(INFO) << "GetTotalTransactionCount request";
    m_storage->asyncOpenTable(SYS_CURRENT_STATE, [this, callback = std::move(_callback)](
                                                     auto&& error, std::optional<Table>&& table) {
        auto tableError = checkTableValid(std::move(error), table, SYS_CURRENT_STATE);
        if (tableError)
        {
            LEDGER_LOG(ERROR) << "GetTotalTransactionCount error"
                              << boost::diagnostic_information(*tableError);
            callback(std::move(tableError), -1, -1, -1);
            return;
        }

        table->asyncGetRows(keys, [callback = std::move(callback)](
                                      auto&& error, std::vector<std::optional<Entry>>&& entries) {
            if (error)
            {
                LEDGER_LOG(ERROR) << "GetTotalTransactionCount error"
                                  << boost::diagnostic_information(*error);
                callback(
                    BCOS_ERROR_WITH_PREV_PTR(LedgerError::GetStorageError, "Get row error", *error),
                    -1, -1, -1);
                return;
            }

            int64_t totalCount = 0, failedCount = 0, blockNumber = 0;
            size_t i = 0;
            for (auto& entry : entries)
            {
                int64_t value = 0;
                if (!entry)
                {
                    LEDGER_LOG(WARNING)
                        << "GetTotalTransactionCount error" << LOG_KV("index", i) << " empty";
                }
                else
                {
                    value = boost::lexical_cast<int64_t>(entry->getField(SYS_VALUE));
                }
                switch (i++)
                {
                case 0:
                    totalCount = value;
                    break;
                case 1:
                    failedCount = value;
                    break;
                case 2:
                    blockNumber = value;
                    break;
                }
            }

            LEDGER_LOG(INFO) << "GetTotalTransactionCount success"
                             << LOG_KV("totalCount", totalCount)
                             << LOG_KV("failedCount", failedCount)
                             << LOG_KV("blockNumber", blockNumber);
            callback(nullptr, totalCount, failedCount, blockNumber);
        });
    });
}

void Ledger::asyncGetSystemConfigByKey(const std::string& _key,
    std::function<void(Error::Ptr, std::string, bcos::protocol::BlockNumber)> _onGetConfig)
{
    LEDGER_LOG(INFO) << "GetSystemConfigByKey request" << LOG_KV("key", _key);

    asyncGetBlockNumber([this, callback = std::move(_onGetConfig), _key](
                            Error::Ptr error, bcos::protocol::BlockNumber blockNumber) {
        if (error)
        {
            LEDGER_LOG(ERROR) << "GetSystemConfigByKey error, "
                              << boost::diagnostic_information(*error);
            callback(std::move(error), "", -1);
            return;
        }

        asyncGetSystemTableEntry(SYS_CONFIG, _key,
            [blockNumber, callback = std::move(callback)](
                Error::Ptr&& error, std::optional<bcos::storage::Entry>&& entry) {
                try
                {
                    if (error)
                    {
                        LEDGER_LOG(ERROR) << "GetSystemConfigByKey error, "
                                          << boost::diagnostic_information(*error);
                        callback(std::move(error), "", -1);
                        return;
                    }

                    auto value = entry->getField(SYS_VALUE);
                    auto number = boost::lexical_cast<bcos::protocol::BlockNumber>(
                        entry->getField(SYS_CONFIG_ENABLE_BLOCK_NUMBER));

                    // The param was reset at height getLatestBlockNumber(), and takes effect in
                    // next block. So we query the status of getLatestBlockNumber() + 1.
                    auto effectNumber = blockNumber + 1;
                    if (number > effectNumber)
                    {
                        LEDGER_LOG(ERROR) << "GetSystemConfigByKey error, config not available"
                                          << LOG_KV("currentBlockNumber", effectNumber)
                                          << LOG_KV("available number", number);
                        callback(BCOS_ERROR_PTR(LedgerError::ErrorArgument, "Config not available"),
                            "", -1);
                        return;
                    }

                    LEDGER_LOG(INFO) << "GetSystemConfigByKey success" << LOG_KV("value", value)
                                     << LOG_KV("number", number);
                    callback(nullptr, std::string(value), number);
                }
                catch (std::exception& e)
                {
                    LEDGER_LOG(ERROR)
                        << "GetSystemConfigByKey error, " << boost::diagnostic_information(e);
                    callback(
                        BCOS_ERROR_WITH_PREV_PTR(LedgerError::GetStorageError, "error", e), "", -1);
                }
            });
    });
}

void Ledger::asyncGetNonceList(bcos::protocol::BlockNumber _startNumber, int64_t _offset,
    std::function<void(
        Error::Ptr, std::shared_ptr<std::map<protocol::BlockNumber, protocol::NonceListPtr>>)>
        _onGetList)
{
    LEDGER_LOG(INFO) << "GetNonceList request" << LOG_KV("startNumber", _startNumber)
                     << LOG_KV("offset", _offset);

    if (_startNumber < 0 || _offset < 0 || _startNumber > _offset)
    {
        LEDGER_LOG(ERROR) << "GetNonceList error";
        _onGetList(BCOS_ERROR_PTR(LedgerError::ErrorArgument, "Wrong argument"), nullptr);
        return;
    }

    m_storage->asyncOpenTable(SYS_BLOCK_NUMBER_2_NONCES, [this, callback = std::move(_onGetList),
                                                             _startNumber, _offset](auto&& error,
                                                             std::optional<Table>&& table) {
        auto tableError = checkTableValid(std::move(error), table, SYS_BLOCK_NUMBER_2_NONCES);
        if (tableError)
        {
            callback(std::move(tableError), nullptr);
            return;
        }

        auto numberList = std::vector<std::string>();
        for (BlockNumber i = _startNumber; i <= _startNumber + _offset; ++i)
        {
            numberList.push_back(boost::lexical_cast<std::string>(i));
        }

        table->asyncGetRows(numberList, [this, numberList, callback = std::move(callback)](
                                            auto&& error,
                                            std::vector<std::optional<Entry>>&& entries) {
            if (error)
            {
                LEDGER_LOG(ERROR) << "GetNonceList error" << boost::diagnostic_information(*error);
                callback(
                    BCOS_ERROR_WITH_PREV_PTR(LedgerError::GetStorageError, "GetNonceList", *error),
                    nullptr);
                return;
            }

            auto retMap =
                std::make_shared<std::map<protocol::BlockNumber, protocol::NonceListPtr>>();

            for (size_t i = 0; i < numberList.size(); ++i)
            {
                try
                {
                    auto number = numberList[i];
                    auto entry = entries[i];
                    if (!entry)
                    {
                        continue;
                    }

                    auto value = entry->getField(SYS_VALUE);
                    auto block = m_blockFactory->createBlock(
                        bcos::bytesConstRef((bcos::byte*)value.data(), value.size()), false, false);

                    auto nonceList = std::make_shared<protocol::NonceList>(block->nonceList());
                    retMap->emplace(
                        std::make_pair(boost::lexical_cast<BlockNumber>(number), nonceList));
                }
                catch (std::exception const& e)
                {
                    LEDGER_LOG(WARNING)
                        << "Parse nonce list error" << boost::diagnostic_information(e);
                    continue;
                }
            }

            LEDGER_LOG(INFO) << "GetNonceList success" << LOG_KV("retMap size", retMap->size());
            callback(nullptr, std::move(retMap));
        });
    });
}

void Ledger::asyncGetNodeListByType(const std::string& _type,
    std::function<void(Error::Ptr, consensus::ConsensusNodeListPtr)> _onGetConfig)
{
    LEDGER_LOG(INFO) << "GetNodeListByType request" << LOG_KV("type", _type);

    asyncGetBlockNumber([this, type = std::move(_type), callback = std::move(_onGetConfig)](
                            Error::Ptr&& error, bcos::protocol::BlockNumber blockNumber) {
        if (error)
        {
            LEDGER_LOG(ERROR) << "GetNodeListByType error" << boost::diagnostic_information(*error);
            callback(BCOS_ERROR_WITH_PREV_PTR(
                         LedgerError::GetStorageError, "GetNodeListByType error", *error),
                nullptr);
            return;
        }

        LEDGER_LOG(DEBUG) << "Get nodeList from" << LOG_KV("blockNumber", blockNumber);

        m_storage->asyncOpenTable(SYS_CONSENSUS, [this, type = std::move(type),
                                                     callback = std::move(callback), blockNumber](
                                                     auto&& error, std::optional<Table>&& table) {
            auto tableError = checkTableValid(std::move(error), table, SYS_CONSENSUS);
            if (tableError)
            {
                callback(std::move(tableError), nullptr);
                return;
            }

            auto tablePtr = std::make_shared<Table>(std::move(*table));

            tablePtr->asyncGetPrimaryKeys({}, [this, type = std::move(type),
                                                  callback = std::move(callback), tablePtr,
                                                  blockNumber](
                                                  auto&& error, std::vector<std::string>&& keys) {
                if (error)
                {
                    LEDGER_LOG(ERROR)
                        << "GetNodeListByType error" << boost::diagnostic_information(*error);
                    callback(BCOS_ERROR_WITH_PREV_PTR(
                                 LedgerError::GetStorageError, "GetNodeListByType error", *error),
                        nullptr);
                    return;
                }

                tablePtr->asyncGetRows(keys,
                    [this, callback = std::move(callback), type = std::move(type), keys,
                        blockNumber](auto&& error, std::vector<std::optional<Entry>>&& entries) {
                        if (error)
                        {
                            LEDGER_LOG(ERROR) << "GetNodeListByType error"
                                              << boost::diagnostic_information(*error);
                            callback(BCOS_ERROR_WITH_PREV_PTR(LedgerError::GetStorageError,
                                         "GetNodeListByType error", *error),
                                nullptr);
                            return;
                        }

                        auto nodes = std::make_shared<consensus::ConsensusNodeList>();
                        for (size_t i = 0; i < entries.size(); ++i)
                        {
                            auto& entry = entries[i];

                            if (!entry)
                            {
                                continue;
                            }
                            try
                            {
                                // The param was reset at height getLatestBlockNumber(), and takes
                                // effect in next block. So we query the status of
                                // getLatestBlockNumber() + 1.
                                auto effectNumber = blockNumber + 1;
                                auto nodeType = entry->getField(NODE_TYPE);
                                auto enableNum = boost::lexical_cast<BlockNumber>(
                                    entry->getField(NODE_ENABLE_NUMBER));
                                auto weight =
                                    boost::lexical_cast<uint64_t>(entry->getField(NODE_WEIGHT));
                                if ((nodeType == type) && enableNum <= effectNumber)
                                {
                                    crypto::NodeIDPtr nodeID =
                                        m_blockFactory->cryptoSuite()->keyFactory()->createKey(
                                            *fromHexString(keys[i]));
                                    // Note: use try-catch to handle the exception case
                                    nodes->emplace_back(
                                        std::make_shared<consensus::ConsensusNode>(nodeID, weight));
                                }
                            }
                            catch (std::exception& e)
                            {
                                LEDGER_LOG(WARNING)
                                    << "Exception: " << boost::diagnostic_information(e);
                                continue;
                            }
                        }

                        LEDGER_LOG(INFO)
                            << "GetNodeListByType success" << LOG_KV("nodes size", nodes->size());
                        callback(nullptr, std::move(nodes));
                    });
            });
        });
    });
}

Error::Ptr Ledger::checkTableValid(Error::UniquePtr&& error,
    const std::optional<bcos::storage::Table>& table, const std::string_view& tableName)
{
    if (error)
    {
        std::stringstream ss;
        ss << "Open table: " << tableName << " failed!";
        LEDGER_LOG(ERROR) << ss.str() << boost::diagnostic_information(*error);

        return BCOS_ERROR_WITH_PREV_PTR(LedgerError::OpenTableFailed, ss.str(), *error);
    }

    if (!table)
    {
        std::stringstream ss;
        ss << "Table: " << tableName << " does not exists!";
        LEDGER_LOG(ERROR) << ss.str();
        return BCOS_ERROR_PTR(LedgerError::OpenTableFailed, ss.str());
    }

    return nullptr;
}

Error::Ptr Ledger::checkEntryValid(Error::UniquePtr&& error,
    const std::optional<bcos::storage::Entry>& entry, const std::string_view& key)
{
    if (error)
    {
        std::stringstream ss;
        ss << "Get row: " << key << " failed!";
        LEDGER_LOG(ERROR) << ss.str() << boost::diagnostic_information(*error);

        return BCOS_ERROR_WITH_PREV_PTR(LedgerError::GetStorageError, ss.str(), *error);
    }

    if (!entry)
    {
        std::stringstream ss;
        ss << "Entry: " << key << " does not exists!";
        LEDGER_LOG(ERROR) << ss.str();

        return BCOS_ERROR_PTR(LedgerError::GetStorageError, ss.str());
    }

    return nullptr;
}

void Ledger::asyncGetBlockHeader(bcos::protocol::Block::Ptr block,
    bcos::protocol::BlockNumber blockNumber, std::function<void(Error::Ptr&&)> callback)
{
    m_storage->asyncOpenTable(SYS_NUMBER_2_BLOCK_HEADER,
        [this, blockNumber, block, callback](auto&& error, std::optional<Table>&& table) {
            auto validError = checkTableValid(std::move(error), table, SYS_NUMBER_2_BLOCK_HEADER);
            if (validError)
            {
                callback(std::move(validError));
                return;
            }

            table->asyncGetRow(boost::lexical_cast<std::string>(blockNumber),
                [this, blockNumber, block, callback](auto&& error, std::optional<Entry>&& entry) {
                    auto validError = checkEntryValid(
                        std::move(error), entry, boost::lexical_cast<std::string>(blockNumber));
                    if (validError)
                    {
                        callback(std::move(validError));
                        return;
                    }

                    auto field = entry->getField(SYS_VALUE);
                    auto headerPtr = m_blockFactory->blockHeaderFactory()->createBlockHeader(
                        bcos::bytesConstRef((bcos::byte*)field.data(), field.size()));

                    block->setBlockHeader(std::move(headerPtr));
                    callback(nullptr);
                });
        });
}

void Ledger::asyncGetBlockTransactionHashes(bcos::protocol::BlockNumber blockNumber,
    std::function<void(Error::Ptr&&, std::vector<std::string>&&)> callback)
{
    m_storage->asyncOpenTable(SYS_NUMBER_2_TXS,
        [this, blockNumber, callback](auto&& error, std::optional<Table>&& table) {
            auto validError = checkTableValid(std::move(error), table, SYS_NUMBER_2_BLOCK_HEADER);
            if (validError)
            {
                callback(std::move(validError), std::vector<std::string>());
                return;
            }

            table->asyncGetRow(boost::lexical_cast<std::string>(blockNumber),
                [this, blockNumber, callback](auto&& error, std::optional<Entry>&& entry) {
                    auto validError = checkEntryValid(
                        std::move(error), entry, boost::lexical_cast<std::string>(blockNumber));
                    if (validError)
                    {
                        callback(std::move(validError), std::vector<std::string>());
                        return;
                    }

                    auto txs = entry->getField(SYS_VALUE);
                    auto blockWithTxs = m_blockFactory->createBlock(
                        bcos::bytesConstRef((bcos::byte*)txs.data(), txs.size()));

                    std::vector<std::string> hashList(blockWithTxs->transactionsHashSize());
                    for (size_t i = 0; i < blockWithTxs->transactionsHashSize(); ++i)
                    {
                        auto hash = blockWithTxs->transactionHash(i);
                        hashList[i] = hash.hex();
                    }

                    callback(nullptr, std::move(hashList));
                });
        });
}

void Ledger::asyncBatchGetTransactions(std::shared_ptr<std::vector<std::string>> hashes,
    std::function<void(Error::Ptr&&, std::vector<protocol::Transaction::Ptr>&&)> callback)
{
    m_storage->asyncOpenTable(SYS_HASH_2_TX, [this, hashes, callback](
                                                 auto&& error, std::optional<Table>&& table) {
        auto validError = checkTableValid(std::move(error), table, SYS_HASH_2_TX);
        if (validError)
        {
            callback(std::move(validError), std::vector<protocol::Transaction::Ptr>());
            return;
        }

        std::vector<std::string_view> hashesView;
        hashesView.reserve(hashes->size());
        for (auto& hash : *hashes)
        {
            hashesView.push_back(hash);
        }

        table->asyncGetRows(hashesView, [this, hashes, callback](auto&& error,
                                            std::vector<std::optional<Entry>>&& entries) {
            if (error)
            {
                LEDGER_LOG(ERROR) << "Batch get transaction error!"
                                  << boost::diagnostic_information(*error);
                callback(BCOS_ERROR_WITH_PREV_PTR(
                             LedgerError::GetStorageError, "Batch get transaction error!", *error),
                    std::vector<protocol::Transaction::Ptr>());

                return;
            }

            std::vector<protocol::Transaction::Ptr> transactions;
            size_t i = 0;
            for (auto& entry : entries)
            {
                if (!entry.has_value())
                {
                    LEDGER_LOG(INFO) << "Get transaction failed: " << (*hashes)[i] << " not found";
                }
                else
                {
                    auto field = entry->getField(SYS_VALUE);
                    auto transaction = m_blockFactory->transactionFactory()->createTransaction(
                        bcos::bytesConstRef((bcos::byte*)field.data(), field.size()));
                    transactions.push_back(std::move(transaction));
                }

                ++i;
            }

            callback(nullptr, std::move(transactions));
        });
    });
}

void Ledger::asyncBatchGetReceipts(std::shared_ptr<std::vector<std::string>> hashes,
    std::function<void(Error::Ptr&&, std::vector<protocol::TransactionReceipt::Ptr>&&)> callback)
{
    m_storage->asyncOpenTable(
        SYS_HASH_2_RECEIPT, [this, hashes, callback](auto&& error, std::optional<Table>&& table) {
            auto validError = checkTableValid(std::move(error), table, SYS_HASH_2_RECEIPT);
            if (validError)
            {
                callback(std::move(validError), std::vector<protocol::TransactionReceipt::Ptr>());
                return;
            }

            table->asyncGetRows(*hashes, [this, hashes, callback](auto&& error,
                                             std::vector<std::optional<Entry>>&& entries) {
                if (error)
                {
                    LEDGER_LOG(ERROR)
                        << "Batch get receipt error!" << boost::diagnostic_information(*error);
                    callback(BCOS_ERROR_WITH_PREV_PTR(
                                 LedgerError::GetStorageError, "Batch get receipt error!", *error),
                        std::vector<protocol::TransactionReceipt::Ptr>());

                    return;
                }

                size_t i = 0;
                std::vector<protocol::TransactionReceipt::Ptr> receipts;
                receipts.reserve(hashes->size());
                for (auto& entry : entries)
                {
                    if (!entry.has_value())
                    {
                        LEDGER_LOG(ERROR) << "Get receipt error: " << (*hashes)[i];
                        callback(BCOS_ERROR_PTR(
                                     LedgerError::GetStorageError, "Batch get transaction error!"),
                            std::vector<protocol::TransactionReceipt::Ptr>());
                        return;
                    }

                    auto field = entry->getField(SYS_VALUE);
                    auto receipt = m_blockFactory->receiptFactory()->createReceipt(
                        bcos::bytesConstRef((bcos::byte*)field.data(), field.size()));
                    receipts.push_back(std::move(receipt));

                    ++i;
                }

                callback(nullptr, std::move(receipts));
            });
        });
}

void Ledger::asyncGetSystemTableEntry(const std::string_view& table, const std::string_view& key,
    std::function<void(Error::Ptr&&, std::optional<bcos::storage::Entry>&&)> callback)
{
    m_storage->asyncOpenTable(table, [this, key = std::string(key), callback = std::move(callback)](
                                         auto&& error, std::optional<Table>&& table) {
        auto tableError =
            checkTableValid(std::forward<decltype(error)>(error), table, SYS_CURRENT_STATE);
        if (tableError)
        {
            callback(std::move(tableError), {});
            return;
        }

        table->asyncGetRow(key, [this, key, callback = std::move(callback)](
                                    auto&& error, std::optional<Entry>&& entry) {
            auto entryError = checkEntryValid(std::move(error), entry, key);
            if (entryError)
            {
                callback(std::move(entryError), {});
                return;
            }

            callback(nullptr, std::move(entry));
        });
    });
}

void Ledger::getTxProof(
    const HashType& _txHash, std::function<void(Error::Ptr, MerkleProofPtr)> _onGetProof)
{
    (void)_txHash;
    _onGetProof(BCOS_ERROR_PTR(-1, "Unimplemented method"), nullptr);
    // get receipt to get block number
    // getStorageGetter()->getReceiptByTxHash(_txHash.hex(), getMemoryTableFactory(0),
    //     [this, _txHash, _onGetProof](Error::Ptr _error, bcos::storage::Entry::Ptr _receiptEntry)
    //     {
    //         if (_error && _error->errorCode() != CommonError::SUCCESS)
    //         {
    //             LEDGER_LOG(ERROR) << LOG_BADGE("getTxProof")
    //                               << LOG_DESC("getReceiptByTxHash from storage error")
    //                               << LOG_KV("errorCode", _error->errorCode())
    //                               << LOG_KV("errorMsg", _error->errorMessage())
    //                               << LOG_KV("txHash", _txHash.hex());
    //             _onGetProof(_error, nullptr);
    //             return;
    //         }
    //         if (!_receiptEntry)
    //         {
    //             LEDGER_LOG(ERROR) << LOG_BADGE("getTxProof")
    //                               << LOG_DESC("getReceiptByTxHash from storage callback null
    //                               entry")
    //                               << LOG_KV("txHash", _txHash.hex());
    //             _onGetProof(nullptr, nullptr);
    //             return;
    //         }
    //         auto receipt = decodeReceipt(getReceiptFactory(),
    //         _receiptEntry->getField(SYS_VALUE)); if (!receipt)
    //         {
    //             LEDGER_LOG(ERROR) << LOG_BADGE("getTxProof") << LOG_DESC("receipt is null or
    //             empty")
    //                               << LOG_KV("txHash", _txHash.hex());
    //             auto error = std::make_shared<Error>(
    //                 LedgerError::DecodeError, "getReceiptByTxHash callback empty receipt");
    //             _onGetProof(error, nullptr);
    //             return;
    //         }
    //         auto blockNumber = receipt->blockNumber();
    //         getTxs(blockNumber, [this, blockNumber, _onGetProof, _txHash](
    //                                 Error::Ptr _error, TransactionsPtr _txs) {
    //             if (_error && _error->errorCode() != CommonError::SUCCESS)
    //             {
    //                 LEDGER_LOG(ERROR)
    //                     << LOG_BADGE("getTxProof") << LOG_DESC("getTxs callback error")
    //                     << LOG_KV("errorCode", _error->errorCode())
    //                     << LOG_KV("errorMsg", _error->errorMessage());
    //                 _onGetProof(_error, nullptr);
    //                 return;
    //             }
    //             if (!_txs)
    //             {
    //                 LEDGER_LOG(ERROR)
    //                     << LOG_BADGE("getTxProof") << LOG_DESC("get txs error")
    //                     << LOG_KV("blockNumber", blockNumber) << LOG_KV("txHash", _txHash.hex());
    //                     auto error = std::make_shared<Error>( LedgerError::CallbackError, "getTxs
    // callback empty txs");
    // _onGetProof(error, nullptr);
    // return;
    //             }
    //             auto merkleProofPtr = std::make_shared<MerkleProof>();
    //             auto parent2ChildList = m_merkleProofUtility->getParent2ChildListByTxsProofCache(
    //                 blockNumber, _txs, m_blockFactory->cryptoSuite());
    //             auto child2Parent = m_merkleProofUtility->getChild2ParentCacheByTransaction(
    //                 parent2ChildList, blockNumber);
    //             m_merkleProofUtility->getMerkleProof(
    //                 _txHash, *parent2ChildList, *child2Parent, *merkleProofPtr);
    //             LEDGER_LOG(TRACE) << LOG_BADGE("getTxProof") << LOG_DESC("get merkle proof
    //             success")
    //                               << LOG_KV("blockNumber", blockNumber)
    //                               << LOG_KV("txHash", _txHash.hex());
    //             _onGetProof(nullptr, merkleProofPtr);
    //         });
    //     });
}

void Ledger::getReceiptProof(protocol::TransactionReceipt::Ptr _receipt,
    std::function<void(Error::Ptr, MerkleProofPtr)> _onGetProof)
{
    (void)_receipt;
    _onGetProof(BCOS_ERROR_PTR(-1, "Unimplemented method"), nullptr);
    // if (!_receipt)
    // {
    //     _onGetProof(nullptr, nullptr);
    //     return;
    // }
    // getStorageGetter()->getTxsFromStorage(_receipt->blockNumber(), getMemoryTableFactory(0),
    //     [this, _onGetProof, _receipt](Error::Ptr _error, bcos::storage::Entry::Ptr _blockEntry) {
    //         if (_error && _error->errorCode() != CommonError::SUCCESS)
    //         {
    //             LEDGER_LOG(ERROR) << LOG_BADGE("getReceiptProof")
    //                               << LOG_DESC("getTxsFromStorage callback error")
    //                               << LOG_KV("errorCode", _error->errorCode())
    //                               << LOG_KV("errorMsg", _error->errorMessage());
    //             _onGetProof(_error, nullptr);
    //             return;
    //         }
    //         if (!_blockEntry)
    //         {
    //             LEDGER_LOG(ERROR) << LOG_BADGE("getReceiptProof")
    //                               << LOG_DESC("getTxsFromStorage callback null entry")
    //                               << LOG_KV("blockNumber", _receipt->blockNumber());
    //             _onGetProof(nullptr, nullptr);
    //             return;
    //         }
    //         auto block = decodeBlock(m_blockFactory, _blockEntry->getField(SYS_VALUE));
    //         if (!block)
    //         {
    //             LEDGER_LOG(ERROR) << LOG_BADGE("getReceiptProof")
    //                               << LOG_DESC("getTxsFromStorage callback empty block txs");
    //             auto error = std::make_shared<Error>(LedgerError::DecodeError, "empty txs");
    //             _onGetProof(error, nullptr);
    //             return;
    //         }
    //         auto txHashList = blockTxHashListGetter(block);
    //         getStorageGetter()->getBatchReceiptsByHashList(txHashList, getMemoryTableFactory(0),
    //             getReceiptFactory(),
    //             [this, _onGetProof, _receipt](Error::Ptr _error, ReceiptsPtr receipts) {
    //                 if (_error && _error->errorCode() != CommonError::SUCCESS)
    //                 {
    //                     LEDGER_LOG(ERROR) << LOG_BADGE("getReceiptProof")
    //                                       << LOG_DESC("getBatchReceiptsByHashList callback
    //                                       error")
    //                                       << LOG_KV("errorCode", _error->errorCode())
    //                                       << LOG_KV("errorMsg", _error->errorMessage());
    //                     _onGetProof(_error, nullptr);
    //                     return;
    //                 }
    //                 if (!receipts || receipts->empty())
    //                 {
    //                     LEDGER_LOG(ERROR)
    //                         << LOG_BADGE("getReceiptProof")
    //                         << LOG_DESC("getBatchReceiptsByHashList callback empty
    //                         receipts");
    //                     auto error =
    //                         std::make_shared<Error>(LedgerError::CallbackError, "empty
    //                         receipts");
    //                     _onGetProof(error, nullptr);
    //                     return;
    //                 }
    //                 auto merkleProof = std::make_shared<MerkleProof>();
    //                 auto parent2ChildList =
    //                     m_merkleProofUtility->getParent2ChildListByReceiptProofCache(
    //                         _receipt->blockNumber(), receipts, m_blockFactory->cryptoSuite());
    //                 auto child2Parent = m_merkleProofUtility->getChild2ParentCacheByReceipt(
    //                     parent2ChildList, _receipt->blockNumber());
    //                 m_merkleProofUtility->getMerkleProof(
    //                     _receipt->hash(), *parent2ChildList, *child2Parent, *merkleProof);
    //                 LEDGER_LOG(INFO)
    //                     << LOG_BADGE("getReceiptProof") << LOG_DESC("call back receipt and
    //                     proof");
    //                 _onGetProof(nullptr, merkleProof);
    //             });
    //     });
}

void Ledger::notifyCommittedBlockNumber(protocol::BlockNumber _blockNumber)
{
    if (!m_committedBlockNotifier)
        return;
    m_committedBlockNotifier(_blockNumber, [_blockNumber](Error::Ptr _error) {
        if (!_error)
        {
            return;
        }
        LEDGER_LOG(WARNING) << LOG_BADGE("notifyCommittedBlockNumber")
                            << LOG_DESC("notify the block number failed")
                            << LOG_KV("blockNumber", _blockNumber);
    });
}

// sync methed
bool Ledger::buildGenesisBlock(LedgerConfig::Ptr _ledgerConfig, const std::string& _groupId,
    size_t _gasLimit, const std::string& _genesisData)
{
    LEDGER_LOG(INFO) << LOG_DESC("[#buildGenesisBlock]");
    if (_ledgerConfig->consensusTimeout() > SYSTEM_CONSENSUS_TIMEOUT_MAX ||
        _ledgerConfig->consensusTimeout() < SYSTEM_CONSENSUS_TIMEOUT_MIN)
    {
        LEDGER_LOG(ERROR) << LOG_BADGE("buildGenesisBlock")
                          << LOG_DESC("consensus timeout set error, return false")
                          << LOG_KV("consensusTimeout", _ledgerConfig->consensusTimeout());
        return false;
    }

    if (_gasLimit < TX_GAS_LIMIT_MIN)
    {
        LEDGER_LOG(ERROR) << LOG_BADGE("buildGenesisBlock")
                          << LOG_DESC("gas limit too low, return false")
                          << LOG_KV("gasLimit", _gasLimit)
                          << LOG_KV("gasLimitMin", TX_GAS_LIMIT_MIN);
        return false;
    }

    std::promise<std::tuple<Error::Ptr, bcos::crypto::HashType>> getBlockPromise;
    asyncGetBlockHashByNumber(0, [&](Error::Ptr error, const bcos::crypto::HashType& hash) {
        getBlockPromise.set_value({std::move(error), hash});
    });

    auto getBlockResult = getBlockPromise.get_future().get();
    if (std::get<0>(getBlockResult) &&
        std::get<0>(getBlockResult)->errorCode() != LedgerError::GetStorageError)
    {
        BOOST_THROW_EXCEPTION(*(std::get<0>(getBlockResult)));
    }

    if (std::get<1>(getBlockResult))
    {
        // genesis block exists, quit
        LEDGER_LOG(INFO) << LOG_DESC("[#buildGenesisBlock] success, block exists");
        return true;
    }

    // clang-format off
    std::string_view tables[] = {
        SYS_CONFIG, "value,enable_number",
        SYS_CONSENSUS, "type,weight,enable_number",
        SYS_CURRENT_STATE, SYS_VALUE,
        SYS_HASH_2_TX, SYS_VALUE,
        SYS_HASH_2_NUMBER, SYS_VALUE,
        SYS_NUMBER_2_HASH, SYS_VALUE,
        SYS_NUMBER_2_BLOCK_HEADER, SYS_VALUE,
        SYS_NUMBER_2_TXS, SYS_VALUE,
        SYS_HASH_2_RECEIPT, SYS_VALUE,
        SYS_BLOCK_NUMBER_2_NONCES, SYS_VALUE
    };
    // clang-format on
    size_t total = sizeof(tables) / sizeof(std::string_view);

    for (size_t i = 0; i < total; i += 2)
    {
        std::promise<std::tuple<Error::UniquePtr>> createTablePromise;
        m_storage->asyncCreateTable(std::string(tables[i]), std::string(tables[i + 1]),
            [&createTablePromise](auto&& error, std::optional<Table>&&) {
                createTablePromise.set_value({std::move(error)});
            });
        auto createTableResult = createTablePromise.get_future().get();
        if (std::get<0>(createTableResult))
        {
            BOOST_THROW_EXCEPTION(*(std::get<0>(createTableResult)));
        }
    }

    createFileSystemTables(_groupId);

    auto txLimit = _ledgerConfig->blockTxCountLimit();
    LEDGER_LOG(INFO) << LOG_DESC("Commit the genesis block") << LOG_KV("txLimit", txLimit);
    // build a block
    auto header = m_blockFactory->blockHeaderFactory()->createBlockHeader();
    header->setNumber(0);
    header->setExtraData(asBytes(_genesisData));

    auto block = m_blockFactory->createBlock();
    block->setBlockHeader(header);

    std::promise<Error::Ptr> genesisBlockPromise;
    asyncPrewriteBlock(m_storage, block, [&genesisBlockPromise](Error::Ptr&& error) {
        genesisBlockPromise.set_value(std::move(error));
    });

    auto error = genesisBlockPromise.get_future().get();
    if (error)
    {
        BOOST_THROW_EXCEPTION(*error);
    }

    // write sys config
    std::promise<std::tuple<Error::UniquePtr, std::optional<Table>>> sysTablePromise;
    m_storage->asyncOpenTable(
        SYS_CONFIG, [&sysTablePromise](auto&& error, std::optional<Table>&& table) {
            sysTablePromise.set_value({std::move(error), std::move(table)});
        });

    auto [tableError, sysTable] = sysTablePromise.get_future().get();
    if (tableError)
    {
        BOOST_THROW_EXCEPTION(*tableError);
    }

    if (!sysTable)
    {
        BOOST_THROW_EXCEPTION(BCOS_ERROR(LedgerError::OpenTableFailed, "Open SYS_CONFIG failed!"));
    }

    Entry txLimitEntry;
    txLimitEntry.importFields(
        {boost::lexical_cast<std::string>(_ledgerConfig->blockTxCountLimit()), "0"});
    sysTable->setRow(SYSTEM_KEY_TX_COUNT_LIMIT, std::move(txLimitEntry));

    Entry gasLimitEntry;
    gasLimitEntry.importFields({boost::lexical_cast<std::string>(_gasLimit), "0"});
    sysTable->setRow(SYSTEM_KEY_TX_GAS_LIMIT, std::move(gasLimitEntry));

    Entry leaderPeriodEntry;
    leaderPeriodEntry.importFields(
        {boost::lexical_cast<std::string>(_ledgerConfig->leaderSwitchPeriod()), "0"});
    sysTable->setRow(SYSTEM_KEY_CONSENSUS_LEADER_PERIOD, std::move(leaderPeriodEntry));

    Entry consensusTimeout;
    consensusTimeout.importFields(
        {boost::lexical_cast<std::string>(_ledgerConfig->consensusTimeout()), "0"});
    sysTable->setRow(SYSTEM_KEY_CONSENSUS_TIMEOUT, std::move(consensusTimeout));

    // write consensus config
    std::promise<std::tuple<Error::UniquePtr, std::optional<Table>>> consensusTablePromise;
    m_storage->asyncOpenTable(
        SYS_CONSENSUS, [&consensusTablePromise](auto&& error, std::optional<Table>&& table) {
            consensusTablePromise.set_value({std::move(error), std::move(table)});
        });

    auto [consensusError, consensusTable] = consensusTablePromise.get_future().get();
    if (consensusError)
    {
        BOOST_THROW_EXCEPTION(*consensusError);
    }

    if (!consensusTable)
    {
        BOOST_THROW_EXCEPTION(
            BCOS_ERROR(LedgerError::OpenTableFailed, "Open SYS_CONSENSUS failed!"));
    }

    for (auto& node : _ledgerConfig->consensusNodeList())
    {
        Entry consensusNodeEntry;
        consensusNodeEntry.importFields({
            CONSENSUS_SEALER,
            boost::lexical_cast<std::string>(node->weight()),
            "0",
        });
        consensusTable->setRow(node->nodeID()->hex(), std::move(consensusNodeEntry));
    }

    for (auto& node : _ledgerConfig->observerNodeList())
    {
        Entry observerNodeEntry;
        observerNodeEntry.importFields({
            CONSENSUS_OBSERVER,
            boost::lexical_cast<std::string>(node->weight()),
            "0",
        });
        consensusTable->setRow(node->nodeID()->hex(), std::move(observerNodeEntry));
    }

    // write current state
    std::promise<std::tuple<Error::UniquePtr, std::optional<Table>>> stateTablePromise;
    m_storage->asyncOpenTable(
        SYS_CURRENT_STATE, [&stateTablePromise](auto&& error, std::optional<Table>&& table) {
            stateTablePromise.set_value({std::move(error), std::move(table)});
        });

    auto [stateError, stateTable] = stateTablePromise.get_future().get();
    if (stateError)
    {
        BOOST_THROW_EXCEPTION(*stateError);
    }

    if (!stateTable)
    {
        BOOST_THROW_EXCEPTION(
            BCOS_ERROR(LedgerError::OpenTableFailed, "Open SYS_CURRENT_STATE failed!"));
    }

    Entry currentNumber;
    currentNumber.importFields({"0"});
    stateTable->setRow(SYS_KEY_CURRENT_NUMBER, std::move(currentNumber));

    Entry txNumber;
    txNumber.importFields({"0"});
    stateTable->setRow(SYS_KEY_TOTAL_TRANSACTION_COUNT, std::move(txNumber));

    Entry txFailedNumber;
    txFailedNumber.importFields({"0"});
    stateTable->setRow(SYS_KEY_TOTAL_FAILED_TRANSACTION, std::move(txFailedNumber));

    return true;
}

void Ledger::createFileSystemTables(const std::string& _groupId)
{
    // create / dir
    std::promise<Error::UniquePtr> createPromise;
    m_storage->asyncCreateTable(
        FS_ROOT, FS_FIELD_COMBINED, [&createPromise](auto&& error, std::optional<Table>&&) {
            createPromise.set_value({std::move(error)});
        });
    auto createError = createPromise.get_future().get();
    if (createError)
    {
        BOOST_THROW_EXCEPTION(*createError);
    }

    std::promise<std::tuple<Error::UniquePtr, std::optional<Table>>> openPromise;
    m_storage->asyncOpenTable(FS_ROOT, [&openPromise](auto&& error, std::optional<Table>&& table) {
        openPromise.set_value({std::move(error), std::move(table)});
    });

    auto [openError, table] = openPromise.get_future().get();

    assert(table);
    auto rootEntry = table->newEntry();
    rootEntry.setField(FS_FIELD_TYPE, FS_TYPE_DIR);
    // TODO: set root default permission?
    rootEntry.setField(FS_FIELD_ACCESS, "");
    rootEntry.setField(FS_FIELD_OWNER, "root");
    rootEntry.setField(FS_FIELD_GID, "/usr");
    rootEntry.setField(FS_FIELD_EXTRA, "");
    table->setRow(FS_ROOT, rootEntry);

    std::string appsDir = "/" + _groupId + FS_APPS;
    std::string tableDir = "/" + _groupId + FS_USER_TABLE;

    recursiveBuildDir(FS_USER);
    recursiveBuildDir(FS_SYS_BIN);
    recursiveBuildDir(appsDir);
    recursiveBuildDir(tableDir);
}
void Ledger::recursiveBuildDir(const std::string& _absoluteDir)
{
    if (_absoluteDir.empty())
    {
        return;
    }
    // transfer /usr/local/bin => ["usr", "local", "bin"]
    auto dirList = std::make_shared<std::vector<std::string>>();
    std::string absoluteDir = _absoluteDir;
    if (absoluteDir[0] == '/')
    {
        absoluteDir = absoluteDir.substr(1);
    }
    if (absoluteDir.at(absoluteDir.size() - 1) == '/')
    {
        absoluteDir = absoluteDir.substr(0, absoluteDir.size() - 1);
    }
    boost::split(*dirList, absoluteDir, boost::is_any_of("/"), boost::token_compress_on);
    std::string root = "/";
    for (auto& dir : *dirList)
    {
        std::promise<std::tuple<Error::UniquePtr, std::optional<Table>>> openPromise;
        m_storage->asyncOpenTable(root, [&openPromise](auto&& error, std::optional<Table>&& table) {
            openPromise.set_value({std::move(error), std::move(table)});
        });

        auto [openError, table] = openPromise.get_future().get();

        if (openError)
        {
            BOOST_THROW_EXCEPTION(*openError);
        }

        if (!table)
        {
            LEDGER_LOG(ERROR) << LOG_BADGE("recursiveBuildDir")
                              << LOG_DESC("can not open path table") << LOG_KV("tableName", root);
            return;
        }
        if (root != "/")
        {
            root += "/";
        }
        auto entry = table->getRow(dir);
        if (entry)
        {
            LEDGER_LOG(DEBUG) << LOG_BADGE("recursiveBuildDir")
                              << LOG_DESC("dir already existed in parent dir, continue")
                              << LOG_KV("parentDir", root) << LOG_KV("dir", dir);
            root += dir;
            continue;
        }
        // not exist, then create table and write in parent dir
        auto newFileEntry = table->newEntry();
        newFileEntry.setField(FS_FIELD_TYPE, FS_TYPE_DIR);
        // FIXME: consider permission inheritance
        newFileEntry.setField(FS_FIELD_ACCESS, "");
        newFileEntry.setField(FS_FIELD_OWNER, "root");
        newFileEntry.setField(FS_FIELD_GID, "/usr");
        newFileEntry.setField(FS_FIELD_EXTRA, "");
        table->setRow(dir, newFileEntry);

        std::promise<Error::UniquePtr> createPromise;
        m_storage->asyncCreateTable(
            root + dir, FS_FIELD_COMBINED, [&createPromise](auto&& error, std::optional<Table>&&) {
                createPromise.set_value({std::move(error)});
            });

        auto createError = createPromise.get_future().get();
        if (createError)
        {
            BOOST_THROW_EXCEPTION(*createError);
        }

        root += dir;
    }
}