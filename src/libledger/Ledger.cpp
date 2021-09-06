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
#include "BatchWriter.h"
#include "bcos-framework/interfaces/consensus/ConsensusNode.h"
#include "interfaces/crypto/CommonType.h"
#include "interfaces/ledger/LedgerTypeDef.h"
#include "interfaces/protocol/ProtocolTypeDef.h"
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

    auto headerBuffer = std::make_shared<bytes>();
    header->encode(*headerBuffer);

    writeNumber(header->number(), storage);
    writeHash2Number(header, storage);
    writeNumber2BlockHeader(header, storage);
    writeTotalTransactionCount(mutableBlock, storage);
    writeNumber2Nonces(mutableBlock, storage);
    writeNumber2Transactions(mutableBlock, storage);
    writeHash2Receipt(mutableBlock, storage);

    callback(nullptr);
}

void Ledger::asyncStoreTransactions(std::shared_ptr<std::vector<bytesPointer>> _txToStore,
    crypto::HashListPtr _txHashList, std::function<void(Error::Ptr)> _onTxStored)
{
    if (!_txToStore || !_txHashList || _txToStore->size() != _txHashList->size())
    {
        _onTxStored(
            BCOS_ERROR_PTR(LedgerError::ErrorArgument, "asyncStoreTransactions argument error!"));
        return;
    }

    m_storage->asyncOpenTable(SYS_HASH_2_TX,
        [this, storage = m_storage, hashList = std::move(_txHashList),
            txList = std::move(_txToStore),
            callback = std::move(_onTxStored)](auto&& error, std::optional<Table>&& table) {
            auto validError = checkTableValid(std::move(error), table, SYS_HASH_2_TX);
            if (validError)
            {
                callback(std::move(validError));
                return;
            }

            auto total = txList->size();
            auto count =
                std::make_shared<std::tuple<std::atomic<size_t>, std::atomic<size_t>>>(0, 0);
            for (size_t i = 0; i < txList->size(); ++i)
            {
                auto entry = table->newEntry();
                entry.setField(SYS_VALUE, asString(*((*txList)[i])));  // TODO: avoid copy the
                                                                       // transaction data

                // TODO: using batch set row
                storage->asyncSetRow(*table->tableInfo(), (*hashList)[i].hex(), std::move(entry),
                    [total, count, callback](Error::Ptr&& error, bool success) {
                        if (error)
                        {
                            ++std::get<1>(*count);
                            LEDGER_LOG(ERROR) << "Set row failed!" << error->what();
                        }
                        else if (!success)
                        {
                            ++std::get<1>(*count);
                            LEDGER_LOG(ERROR) << "Transaction already exists!";
                        }
                        else
                        {
                            ++std::get<0>(*count);
                        }

                        if (std::get<0>(*count) + std::get<1>(*count) == total)
                        {
                            // All finished
                            callback(nullptr);
                        }
                    });
            }
        });
}

void Ledger::asyncGetBlockDataByNumber(bcos::protocol::BlockNumber _blockNumber, int32_t _blockFlag,
    std::function<void(Error::Ptr, bcos::protocol::Block::Ptr)> _onGetBlock)
{
    if (_blockNumber < 0 || _blockFlag < 0)
    {
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
                LEDGER_LOG(ERROR) << "Get block failed with errors!";
                _onGetBlock(BCOS_ERROR_PTR(LedgerError::CollectAsyncCallbackError,
                                "Get block failed with errors!"),
                    nullptr);
                return;
            }

            _onGetBlock(nullptr, std::move(block));
        }
    };

    if (_blockFlag ^ HEADER)
    {
        ++(*total);

        fetchers.push_back([this, _blockNumber, block, finally]() {
            asyncGetBlockHeader(
                block, _blockNumber, [finally](Error::Ptr&& error) { finally(std::move(error)); });
        });
    }
    if (_blockFlag ^ TRANSACTIONS)
        ++(*total);
    if (_blockFlag ^ RECEIPTS)
        ++(*total);

    if ((_blockFlag ^ TRANSACTIONS) || (_blockFlag ^ RECEIPTS))
    {
        fetchers.push_back([this, block, _blockNumber, finally, _blockFlag]() {
            asyncGetBlockTransactionHashes(block, _blockNumber,
                [this, _blockFlag, block, finally](
                    Error::Ptr&& error, std::vector<std::string>&& hashes) {
                    if (error)
                    {
                        if (_blockFlag ^ TRANSACTIONS)
                            finally(std::move(error));
                        if (_blockFlag ^ RECEIPTS)
                            finally(std::move(error));
                        return;
                    }

                    auto hashesPtr = std::make_shared<std::vector<std::string>>(std::move(hashes));
                    if (_blockFlag ^ TRANSACTIONS)
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
                    if (_blockFlag ^ RECEIPTS)
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
                             "Get block number storage error", std::move(error)),
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
    auto key = boost::lexical_cast<std::string>(_blockNumber);
    asyncGetSystemTableEntry(SYS_NUMBER_2_HASH, key,
        [callback = std::move(_onGetBlock)](
            Error::Ptr&& error, std::optional<bcos::storage::Entry>&& entry) {
            if (error)
            {
                LEDGER_LOG(ERROR) << "GetBlockHashByNumber error"
                                  << boost::diagnostic_information(error);
                callback(BCOS_ERROR_WITH_PREV_PTR(LedgerError::GetStorageError,
                             "GetBlockHashByNumber error", std::move(error)),
                    bcos::crypto::HashType());
                return;
            }

            auto hashStr = entry->getField(SYS_VALUE);
            bcos::crypto::HashType hash(
                bcos::bytesConstRef((bcos::byte*)hashStr.data(), hashStr.size()));

            LEDGER_LOG(INFO) << "GetBlockHashByNumber success" << LOG_KV("hash", hashStr);
            callback(nullptr, std::move(hash));
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
                        << "GetBlockNumberByHash error" << boost::diagnostic_information(error);
                    callback(BCOS_ERROR_WITH_PREV_PTR(LedgerError::GetStorageError,
                                 "GetBlockHashByNumber error", std::move(error)),
                        0);
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
    LEDGER_LOG(INFO) << "GetBatchTxsByHashList request" << LOG_KV("hashes", _txHashList->size())
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
                LEDGER_LOG(ERROR) << "GetBatchTxsByHashList error"
                                  << boost::diagnostic_information(error);
                callback(BCOS_ERROR_WITH_PREV_PTR(LedgerError::GetStorageError,
                             "GetBatchTxsByHashList error", std::move(error)),
                    nullptr, nullptr);
                return;
            }

            bcos::protocol::TransactionsPtr results =
                std::make_shared<bcos::protocol::Transactions>(std::move(transactions));

            if (_withProof)
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
                LEDGER_LOG(INFO) << LOG_BADGE("GetBatchTxsByHashList success")
                                 << LOG_KV("txHashListSize", _txHashList->size())
                                 << LOG_KV("withProof", _withProof);
                callback(nullptr, results, nullptr);
            }
        });
}

void Ledger::asyncGetTransactionReceiptByHash(bcos::crypto::HashType const& _txHash,
    bool _withProof,
    std::function<void(Error::Ptr, bcos::protocol::TransactionReceipt::ConstPtr, MerkleProofPtr)>
        _onGetTx)
{
    auto key = _txHash.hex();

    LEDGER_LOG(INFO) << "GetTransactionReceiptByHash" << LOG_KV("hash", key);

    asyncGetSystemTableEntry(SYS_HASH_2_RECEIPT, key,
        [this, callback = std::move(_onGetTx), _withProof, key](
            Error::Ptr&& error, std::optional<bcos::storage::Entry>&& entry) {
            if (error)
            {
                LEDGER_LOG(ERROR) << "GetTransactionReceiptByHash error"
                                  << boost::diagnostic_information(error);
                callback(BCOS_ERROR_WITH_PREV_PTR(LedgerError::GetStorageError,
                             "GetTransactionReceiptByHash", std::move(error)),
                    nullptr, nullptr);

                return;
            }

            auto value = entry->getField(SYS_VALUE);
            auto receipt = m_blockFactory->receiptFactory()->createReceipt(
                bcos::bytesConstRef((bcos::byte*)value.data(), value.size()));

            if (_withProof)
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
                LEDGER_LOG(DEBUG) << "GetTransactionReceiptByHash success" << LOG_KV("hash", key);
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
    m_storage->asyncOpenTable(SYS_CURRENT_STATE,
        [this, callback = std::move(_callback)](Error::Ptr&& error, std::optional<Table>&& table) {
            auto tableError = checkTableValid(std::move(error), table, SYS_CURRENT_STATE);
            if (tableError)
            {
                LEDGER_LOG(ERROR) << "GetTotalTransactionCount error"
                                  << boost::diagnostic_information(tableError);
                callback(std::move(tableError), 0, 0, 0);
                return;
            }

            table->asyncGetRows(keys, [callback = std::move(callback)](Error::Ptr&& error,
                                          std::vector<std::optional<Entry>>&& entries) {
                if (error)
                {
                    LEDGER_LOG(ERROR)
                        << "GetTotalTransactionCount error" << boost::diagnostic_information(error);
                    callback(std::move(error), 0, 0, 0);
                    return;
                }

                int64_t totalCount, failedCount, blockNumber;
                size_t i = 0;
                for (auto& entry : entries)
                {
                    if (!entry)
                    {
                        LEDGER_LOG(ERROR)
                            << "GetTotalTransactionCount error" << LOG_KV("index", i) << " empty";
                        callback(std::move(error), 0, 0, 0);
                        return;
                    }

                    switch (i)
                    {
                    case 0:
                        totalCount = boost::lexical_cast<int64_t>(entry->getField(SYS_VALUE));
                        break;
                    case 1:
                        failedCount = boost::lexical_cast<int64_t>(entry->getField(SYS_VALUE));
                        break;
                    case 2:
                        blockNumber = boost::lexical_cast<int64_t>(entry->getField(SYS_VALUE));
                        break;
                    }
                }

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
            LEDGER_LOG(ERROR) << "GetSystemConfigByKey error"
                              << boost::diagnostic_information(*error);
            callback(std::move(error), "", 0);
            return;
        }

        asyncGetSystemTableEntry(SYS_CONFIG, _key,
            [blockNumber, callback = std::move(callback)](
                Error::Ptr&& error, std::optional<bcos::storage::Entry>&& entry) {
                try
                {
                    if (error)
                    {
                        LEDGER_LOG(ERROR) << "GetSystemConfigByKey error"
                                          << boost::diagnostic_information(*error);
                        callback(std::move(error), "", 0);
                        return;
                    }

                    auto value = entry->getField(SYS_VALUE);
                    auto number = boost::lexical_cast<bcos::protocol::BlockNumber>(
                        entry->getField(SYS_CONFIG_ENABLE_BLOCK_NUMBER));

                    if (number > blockNumber)
                    {
                        LEDGER_LOG(ERROR) << "GetSystemConfigByKey error, config not available"
                                          << LOG_KV("currentBlockNumber", blockNumber)
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
                        << "GetSystemConfigByKey error" << boost::diagnostic_information(e);
                    callback(
                        BCOS_ERROR_WITH_PREV_PTR(LedgerError::GetStorageError, "error", e), "", 0);
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
    m_storage->asyncOpenTable(SYS_BLOCK_NUMBER_2_NONCES, [this, callback = std::move(_onGetList),
                                                             _startNumber,
                                                             _offset](Error::Ptr&& error,
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

        table->asyncGetRows(asView(numberList), [this, numberList, callback = std::move(callback)](
                                                    Error::Ptr&& error,
                                                    std::vector<std::optional<Entry>>&& entries) {
            if (error)
            {
                LEDGER_LOG(ERROR) << "GetNonceList error" << boost::diagnostic_information(*error);
                callback(BCOS_ERROR_WITH_PREV_PTR(
                             LedgerError::GetStorageError, "GetNonceList", std::move(error)),
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
                        bcos::bytesConstRef((bcos::byte*)value.data(), value.size()));

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
                         LedgerError::GetStorageError, "GetNodeListByType error", std::move(error)),
                nullptr);
            return;
        }

        m_storage->asyncOpenTable(SYS_CONSENSUS,
            [this, type = std::move(type), callback = std::move(callback), blockNumber](
                Error::Ptr&& error, std::optional<Table>&& table) {
                auto tableError = checkTableValid(std::move(error), table, SYS_CONSENSUS);
                if (tableError)
                {
                    callback(std::move(tableError), nullptr);
                    return;
                }

                auto tablePtr = std::make_shared<Table>(std::move(*table));

                tablePtr->asyncGetPrimaryKeys({}, [this, type = std::move(type),
                                                      callback = std::move(callback), tablePtr,
                                                      blockNumber](Error::Ptr&& error,
                                                      std::vector<std::string>&& keys) {
                    if (error)
                    {
                        LEDGER_LOG(ERROR)
                            << "GetNodeListByType error" << boost::diagnostic_information(*error);
                        callback(BCOS_ERROR_WITH_PREV_PTR(LedgerError::GetStorageError,
                                     "GetNodeListByType error", std::move(error)),
                            nullptr);
                        return;
                    }

                    tablePtr->asyncGetRows(
                        asView(keys), [this, callback = std::move(callback), type = std::move(type),
                                          keys, blockNumber](Error::Ptr&& error,
                                          std::vector<std::optional<Entry>>&& entries) {
                            if (error)
                            {
                                LEDGER_LOG(ERROR) << "GetNodeListByType error"
                                                  << boost::diagnostic_information(*error);
                                callback(BCOS_ERROR_WITH_PREV_PTR(LedgerError::GetStorageError,
                                             "GetNodeListByType error", std::move(error)),
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
                                    auto nodeType = entry->getField(NODE_TYPE);
                                    auto enableNum = boost::lexical_cast<BlockNumber>(
                                        entry->getField(NODE_ENABLE_NUMBER));
                                    auto weight =
                                        boost::lexical_cast<uint64_t>(entry->getField(NODE_WEIGHT));
                                    if ((nodeType == type) && enableNum <= blockNumber)
                                    {
                                        crypto::NodeIDPtr nodeID =
                                            m_blockFactory->cryptoSuite()->keyFactory()->createKey(
                                                *fromHexString(keys[i]));
                                        // Note: use try-catch to handle the exception case
                                        nodes->emplace_back(
                                            std::make_shared<consensus::ConsensusNode>(
                                                nodeID, weight));
                                    }
                                }
                                catch (...)
                                {
                                    continue;
                                }
                            }

                            LEDGER_LOG(INFO) << "GetNodeListByType success"
                                             << LOG_KV("nodes size", nodes->size());
                            callback(nullptr, std::move(nodes));
                        });
                });
            });
    });
}

Error::Ptr Ledger::checkTableValid(bcos::Error::Ptr&& error,
    const std::optional<bcos::storage::Table>& table, const std::string_view& tableName)
{
    if (error)
    {
        std::stringstream ss;
        ss << "Open table: " << tableName << " failed!";
        LEDGER_LOG(ERROR) << ss << boost::diagnostic_information(*error);

        return BCOS_ERROR_WITH_PREV_PTR(LedgerError::OpenTableFailed, ss.str(), std::move(error));
    }

    if (!table)
    {
        std::stringstream ss;
        ss << "Table: " << tableName << " does not exists!";
        LEDGER_LOG(ERROR) << ss;
        return BCOS_ERROR_PTR(LedgerError::OpenTableFailed, ss.str());
    }

    return nullptr;
}

Error::Ptr checkEntryValid(bcos::Error::Ptr&& error,
    const std::optional<bcos::storage::Entry>& entry, const std::string_view& key)
{
    if (error)
    {
        std::stringstream ss;
        ss << "Get row: " << key << " failed!";
        LEDGER_LOG(ERROR) << ss << boost::diagnostic_information(*error);

        return BCOS_ERROR_WITH_PREV_PTR(LedgerError::GetStorageError, ss.str(), std::move(error));
    }

    if (!entry)
    {
        std::stringstream ss;
        ss << "Entry: " << key << " does not exists!";
        LEDGER_LOG(ERROR) << ss;

        return BCOS_ERROR_PTR(LedgerError::GetStorageError, ss.str());
    }

    return nullptr;
}

void Ledger::asyncGetBlockHeader(bcos::protocol::Block::Ptr block,
    bcos::protocol::BlockNumber blockNumber, std::function<void(Error::Ptr&&)> callback)
{
    m_storage->asyncOpenTable(SYS_NUMBER_2_BLOCK_HEADER,
        [this, blockNumber, block, callback](Error::Ptr&& error, std::optional<Table>&& table) {
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

void Ledger::asyncGetBlockTransactionHashes(bcos::protocol::Block::Ptr block,
    bcos::protocol::BlockNumber blockNumber,
    std::function<void(Error::Ptr&&, std::vector<std::string>&&)> callback)
{
    m_storage->asyncOpenTable(SYS_NUMBER_2_TXS,
        [this, blockNumber, block, callback](Error::Ptr&& error, std::optional<Table>&& table) {
            auto validError = checkTableValid(std::move(error), table, SYS_NUMBER_2_BLOCK_HEADER);
            if (validError)
            {
                callback(std::move(validError), std::vector<std::string>());
                return;
            }

            table->asyncGetRow(boost::lexical_cast<std::string>(blockNumber),
                [this, blockNumber, callback, block](
                    Error::Ptr&& error, std::optional<Entry>&& entry) {
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
                        auto& hash = blockWithTxs->transactionHash(i);
                        hashList[i] = hash.hex();
                    }

                    callback(nullptr, std::move(hashList));
                });
        });
}

void Ledger::asyncBatchGetTransactions(std::shared_ptr<std::vector<std::string>> hashes,
    std::function<void(Error::Ptr&&, std::vector<protocol::Transaction::Ptr>&&)> callback)
{
    m_storage->asyncOpenTable(
        SYS_HASH_2_TX, [this, hashes, callback](Error::Ptr&& error, std::optional<Table>&& table) {
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

            table->asyncGetRows(hashesView, [this, hashes, callback](Error::Ptr&& error,
                                                std::vector<std::optional<Entry>>&& entries) {
                if (error)
                {
                    LEDGER_LOG(ERROR)
                        << "Batch get transaction error!" << boost::diagnostic_information(*error);
                    callback(BCOS_ERROR_WITH_PREV_PTR(LedgerError::GetStorageError,
                                 "Batch get transaction error!", std::move(error)),
                        std::vector<protocol::Transaction::Ptr>());

                    return;
                }

                std::vector<protocol::Transaction::Ptr> transactions(hashes->size());
                size_t i = 0;
                for (auto& entry : entries)
                {
                    if (!entry.has_value())
                    {
                        LEDGER_LOG(ERROR) << "Get transaction error: " << (*hashes)[i];
                        callback(BCOS_ERROR_PTR(
                                     LedgerError::GetStorageError, "Batch get transaction error!"),
                            std::vector<protocol::Transaction::Ptr>());
                        return;
                    }

                    auto field = entry->getField(SYS_VALUE);
                    auto transaction = m_blockFactory->transactionFactory()->createTransaction(
                        bcos::bytesConstRef((bcos::byte*)field.data(), field.size()));
                    transactions[i] = transaction;

                    ++i;
                }

                callback(nullptr, std::move(transactions));
            });
        });
}

void Ledger::asyncBatchGetReceipts(std::shared_ptr<std::vector<std::string>> hashes,
    std::function<void(Error::Ptr&&, std::vector<protocol::TransactionReceipt::Ptr>&&)> callback)
{
    m_storage->asyncOpenTable(SYS_HASH_2_RECEIPT,
        [this, hashes, callback](Error::Ptr&& error, std::optional<Table>&& table) {
            auto validError = checkTableValid(std::move(error), table, SYS_HASH_2_RECEIPT);
            if (validError)
            {
                callback(std::move(validError), std::vector<protocol::TransactionReceipt::Ptr>());
                return;
            }

            std::vector<std::string_view> hashesView;
            hashesView.reserve(hashes->size());
            for (auto& hash : *hashes)
            {
                hashesView.push_back(hash);
            }

            table->asyncGetRows(hashesView, [this, hashes, callback](Error::Ptr&& error,
                                                std::vector<std::optional<Entry>>&& entries) {
                if (error)
                {
                    LEDGER_LOG(ERROR)
                        << "Batch get receipt error!" << boost::diagnostic_information(*error);
                    callback(BCOS_ERROR_WITH_PREV_PTR(LedgerError::GetStorageError,
                                 "Batch get receipt error!", std::move(error)),
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
                    // block->appendReceipt(std::move(receipt));
                    receipts[i] = std::move(receipt);

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
                                         Error::Ptr&& error, std::optional<Table>&& table) {
        auto tableError = checkTableValid(std::move(error), table, SYS_CURRENT_STATE);
        if (tableError)
        {
            callback(std::move(tableError), {});
            return;
        }

        table->asyncGetRow(key, [this, key, callback = std::move(callback)](
                                    Error::Ptr&& error, std::optional<Entry>&& entry) {
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

std::vector<std::string_view> Ledger::asView(const std::vector<std::string>& list)
{
    std::vector<std::string_view> result;
    for (auto& it : list)
    {
        result.push_back(it);
    }

    return result;
}

void Ledger::getTxProof(
    const HashType& _txHash, std::function<void(Error::Ptr, MerkleProofPtr)> _onGetProof)
{
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
    // callback empty txs"); _onGetProof(error, nullptr); return;
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
    if (std::get<0>(getBlockResult))
    {
        BOOST_THROW_EXCEPTION(*(std::get<0>(getBlockResult)));
    }

    if (std::get<1>(getBlockResult))
    {
        // genesis block exists, quit
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
    size_t total = sizeof(tables) / sizeof(std::string_view);

    for (size_t i = 0; i < total; i += 2)
    {
        std::promise<std::tuple<Error::Ptr, bool>> createTablePromise;
        m_storage->asyncCreateTable(std::string(tables[i]), std::string(tables[i + 1]),
            [&createTablePromise](
                Error::Ptr&& error, bool success) { createTablePromise.set_value({std::move(error), success}); });
        auto createTableResult = createTablePromise.get_future().get();
        if(std::get<0>(createTableResult)) {
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
        try
        {
 writeHash2Number(header, tableFactory);
               
                    getStorageSetter()->setSysConfig(tableFactory, SYSTEM_KEY_TX_COUNT_LIMIT,
                        boost::lexical_cast<std::string>(_ledgerConfig->blockTxCountLimit()), "0");

                [this, tableFactory, _gasLimit]() {
                    getStorageSetter()->setSysConfig(tableFactory, SYSTEM_KEY_TX_GAS_LIMIT,
                        boost::lexical_cast<std::string>(_gasLimit), "0");
                },
                [this, tableFactory, _ledgerConfig]() {
                    getStorageSetter()->setSysConfig(tableFactory,
                        SYSTEM_KEY_CONSENSUS_LEADER_PERIOD,
                        boost::lexical_cast<std::string>(_ledgerConfig->leaderSwitchPeriod()), "0");
                },
                [this, tableFactory, _ledgerConfig]() {
                    getStorageSetter()->setSysConfig(tableFactory, SYSTEM_KEY_CONSENSUS_TIMEOUT,
                        boost::lexical_cast<std::string>(_ledgerConfig->consensusTimeout()), "0");
                });
            tbb::parallel_invoke(
                [this, tableFactory, _ledgerConfig]() {
                    getStorageSetter()->setConsensusConfig(
                        tableFactory, CONSENSUS_SEALER, _ledgerConfig->consensusNodeList(), "0");
                },
                [this, tableFactory, _ledgerConfig]() {
                    getStorageSetter()->setConsensusConfig(
                        tableFactory, CONSENSUS_OBSERVER, _ledgerConfig->observerNodeList(), "0");
                },
                [this, tableFactory, header]() { writeNumber2BlockHeader(header, tableFactory); },
                [this, tableFactory]() {
                    getStorageSetter()->setCurrentState(tableFactory, SYS_KEY_CURRENT_NUMBER, "0");
                },
                [this, tableFactory]() {
                    getStorageSetter()->setCurrentState(
                        tableFactory, SYS_KEY_TOTAL_TRANSACTION_COUNT, "0");
                },
                [this, tableFactory]() {
                    getStorageSetter()->setCurrentState(
                        tableFactory, SYS_KEY_TOTAL_FAILED_TRANSACTION, "0");
                });
        }
        catch (OpenSysTableFailed const& e)
        {
            LEDGER_LOG(FATAL) << LOG_DESC(
                                     "[#buildGenesisBlock]System meets error when try to
                                     write " "block to storage")
                              << LOG_KV("EINFO", boost::diagnostic_information(e));
            raise(SIGTERM);
            BOOST_THROW_EXCEPTION(
                OpenSysTableFailed() << errinfo_comment(" write block to storage failed."));
        }
    return true;
}

void Ledger::createFileSystemTables(
    const std::string& _groupId)
{
    // create / dir
    _tableFactory->createTable(FS_ROOT, FS_KEY_NAME, FS_FIELD_COMBINED);
    auto table = _tableFactory->openTable(FS_ROOT);
    assert(table);
    auto rootEntry = table->newEntry();
    rootEntry->setField(FS_FIELD_TYPE, FS_TYPE_DIR);
    // TODO: set root default permission?
    rootEntry->setField(FS_FIELD_ACCESS, "");
    rootEntry->setField(FS_FIELD_OWNER, "root");
    rootEntry->setField(FS_FIELD_GID, "/usr");
    rootEntry->setField(FS_FIELD_EXTRA, "");
    table->setRow(FS_ROOT, rootEntry);

    std::string appsDir = "/" + _groupId + FS_APPS;
    std::string tableDir = "/" + _groupId + FS_USER_TABLE;

    recursiveBuildDir(_tableFactory, FS_USER);
    recursiveBuildDir(_tableFactory, FS_SYS_BIN);
    recursiveBuildDir(_tableFactory, appsDir);
    recursiveBuildDir(_tableFactory, tableDir);
}
void Ledger::recursiveBuildDir(
    const std::string& _absoluteDir)
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
        auto table = _tableFactory->openTable(root);
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
        newFileEntry->setField(FS_FIELD_TYPE, FS_TYPE_DIR);
        // FIXME: consider permission inheritance
        newFileEntry->setField(FS_FIELD_ACCESS, "");
        newFileEntry->setField(FS_FIELD_OWNER, "root");
        newFileEntry->setField(FS_FIELD_GID, "/usr");
        newFileEntry->setField(FS_FIELD_EXTRA, "");
        table->setRow(dir, newFileEntry);

        _tableFactory->createTable(root + dir, FS_KEY_NAME, FS_FIELD_COMBINED);
        root += dir;
    }
}