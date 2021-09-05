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
 */

#include "Ledger.h"
#include "interfaces/ledger/LedgerTypeDef.h"
// #include "storage/StorageSetter.h"
#include "interfaces/protocol/ProtocolTypeDef.h"
#include "libutilities/BoostLog.h"
#include "libutilities/Common.h"
#include "libutilities/DataConvertUtility.h"
#include "utilities/BlockUtilities.h"
#include <bcos-framework/interfaces/protocol/CommonError.h>
#include <bcos-framework/interfaces/storage/Table.h>
#include <bcos-framework/libprotocol/ParallelMerkleProof.h>
#include <tbb/parallel_for.h>
#include <tbb/parallel_invoke.h>
#include <boost/exception/diagnostic_information.hpp>
#include <boost/lexical_cast.hpp>
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
                storage->asyncSetRow(table->tableInfo(), (*hashList)[i].hex(), std::move(entry),
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
                        asyncBatchGetTransactions(block, hashesPtr,
                            [finally](Error::Ptr&& error) { finally(std::move(error)); });
                    }
                    if (_blockFlag ^ RECEIPTS)
                    {
                        asyncBatchGetReceipts(block, hashesPtr,
                            [finally](Error::Ptr&& error) { finally(std::move(error)); });
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
    m_storage->asyncOpenTable(SYS_CURRENT_STATE, [this, callback = std::move(_onGetBlock)](
                                                     Error::Ptr&& error,
                                                     std::optional<Table>&& table) {
        auto tableError = checkTableValid(std::move(error), table, SYS_CURRENT_STATE);
        if (tableError)
        {
            callback(std::move(tableError), 0);
            return;
        }

        table->asyncGetRow(SYS_KEY_CURRENT_NUMBER, [this, callback = std::move(callback)](
                                                       Error::Ptr&& error,
                                                       std::optional<Entry>&& entry) {
            auto entryError = checkEntryValid(std::move(error), entry, SYS_KEY_CURRENT_NUMBER);
            if (entryError)
            {
                callback(std::move(entryError), 0);
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
            callback(nullptr, blockNumber);
        });
    });
}

void Ledger::asyncGetBlockHashByNumber(bcos::protocol::BlockNumber _blockNumber,
    std::function<void(Error::Ptr, const bcos::crypto::HashType&)> _onGetBlock)
{
    m_storage->asyncOpenTable(
        SYS_NUMBER_2_HASH, [this, _blockNumber, callback = std::move(_onGetBlock)](
                               Error::Ptr&& error, std::optional<Table>&& table) {
            auto tableError = checkTableValid(std::move(error), table, SYS_NUMBER_2_HASH);
            if (tableError)
            {
                callback(std::move(tableError), bcos::crypto::HashType());
                return;
            }

            auto key = boost::lexical_cast<std::string>(_blockNumber);
            table->asyncGetRow(boost::lexical_cast<std::string>(key),
                [this, key, callback = std::move(callback)](
                    Error::Ptr&& error, std::optional<Entry>&& entry) {
                    auto entryError = checkEntryValid(std::move(error), entry, key);
                    if (entryError)
                    {
                        callback(std::move(entryError), bcos::crypto::HashType());
                        return;
                    }

                    auto hash = entry->getField(SYS_VALUE);
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

void Ledger::asyncBatchGetTransactions(bcos::protocol::Block::Ptr block,
    std::shared_ptr<std::vector<std::string>> hashes, std::function<void(Error::Ptr&&)> callback)
{
    m_storage->asyncOpenTable(SYS_HASH_2_TX,
        [this, hashes, block, callback](Error::Ptr&& error, std::optional<Table>&& table) {
            auto validError = checkTableValid(std::move(error), table, SYS_HASH_2_TX);
            if (validError)
            {
                callback(std::move(validError));
                return;
            }

            table->asyncGetRows(*hashes, [this, hashes, block, callback](Error::Ptr&& error,
                                             std::vector<std::optional<Entry>>&& entries) {
                if (error)
                {
                    LEDGER_LOG(ERROR)
                        << "Batch get transaction error!" << boost::diagnostic_information(*error);
                    callback(BCOS_ERROR_WITH_PREV_PTR(LedgerError::GetStorageError,
                        "Batch get transaction error!", std::move(error)));

                    return;
                }

                size_t i = 0;
                for (auto& entry : entries)
                {
                    if (!entry.has_value())
                    {
                        LEDGER_LOG(ERROR) << "Get transaction error: " << (*hashes)[i];
                        callback(BCOS_ERROR_PTR(
                            LedgerError::GetStorageError, "Batch get transaction error!"));
                        return;
                    }

                    auto field = entry->getField(SYS_VALUE);
                    auto transaction = m_blockFactory->transactionFactory()->createTransaction(
                        bcos::bytesConstRef((bcos::byte*)field.data(), field.size()));
                    block->appendTransaction(std::move(transaction));

                    ++i;
                }

                callback(nullptr);
            });
        });
}

void Ledger::asyncBatchGetReceipts(bcos::protocol::Block::Ptr block,
    std::shared_ptr<std::vector<std::string>> hashes, std::function<void(Error::Ptr&&)> callback)
{
    m_storage->asyncOpenTable(SYS_HASH_2_RECEIPT,
        [this, hashes, block, callback](Error::Ptr&& error, std::optional<Table>&& table) {
            auto validError = checkTableValid(std::move(error), table, SYS_HASH_2_RECEIPT);
            if (validError)
            {
                callback(std::move(validError));
                return;
            }

            table->asyncGetRows(*hashes, [this, hashes, block, callback](Error::Ptr&& error,
                                             std::vector<std::optional<Entry>>&& entries) {
                if (error)
                {
                    LEDGER_LOG(ERROR)
                        << "Batch get receipt error!" << boost::diagnostic_information(*error);
                    callback(BCOS_ERROR_WITH_PREV_PTR(LedgerError::GetStorageError,
                        "Batch get receipt error!", std::move(error)));

                    return;
                }

                size_t i = 0;
                for (auto& entry : entries)
                {
                    if (!entry.has_value())
                    {
                        LEDGER_LOG(ERROR) << "Get receipt error: " << (*hashes)[i];
                        callback(BCOS_ERROR_PTR(
                            LedgerError::GetStorageError, "Batch get transaction error!"));
                        return;
                    }

                    auto field = entry->getField(SYS_VALUE);
                    auto receipt = m_blockFactory->receiptFactory()->createReceipt(
                        bcos::bytesConstRef((bcos::byte*)field.data(), field.size()));
                    block->appendReceipt(std::move(receipt));

                    ++i;
                }

                callback(nullptr);
            });
        });
}

void Ledger::asyncGetSystemTableEntry(const std::string_view& table, const std::string_view& key,
    std::function<void(Error::Ptr&&, std::optional<bcos::storage::Entry>&& entry)> callback)
{
    m_storage->asyncOpenTable(std::string(table), [this, table, callback = std::move(callback)](
                                                      Error::Ptr&& error,
                                                      std::optional<Table>&& table) {
        auto tableError = checkTableValid(std::move(error), table, SYS_CURRENT_STATE);
        if (tableError)
        {
            callback(std::move(tableError), {});
            return;
        }

        table->asyncGetRow(SYS_KEY_CURRENT_NUMBER, [this, callback = std::move(callback)](
                                                       Error::Ptr&& error,
                                                       std::optional<Entry>&& entry) {
            auto entryError = checkEntryValid(std::move(error), entry, SYS_KEY_CURRENT_NUMBER);
            if (entryError)
            {
                callback(std::move(entryError), 0);
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
            callback(nullptr, blockNumber);
        });
    });
}

// void Ledger::asyncGetBlockHashByNumber(bcos::protocol::BlockNumber _blockNumber,
//     std::function<void(Error::Ptr, const bcos::crypto::HashType&)> _onGetBlock)
// {
//     if (_blockNumber < 0)
//     {
//         LEDGER_LOG(ERROR) << LOG_BADGE("asyncGetBlockHashByNumber") << LOG_DESC("Error
//         parameters"); auto error = std::make_shared<Error>(
//             LedgerError::ErrorArgument, "wrong block number, callback empty hash");
//         _onGetBlock(error, HashType());
//         return;
//     }
//     getStorageGetter()->getBlockHashByNumber(_blockNumber, getMemoryTableFactory(0),
//         [_onGetBlock, _blockNumber](Error::Ptr _error, bcos::storage::Entry::Ptr _hashEntry) {
//             if (_error && _error->errorCode() != CommonError::SUCCESS)
//             {
//                 LEDGER_LOG(ERROR) << LOG_BADGE("asyncGetBlockHashByNumber")
//                                   << LOG_DESC("error happened in open table or get entry")
//                                   << LOG_KV("errorCode", _error->errorCode())
//                                   << LOG_KV("errorMsg", _error->errorMessage())
//                                   << LOG_KV("blockNumber", _blockNumber);
//                 _onGetBlock(_error, HashType());
//                 return;
//             }
//             if (!_hashEntry)
//             {
//                 LEDGER_LOG(ERROR) << LOG_BADGE("asyncGetBlockHashByNumber")
//                                   << LOG_DESC("getBlockHashByNumber callback null entry")
//                                   << LOG_KV("blockNumber", _blockNumber);
//                 auto error = std::make_shared<Error>(
//                     LedgerError::GetStorageError, "can not get hash from storage.");
//                 _onGetBlock(error, HashType());
//                 return;
//             }
//             _onGetBlock(nullptr, HashType(std::string(_hashEntry->getField(SYS_VALUE))));
//         });
// }

// void Ledger::asyncGetBlockNumberByHash(const crypto::HashType& _blockHash,
//     std::function<void(Error::Ptr, bcos::protocol::BlockNumber)> _onGetBlock)
// {
//     getStorageGetter()->getBlockNumberByHash(_blockHash.hex(), getMemoryTableFactory(0),
//         [_blockHash, _onGetBlock](Error::Ptr _error, bcos::storage::Entry::Ptr _numberEntry) {
//             if (_error && _error->errorCode() != CommonError::SUCCESS)
//             {
//                 LEDGER_LOG(ERROR) << LOG_BADGE("asyncGetBlockNumberByHash")
//                                   << LOG_DESC("error happened in open table or get entry")
//                                   << LOG_KV("errorCode", _error->errorCode())
//                                   << LOG_KV("errorMsg", _error->errorMessage())
//                                   << LOG_KV("blockHash", _blockHash.hex());
//                 _onGetBlock(_error, -1);
//                 return;
//             }
//             if (!_numberEntry)
//             {
//                 LEDGER_LOG(ERROR) << LOG_BADGE("asyncGetBlockNumberByHash")
//                                   << LOG_DESC("getBlockNumberByHash callback null entry")
//                                   << LOG_KV("blockHash", _blockHash.hex());
//                 auto error = std::make_shared<Error>(
//                     LedgerError::GetStorageError, "can not get number from storage");
//                 _onGetBlock(error, -1);
//                 return;
//             }
//             try
//             {
//                 BlockNumber number =
//                     boost::lexical_cast<BlockNumber>(_numberEntry->getField(SYS_VALUE));
//                 _onGetBlock(nullptr, number);
//             }
//             catch (std::exception const& e)
//             {
//                 LEDGER_LOG(WARNING) << LOG_BADGE("asyncGetBlockNumberByHash")
//                                     << LOG_KV("exception", boost::diagnostic_information(e))
//                                     << LOG_KV("blockHash", _blockHash.hex());
//                 auto error = std::make_shared<Error>(LedgerError::CallbackError,
//                     "asyncGetBlockNumberByHash get a empty number string");
//                 _onGetBlock(error, -1);
//             }
//         });
// }

// void Ledger::asyncGetBatchTxsByHashList(crypto::HashListPtr _txHashList, bool _withProof,
//     std::function<void(Error::Ptr, bcos::protocol::TransactionsPtr,
//         std::shared_ptr<std::map<std::string, MerkleProofPtr>>)>
//         _onGetTx)
// {
//     if (!_txHashList)
//     {
//         LEDGER_LOG(ERROR) << LOG_BADGE("asyncGetBatchTxsByHashList")
//                           << LOG_DESC("Error parameters");
//         auto error = std::make_shared<Error>(LedgerError::ErrorArgument, "nullptr in
//         parameters"); _onGetTx(error, nullptr, nullptr); return;
//     }
//     auto txHashStrList = std::make_shared<std::vector<std::string>>();
//     for (auto& txHash : *_txHashList)
//     {
//         txHashStrList->emplace_back(txHash.hex());
//     }
//     getStorageGetter()->getBatchTxByHashList(txHashStrList, getMemoryTableFactory(0),
//         getTransactionFactory(),
//         [this, _txHashList, _withProof, _onGetTx](Error::Ptr _error, TransactionsPtr _txList) {
//             if (_error && _error->errorCode() != CommonError::SUCCESS)
//             {
//                 LEDGER_LOG(ERROR) << LOG_BADGE("asyncGetBatchTxsByHashList")
//                                   << LOG_DESC("getBatchTxByHashList callback error")
//                                   << LOG_KV("errorCode", _error->errorCode())
//                                   << LOG_KV("errorMsg", _error->errorMessage());
//                 _onGetTx(_error, nullptr, nullptr);
//                 return;
//             }
//             if (_withProof)
//             {
//                 auto con_proofMap =
//                     std::make_shared<tbb::concurrent_unordered_map<std::string,
//                     MerkleProofPtr>>();
//                 auto count = std::make_shared<std::atomic_uint64_t>(0);
//                 auto counter = [_txList, _txHashList, count, con_proofMap, _onGetTx]() {
//                     count->fetch_add(1);
//                     if (count->load() == _txHashList->size())
//                     {
//                         auto proofMap = std::make_shared<std::map<std::string, MerkleProofPtr>>(
//                             con_proofMap->begin(), con_proofMap->end());
//                         LEDGER_LOG(DEBUG) << LOG_BADGE("asyncGetBatchTxsByHashList")
//                                           << LOG_DESC("get tx list and proofMap complete")
//                                           << LOG_KV("txHashListSize", _txHashList->size())
//                                           << LOG_KV("proofMapSize", proofMap->size());
//                         _onGetTx(nullptr, _txList, proofMap);
//                     }
//                 };
//                 tbb::parallel_for(tbb::blocked_range<size_t>(0, _txHashList->size()),
//                     [&, counter](const tbb::blocked_range<size_t>& range) {
//                         for (size_t i = range.begin(); i < range.end(); ++i)
//                         {
//                             auto txHash = _txHashList->at(i);
//                             getTxProof(txHash, [con_proofMap, txHash, counter](
//                                                    Error::Ptr _error, MerkleProofPtr _proof) {
//                                 if (!_error && _proof)
//                                 {
//                                     con_proofMap->insert(std::make_pair(txHash.hex(), _proof));
//                                 }
//                                 counter();
//                             });
//                         }
//                     });
//             }
//             else
//             {
//                 LEDGER_LOG(DEBUG) << LOG_BADGE("asyncGetBatchTxsByHashList")
//                                   << LOG_DESC("get tx list complete")
//                                   << LOG_KV("txHashListSize", _txHashList->size())
//                                   << LOG_KV("withProof", _withProof);
//                 _onGetTx(nullptr, _txList, nullptr);
//             }
//         });
// }

// void Ledger::asyncGetTransactionReceiptByHash(bcos::crypto::HashType const& _txHash,
//     bool _withProof,
//     std::function<void(Error::Ptr, bcos::protocol::TransactionReceipt::ConstPtr, MerkleProofPtr)>
//         _onGetTx)
// {
//     getStorageGetter()->getReceiptByTxHash(_txHash.hex(), getMemoryTableFactory(0),
//         [this, _withProof, _onGetTx, _txHash](
//             Error::Ptr _error, bcos::storage::Entry::Ptr _receiptEntry) {
//             if (_error && _error->errorCode() != CommonError::SUCCESS)
//             {
//                 LEDGER_LOG(ERROR) << LOG_BADGE("asyncGetTransactionReceiptByHash")
//                                   << LOG_DESC("getReceiptByTxHash callback error")
//                                   << LOG_KV("errorCode", _error->errorCode())
//                                   << LOG_KV("errorMsg", _error->errorMessage())
//                                   << LOG_KV("txHash", _txHash.abridged());
//                 _onGetTx(_error, nullptr, nullptr);
//                 return;
//             }
//             if (!_receiptEntry)
//             {
//                 LEDGER_LOG(ERROR) << LOG_BADGE("asyncGetTransactionReceiptByHash")
//                                   << LOG_DESC("getReceiptByTxHash callback null entry")
//                                   << LOG_KV("txHash", _txHash.abridged());
//                 auto error = std::make_shared<Error>(
//                     LedgerError::GetStorageError, "can not get receipt from storage");
//                 _onGetTx(error, nullptr, nullptr);
//                 return;
//             }
//             auto receipt = decodeReceipt(getReceiptFactory(),
//             _receiptEntry->getField(SYS_VALUE)); if (!receipt)
//             {
//                 LEDGER_LOG(ERROR) << LOG_BADGE("asyncGetTransactionReceiptByHash")
//                                   << LOG_DESC("receipt is null or empty")
//                                   << LOG_KV("txHash", _txHash.abridged());
//                 auto error = std::make_shared<Error>(
//                     LedgerError::DecodeError, "getReceiptByTxHash callback wrong receipt");
//                 _onGetTx(error, nullptr, nullptr);
//                 return;
//             }
//             if (_withProof)
//             {
//                 getReceiptProof(
//                     receipt, [receipt, _onGetTx](Error::Ptr _error, MerkleProofPtr _proof) {
//                         if (_error && _error->errorCode() != CommonError::SUCCESS)
//                         {
//                             LEDGER_LOG(ERROR) << LOG_BADGE("asyncGetTransactionReceiptByHash")
//                                               << LOG_DESC("getReceiptProof callback error")
//                                               << LOG_KV("errorCode", _error->errorCode())
//                                               << LOG_KV("errorMsg", _error->errorMessage());
//                             _onGetTx(_error, receipt, nullptr);
//                             return;
//                         }
//                         // proof wont be nullptr
//                         _onGetTx(nullptr, receipt, _proof);
//                     });
//             }
//             else
//             {
//                 LEDGER_LOG(DEBUG) << LOG_BADGE("asyncGetTransactionReceiptByHash")
//                                   << LOG_DESC("call back receipt")
//                                   << LOG_KV("txHash", _txHash.abridged());
//                 _onGetTx(nullptr, receipt, nullptr);
//             }
//         });
// }

// void Ledger::asyncGetTotalTransactionCount(
//     std::function<void(Error::Ptr, int64_t, int64_t, bcos::protocol::BlockNumber)> _callback)
// {
//     auto self = std::weak_ptr<Ledger>(std::dynamic_pointer_cast<Ledger>(shared_from_this()));
//     auto tableFactory = getMemoryTableFactory(0);
//     getStorageGetter()->getCurrentState(SYS_KEY_TOTAL_TRANSACTION_COUNT, tableFactory,
//         [self, _callback, tableFactory](
//             Error::Ptr _error, bcos::storage::Entry::Ptr _totalCountEntry) {
//             auto ledger = self.lock();
//             if (!ledger)
//             {
//                 auto error = std::make_shared<Error>(
//                     LedgerError::LedgerLockError, "can't not get ledger weak_ptr");
//                 _callback(error, -1, -1, -1);
//                 return;
//             }
//             if (!_error || _error->errorCode() == CommonError::SUCCESS)
//             {
//                 // entry must exist
//                 auto totalStr = _totalCountEntry->getField(SYS_VALUE);
//                 int64_t totalCount = totalStr.empty() ? -1 :
//                 boost::lexical_cast<int64_t>(totalStr);
//                 ledger->getStorageGetter()->getCurrentState(SYS_KEY_TOTAL_FAILED_TRANSACTION,
//                     tableFactory,
//                     [totalCount, ledger, _callback, tableFactory](
//                         Error::Ptr _error, bcos::storage::Entry::Ptr _totalFailedEntry) {
//                         if (!_error || _error->errorCode() == CommonError::SUCCESS)
//                         {
//                             // entry must exist
//                             auto totalFailedStr = _totalFailedEntry->getField(SYS_VALUE);
//                             auto totalFailed = totalFailedStr.empty() ?
//                                                    -1 :
//                                                    boost::lexical_cast<int64_t>(totalFailedStr);
//                             ledger->getLatestBlockNumber(
//                                 [totalCount, totalFailed, _callback](BlockNumber _number) {
//                                     if (totalCount == -1 || totalFailed == -1 || _number == -1)
//                                     {
//                                         LEDGER_LOG(ERROR)
//                                             << LOG_BADGE("asyncGetTransactionReceiptByHash")
//                                             << LOG_DESC("error happened in get total tx count");
//                                         auto error = std::make_shared<Error>(
//                                             LedgerError::CollectAsyncCallbackError,
//                                             "can't not fetch all data in "
//                                             "asyncGetTotalTransactionCount");
//                                         _callback(error, -1, -1, -1);
//                                         return;
//                                     }
//                                     _callback(nullptr, totalCount, totalFailed, _number);
//                                 });
//                             return;
//                         }
//                         LEDGER_LOG(ERROR)
//                             << LOG_BADGE("asyncGetTransactionReceiptByHash")
//                             << LOG_DESC("error happened in get
//                             SYS_KEY_TOTAL_FAILED_TRANSACTION");
//                         auto error =
//                         std::make_shared<Error>(LedgerError::CollectAsyncCallbackError,
//                             "can't not fetch SYS_KEY_TOTAL_FAILED_TRANSACTION in "
//                             "asyncGetTotalTransactionCount");
//                         _callback(error, -1, -1, -1);
//                     });
//                 return;
//             }
//             LEDGER_LOG(ERROR) << LOG_BADGE("asyncGetTransactionReceiptByHash")
//                               << LOG_DESC("error happened in get
//                               SYS_KEY_TOTAL_TRANSACTION_COUNT");
//             auto error = std::make_shared<Error>(LedgerError::CollectAsyncCallbackError,
//                 "can't not fetch SYS_KEY_TOTAL_TRANSACTION_COUNT in "
//                 "asyncGetTotalTransactionCount");
//             _callback(error, -1, -1, -1);
//         });
// }

// void Ledger::asyncGetSystemConfigByKey(const std::string& _key,
//     std::function<void(Error::Ptr, std::string, bcos::protocol::BlockNumber)> _onGetConfig)
// {
//     auto self = std::weak_ptr<Ledger>(std::dynamic_pointer_cast<Ledger>(shared_from_this()));
//     getLatestBlockNumber([self, _key, _onGetConfig](BlockNumber _number) {
//         auto ledger = self.lock();
//         if (!ledger)
//         {
//             auto error = std::make_shared<Error>(
//                 LedgerError::LedgerLockError, "can't not get ledger weak_ptr");
//             _onGetConfig(error, "", -1);
//             return;
//         }
//         ledger->getStorageGetter()->getSysConfig(_key, ledger->getMemoryTableFactory(0),
//             [_number, _key, _onGetConfig](
//                 Error::Ptr _error, bcos::storage::Entry::Ptr _configEntry) {
//                 if (_error && _error->errorCode() != CommonError::SUCCESS)
//                 {
//                     LEDGER_LOG(ERROR) << LOG_BADGE("asyncGetSystemConfigByKey")
//                                       << LOG_DESC("getSysConfig callback error")
//                                       << LOG_KV("errorCode", _error->errorCode())
//                                       << LOG_KV("errorMsg", _error->errorMessage());
//                     _onGetConfig(_error, "", -1);
//                     return;
//                 }
//                 // The param was reset at height getLatestBlockNumber(), and takes effect in
//                 // next block. So we query the status of getLatestBlockNumber() + 1.
//                 auto number = _number + 1;
//                 if (!_configEntry)
//                 {
//                     LEDGER_LOG(ERROR)
//                         << LOG_BADGE("asyncGetSystemConfigByKey")
//                         << LOG_DESC("getSysConfig callback null entry") << LOG_KV("key", _key);
//                     auto error = std::make_shared<Error>(
//                         LedgerError::GetStorageError, "can not get config from storage");
//                     _onGetConfig(error, "", -1);
//                     return;
//                 }
//                 try
//                 {
//                     auto value = _configEntry->getField(SYS_VALUE);
//                     auto numberStr = _configEntry->getField(SYS_CONFIG_ENABLE_BLOCK_NUMBER);
//                     BlockNumber enableNumber =
//                         numberStr.empty() ? -1 : boost::lexical_cast<BlockNumber>(numberStr);
//                     if (enableNumber <= number)
//                     {
//                         _onGetConfig(nullptr, std::string(value), enableNumber);
//                     }
//                     else
//                     {
//                         LEDGER_LOG(WARNING)
//                             << LOG_BADGE("asyncGetSystemConfigByKey")
//                             << LOG_DESC("config not enable in latest number")
//                             << LOG_KV("enableNum", enableNumber) << LOG_KV("latestNum", _number);
//                         auto error = std::make_shared<Error>(LedgerError::GetStorageError,
//                             "enable number larger than latest number");
//                         _onGetConfig(error, "", -1);
//                     }
//                 }
//                 catch (std::exception const& e)
//                 {
//                     LEDGER_LOG(ERROR) << LOG_BADGE("asyncGetSystemConfigByKey")
//                                       << LOG_KV("exception", boost::diagnostic_information(e));
//                     auto error = std::make_shared<Error>(
//                         LedgerError::GetStorageError, boost::diagnostic_information(e));
//                     _onGetConfig(error, "", -1);
//                 }
//             });
//     });
// }

// void Ledger::asyncGetNonceList(bcos::protocol::BlockNumber _startNumber, int64_t _offset,
//     std::function<void(
//         Error::Ptr, std::shared_ptr<std::map<protocol::BlockNumber, protocol::NonceListPtr>>)>
//         _onGetList)
// {
//     if (_startNumber < 0 || _offset < 0)
//     {
//         LEDGER_LOG(ERROR) << LOG_BADGE("asyncGetNonceList") << LOG_DESC("Error parameters");
//         auto error = std::make_shared<Error>(LedgerError::ErrorArgument, "error parameter");
//         _onGetList(error, nullptr);
//         return;
//     }
//     getStorageGetter()->getNoncesBatchFromStorage(_startNumber, _startNumber + _offset,
//         getMemoryTableFactory(0), m_blockFactory,
//         [_onGetList](Error::Ptr _error,
//             std::shared_ptr<std::map<protocol::BlockNumber, protocol::NonceListPtr>> _nonceMap) {
//             if (!_error || _error->errorCode() == CommonError::SUCCESS)
//             {
//                 LEDGER_LOG(DEBUG) << LOG_BADGE("asyncGetNonceList")
//                                   << LOG_KV("nonceMapSize", _nonceMap->size());
//                 // nonceMap wont be nullptr
//                 _onGetList(nullptr, _nonceMap);
//             }
//             else
//             {
//                 LEDGER_LOG(ERROR) << LOG_BADGE("asyncGetNonceList")
//                                   << LOG_DESC("error happened in open table or get entry");
//                 _onGetList(_error, nullptr);
//             }
//         });
// }

// void Ledger::asyncGetNodeListByType(const std::string& _type,
//     std::function<void(Error::Ptr, consensus::ConsensusNodeListPtr)> _onGetConfig)
// {
//     auto self = std::weak_ptr<Ledger>(std::dynamic_pointer_cast<Ledger>(shared_from_this()));
//     getLatestBlockNumber([self, _type, _onGetConfig](BlockNumber _number) {
//         auto ledger = self.lock();
//         if (!ledger)
//         {
//             auto error = std::make_shared<Error>(
//                 LedgerError::LedgerLockError, "can't not get ledger weak_ptr");
//             _onGetConfig(error, nullptr);
//             return;
//         }
//         // The param was reset at height getLatestBlockNumber(), and takes effect in next
//         // block. So we query the status of getLatestBlockNumber() + 1.
//         auto number = _number + 1;
//         ledger->getStorageGetter()->asyncGetConsensusConfig(_type, number,
//             ledger->getMemoryTableFactory(0),
//             ledger->m_blockFactory->cryptoSuite()->keyFactory(),
//             [_onGetConfig](Error::Ptr _error, consensus::ConsensusNodeListPtr _nodeList) {
//                 if (!_error || _error->errorCode() == CommonError::SUCCESS)
//                 {
//                     LEDGER_LOG(DEBUG) << LOG_BADGE("asyncGetNodeListByType")
//                                       << LOG_KV("nodeListSize", _nodeList->size());
//                     // nodeList wont be nullptr
//                     _onGetConfig(nullptr, _nodeList);
//                 }
//                 else
//                 {
//                     LEDGER_LOG(ERROR) << LOG_BADGE("asyncGetNodeListByType")
//                                       << LOG_DESC("error happened in open table or get entry")
//                                       << LOG_KV("errorCode", _error->errorCode())
//                                       << LOG_KV("errorMsg", _error->errorMessage());
//                     _onGetConfig(_error, nullptr);
//                 }
//             });
//     });
// }

// void Ledger::getBlock(const BlockNumber& _blockNumber, int32_t _blockFlag,
//     std::function<void(Error::Ptr, BlockFetcher::Ptr)> _onGetBlock)
// {
//     auto cachedBlock = m_blockCache.get(_blockNumber);
//     if (cachedBlock.second)
//     {
//         LEDGER_LOG(DEBUG) << LOG_BADGE("getBlock") << LOG_DESC("Cache hit, read from cache")
//                           << LOG_KV("blockNumber", _blockNumber);
//         auto blockFetcher = std::make_shared<BlockFetcher>(cachedBlock.second);
//         _onGetBlock(nullptr, blockFetcher);
//         return;
//     }
//     LEDGER_LOG(DEBUG) << LOG_BADGE("getBlock") << LOG_DESC("Cache missed, read from storage")
//                       << LOG_KV("blockNumber", _blockNumber);
//     auto block = m_blockFactory->createBlock();
//     auto blockFetcher = std::make_shared<BlockFetcher>(block);

//     if (_blockFlag & HEADER)
//     {
//         getBlockHeader(_blockNumber, [blockFetcher, _onGetBlock, _blockNumber, block](
//                                          Error::Ptr _error, BlockHeader::Ptr _header) {
//             if (!_error || _error->errorCode() == CommonError::SUCCESS)
//             {
//                 /// should handle nullptr header
//                 if (!_header)
//                 {
//                     auto error = std::make_shared<Error>(
//                         LedgerError::CallbackError, "getBlockHeader callback nullptr");
//                     _onGetBlock(error, nullptr);
//                     return;
//                 }
//                 LEDGER_LOG(DEBUG) << LOG_BADGE("getBlock") << LOG_DESC("get block header
//                 success")
//                                   << LOG_KV("headerNumber", _header->number())
//                                   << LOG_KV("headerHash", _header->hash().hex());
//                 block->setBlockHeader(_header);
//                 blockFetcher->setHeaderFetched(true);
//                 _onGetBlock(nullptr, blockFetcher);
//                 return;
//             }
//             LEDGER_LOG(ERROR) << LOG_BADGE("getBlock")
//                               << LOG_DESC("Can't find the header, callback error")
//                               << LOG_KV("errorCode", _error->errorCode())
//                               << LOG_KV("errorMsg", _error->errorMessage())
//                               << LOG_KV("blockNumber", _blockNumber);
//             _onGetBlock(_error, nullptr);
//         });
//     }
//     if (_blockFlag & TRANSACTIONS)
//     {
//         getTxs(_blockNumber, [_blockNumber, blockFetcher, _onGetBlock, block](
//                                  Error::Ptr _error, bcos::protocol::TransactionsPtr _txs) {
//             if ((!_error || _error->errorCode() == CommonError::SUCCESS))
//             {
//                 /// not handle nullptr txs
//                 auto insertSize = blockTransactionListSetter(block, _txs);
//                 LEDGER_LOG(DEBUG) << LOG_BADGE("getBlock") << LOG_DESC("get block transactions")
//                                   << LOG_KV("txSize", insertSize)
//                                   << LOG_KV("blockNumber", _blockNumber);
//                 blockFetcher->setTxsFetched(true);
//                 _onGetBlock(nullptr, blockFetcher);
//                 return;
//             }
//             LEDGER_LOG(ERROR) << LOG_BADGE("getBlock")
//                               << LOG_DESC("Can't find the Txs, callback error")
//                               << LOG_KV("errorCode", _error->errorCode())
//                               << LOG_KV("errorMsg", _error->errorMessage())
//                               << LOG_KV("blockNumber", _blockNumber);
//             _onGetBlock(_error, nullptr);
//         });
//     }
//     if (_blockFlag & RECEIPTS)
//     {
//         getReceipts(_blockNumber, [_blockNumber, blockFetcher, _onGetBlock, block](
//                                       Error::Ptr _error, protocol::ReceiptsPtr _receipts) {
//             if ((!_error || _error->errorCode() == CommonError::SUCCESS))
//             {
//                 /// not handle nullptr _receipts
//                 auto insertSize = blockReceiptListSetter(block, _receipts);
//                 LEDGER_LOG(DEBUG) << LOG_BADGE("getBlock") << LOG_DESC("get block receipts")
//                                   << LOG_KV("receiptsSize", insertSize)
//                                   << LOG_KV("blockNumber", _blockNumber);
//                 blockFetcher->setReceiptsFetched(true);
//                 _onGetBlock(nullptr, blockFetcher);
//                 return;
//             }
//             LEDGER_LOG(ERROR) << LOG_BADGE("getBlock") << LOG_DESC("Can't find the Receipts")
//                               << LOG_KV("errorCode", _error->errorCode())
//                               << LOG_KV("errorMsg", _error->errorMessage())
//                               << LOG_KV("blockNumber", _blockNumber);
//             _onGetBlock(_error, nullptr);
//         });
//     }
// }

// void Ledger::getLatestBlockNumber(std::function<void(protocol::BlockNumber)> _onGetNumber)
// {
//     if (m_blockNumber != -1)
//     {
//         LEDGER_LOG(TRACE) << LOG_BADGE("getLatestBlockNumber") << LOG_DESC("blockNumber cache
//         hit")
//                           << LOG_KV("number", m_blockNumber);
//         _onGetNumber(m_blockNumber);
//         return;
//     }
//     else
//     {
//         auto self = std::weak_ptr<Ledger>(std::dynamic_pointer_cast<Ledger>(shared_from_this()));
//         getStorageGetter()->getCurrentState(SYS_KEY_CURRENT_NUMBER, getMemoryTableFactory(0),
//             [self, _onGetNumber](Error::Ptr _error, bcos::storage::Entry::Ptr _numberEntry) {
//                 auto ledger = self.lock();
//                 if (!ledger || !_numberEntry)
//                 {
//                     _onGetNumber(-1);
//                     return;
//                 }
//                 if (!_error)
//                 {
//                     try
//                     {
//                         // number entry must exist
//                         auto numberStr = _numberEntry->getField(SYS_VALUE);
//                         BlockNumber number =
//                             numberStr.empty() ? -1 : boost::lexical_cast<BlockNumber>(numberStr);
//                         _onGetNumber(number);
//                         return;
//                     }
//                     catch (std::exception const& e)
//                     {
//                         LEDGER_LOG(ERROR) << LOG_BADGE("getLatestBlockNumber")
//                                           << LOG_KV("exception",
//                                           boost::diagnostic_information(e));
//                         _onGetNumber(-1);
//                     }
//                 }
//                 LEDGER_LOG(ERROR) << LOG_BADGE("getLatestBlockNumber")
//                                   << LOG_DESC("Get number from storage error")
//                                   << LOG_KV("errorCode", _error->errorCode())
//                                   << LOG_KV("errorMsg", _error->errorMessage());
//                 _onGetNumber(-1);
//             });
//     }
// }

// void Ledger::getLatestBlockHash(
//     protocol::BlockNumber _number, std::function<void(std::string_view)> _onGetHash)
// {
//     if (m_blockHash != HashType())
//     {
//         LEDGER_LOG(TRACE) << LOG_BADGE("getLatestBlockHash") << LOG_DESC("blockHash cache hit")
//                           << LOG_KV("hash", m_blockHash.hex());
//         _onGetHash(m_blockHash.hex());
//         return;
//     }
//     else
//     {
//         getStorageGetter()->getBlockHashByNumber(_number, getMemoryTableFactory(0),
//             [_number, _onGetHash](Error::Ptr _error, bcos::storage::Entry::Ptr _hashEntry) {
//                 if (!_error || _error->errorCode() == CommonError::SUCCESS)
//                 {
//                     if (!_hashEntry)
//                     {
//                         LEDGER_LOG(ERROR)
//                             << LOG_BADGE("getLatestBlockHash")
//                             << LOG_DESC("storage callback null entry") << LOG_KV("number",
//                             _number);
//                         _onGetHash("");
//                         return;
//                     }
//                     _onGetHash(_hashEntry->getField(SYS_VALUE));
//                     return;
//                 }
//                 LEDGER_LOG(ERROR) << LOG_BADGE("getLatestBlockHash")
//                                   << LOG_DESC("Get block hash error") << LOG_KV("number",
//                                   _number);
//                 _onGetHash("");
//             });
//     }
// }

// void Ledger::getBlockHeader(const bcos::protocol::BlockNumber& _blockNumber,
//     std::function<void(Error::Ptr, BlockHeader::Ptr)> _onGetHeader)
// {
//     auto cachedHeader = m_blockHeaderCache.get(_blockNumber);

//     if (cachedHeader.second)
//     {
//         LEDGER_LOG(DEBUG) << LOG_BADGE("getBlockHeader")
//                           << LOG_DESC("CacheHeader hit, read from cache")
//                           << LOG_KV("blockNumber", _blockNumber);
//         _onGetHeader(nullptr, cachedHeader.second);
//         return;
//     }
//     LEDGER_LOG(DEBUG) << LOG_BADGE("getBlockHeader") << LOG_DESC("Cache missed, read from
//     storage")
//                       << LOG_KV("blockNumber", _blockNumber);
//     getStorageGetter()->getBlockHeaderFromStorage(_blockNumber, getMemoryTableFactory(0),
//         [this, _onGetHeader, _blockNumber](
//             Error::Ptr _error, bcos::storage::Entry::Ptr _headerEntry) {
//             if (_error && _error->errorCode() != CommonError::SUCCESS)
//             {
//                 LEDGER_LOG(ERROR) << LOG_BADGE("getBlockHeader")
//                                   << LOG_DESC("Get header from storage error")
//                                   << LOG_KV("errorCode", _error->errorCode())
//                                   << LOG_KV("errorMsg", _error->errorMessage())
//                                   << LOG_KV("blockNumber", _blockNumber);
//                 _onGetHeader(_error, nullptr);
//                 return;
//             }
//             if (!_headerEntry)
//             {
//                 LEDGER_LOG(ERROR) << LOG_BADGE("getBlockHeader")
//                                   << LOG_DESC("Get header from storage callback null entry")
//                                   << LOG_KV("blockNumber", _blockNumber);
//                 _onGetHeader(nullptr, nullptr);
//                 return;
//             }
//             auto headerPtr =
//                 decodeBlockHeader(getBlockHeaderFactory(), _headerEntry->getField(SYS_VALUE));
//             LEDGER_LOG(DEBUG) << LOG_BADGE("getBlockHeader") << LOG_DESC("Get header from
//             storage")
//                               << LOG_KV("blockNumber", _blockNumber);
//             if (headerPtr)
//             {
//                 m_blockHeaderCache.add(_blockNumber, headerPtr);
//             }
//             _onGetHeader(nullptr, headerPtr);
//         });
// }

// void Ledger::getTxs(const bcos::protocol::BlockNumber& _blockNumber,
//     std::function<void(Error::Ptr, bcos::protocol::TransactionsPtr)> _onGetTxs)
// {
//     auto cachedTransactions = m_transactionsCache.get(_blockNumber);
//     if (cachedTransactions.second)
//     {
//         LEDGER_LOG(DEBUG) << LOG_BADGE("getTxs") << LOG_DESC("CacheTxs hit, read from cache")
//                           << LOG_KV("blockNumber", _blockNumber);
//         _onGetTxs(nullptr, cachedTransactions.second);
//         return;
//     }
//     LEDGER_LOG(DEBUG) << LOG_BADGE("getTxs") << LOG_DESC("Cache missed, read from storage")
//                       << LOG_KV("blockNumber", _blockNumber);
//     // block with tx hash list
//     getStorageGetter()->getTxsFromStorage(_blockNumber, getMemoryTableFactory(0),
//         [this, _onGetTxs, _blockNumber](Error::Ptr _error, bcos::storage::Entry::Ptr _blockEntry)
//         {
//             if (_error && _error->errorCode() != CommonError::SUCCESS)
//             {
//                 LEDGER_LOG(ERROR) << LOG_BADGE("getTxs")
//                                   << LOG_DESC("Get txHashList from storage error")
//                                   << LOG_KV("errorCode", _error->errorCode())
//                                   << LOG_KV("errorMsg", _error->errorMessage())
//                                   << LOG_KV("blockNumber", _blockNumber);
//                 _onGetTxs(_error, nullptr);
//                 return;
//             }
//             if (!_blockEntry)
//             {
//                 LEDGER_LOG(ERROR) << LOG_BADGE("getTxs")
//                                   << LOG_DESC("Get txHashList from storage callback null entry")
//                                   << LOG_KV("blockNumber", _blockNumber);
//                 _onGetTxs(nullptr, nullptr);
//                 return;
//             }
//             auto block = decodeBlock(m_blockFactory, _blockEntry->getField(SYS_VALUE));
//             if (!block)
//             {
//                 LEDGER_LOG(ERROR) << LOG_BADGE("getTxs")
//                                   << LOG_DESC("getTxsFromStorage get error block")
//                                   << LOG_KV("blockNumber", _blockNumber);
//                 auto error = std::make_shared<Error>(
//                     LedgerError::DecodeError, "getTxsFromStorage get error block");
//                 _onGetTxs(error, nullptr);
//                 return;
//             }
//             auto txHashList = blockTxHashListGetter(block);
//             getStorageGetter()->getBatchTxByHashList(txHashList, getMemoryTableFactory(0),
//                 getTransactionFactory(),
//                 [this, _onGetTxs, _blockNumber](Error::Ptr _error, protocol::TransactionsPtr
//                 _txs) {
//                     if (_error && _error->errorCode() != CommonError::SUCCESS)
//                     {
//                         LEDGER_LOG(ERROR)
//                             << LOG_BADGE("getTxs") << LOG_DESC("Get txs from storage error")
//                             << LOG_KV("errorCode", _error->errorCode())
//                             << LOG_KV("errorMsg", _error->errorMessage())
//                             << LOG_KV("txsSize", _txs->size());
//                         _onGetTxs(_error, nullptr);
//                         return;
//                     }
//                     LEDGER_LOG(INFO) << LOG_BADGE("getTxs") << LOG_DESC("Get txs from storage");
//                     if (_txs && !_txs->empty())
//                     {
//                         m_transactionsCache.add(_blockNumber, _txs);
//                     }
//                     _onGetTxs(nullptr, _txs);
//                 });
//         });
// }

// void Ledger::getReceipts(const bcos::protocol::BlockNumber& _blockNumber,
//     std::function<void(Error::Ptr, bcos::protocol::ReceiptsPtr)> _onGetReceipts)
// {
//     auto cachedReceipts = m_receiptCache.get(_blockNumber);
//     if (bool(cachedReceipts.second))
//     {
//         LEDGER_LOG(DEBUG) << LOG_BADGE("getReceipts")
//                           << LOG_DESC("Cache Receipts hit, read from cache")
//                           << LOG_KV("blockNumber", _blockNumber);
//         _onGetReceipts(nullptr, cachedReceipts.second);
//         return;
//     }
//     LEDGER_LOG(DEBUG) << LOG_BADGE("getReceipts") << LOG_DESC("Cache missed, read from storage")
//                       << LOG_KV("blockNumber", _blockNumber);
//     // block with tx hash list
//     getStorageGetter()->getTxsFromStorage(_blockNumber, getMemoryTableFactory(0),
//         [this, _onGetReceipts, _blockNumber](
//             Error::Ptr _error, bcos::storage::Entry::Ptr _blockEntry) {
//             if (_error && _error->errorCode() != CommonError::SUCCESS)
//             {
//                 LEDGER_LOG(ERROR) << LOG_BADGE("getReceipts")
//                                   << LOG_DESC("Get receipts from storage error")
//                                   << LOG_KV("errorCode", _error->errorCode())
//                                   << LOG_KV("errorMsg", _error->errorMessage())
//                                   << LOG_KV("blockNumber", _blockNumber);
//                 _onGetReceipts(_error, nullptr);
//                 return;
//             }
//             if (!_blockEntry)
//             {
//                 LEDGER_LOG(ERROR) << LOG_BADGE("getReceipts")
//                                   << LOG_DESC("Get receipts from storage callback null entry")
//                                   << LOG_KV("blockNumber", _blockNumber);
//                 _onGetReceipts(nullptr, nullptr);
//                 return;
//             }
//             auto block = decodeBlock(m_blockFactory, _blockEntry->getField(SYS_VALUE));
//             if (!block)
//             {
//                 LEDGER_LOG(ERROR) << LOG_BADGE("getReceipts")
//                                   << LOG_DESC("getTxsFromStorage get txHashList error")
//                                   << LOG_KV("blockNumber", _blockNumber);
//                 auto error = std::make_shared<Error>(
//                     LedgerError::DecodeError, "getTxsFromStorage get empty block");
//                 _onGetReceipts(error, nullptr);
//                 return;
//             }

//             auto txHashList = blockTxHashListGetter(block);
//             getStorageGetter()->getBatchReceiptsByHashList(txHashList, getMemoryTableFactory(0),
//                 getReceiptFactory(), [=](Error::Ptr _error, ReceiptsPtr _receipts) {
//                     if (_error && _error->errorCode() != CommonError::SUCCESS)
//                     {
//                         LEDGER_LOG(ERROR) << LOG_BADGE("getReceipts")
//                                           << LOG_DESC("Get receipts from storage error")
//                                           << LOG_KV("errorCode", _error->errorCode())
//                                           << LOG_KV("errorMsg", _error->errorMessage())
//                                           << LOG_KV("blockNumber", _blockNumber);
//                         _onGetReceipts(_error, nullptr);
//                         return;
//                     }
//                     LEDGER_LOG(INFO)
//                         << LOG_BADGE("getReceipts") << LOG_DESC("Get receipts from storage");
//                     if (_receipts && _receipts->size() > 0)
//                         m_receiptCache.add(_blockNumber, _receipts);
//                     _onGetReceipts(nullptr, _receipts);
//                 });
//         });
// }

// void Ledger::getTxProof(
//     const HashType& _txHash, std::function<void(Error::Ptr, MerkleProofPtr)> _onGetProof)
// {
//     // get receipt to get block number
//     getStorageGetter()->getReceiptByTxHash(_txHash.hex(), getMemoryTableFactory(0),
//         [this, _txHash, _onGetProof](Error::Ptr _error, bcos::storage::Entry::Ptr _receiptEntry)
//         {
//             if (_error && _error->errorCode() != CommonError::SUCCESS)
//             {
//                 LEDGER_LOG(ERROR) << LOG_BADGE("getTxProof")
//                                   << LOG_DESC("getReceiptByTxHash from storage error")
//                                   << LOG_KV("errorCode", _error->errorCode())
//                                   << LOG_KV("errorMsg", _error->errorMessage())
//                                   << LOG_KV("txHash", _txHash.hex());
//                 _onGetProof(_error, nullptr);
//                 return;
//             }
//             if (!_receiptEntry)
//             {
//                 LEDGER_LOG(ERROR) << LOG_BADGE("getTxProof")
//                                   << LOG_DESC("getReceiptByTxHash from storage callback null
//                                   entry")
//                                   << LOG_KV("txHash", _txHash.hex());
//                 _onGetProof(nullptr, nullptr);
//                 return;
//             }
//             auto receipt = decodeReceipt(getReceiptFactory(),
//             _receiptEntry->getField(SYS_VALUE)); if (!receipt)
//             {
//                 LEDGER_LOG(ERROR) << LOG_BADGE("getTxProof") << LOG_DESC("receipt is null or
//                 empty")
//                                   << LOG_KV("txHash", _txHash.hex());
//                 auto error = std::make_shared<Error>(
//                     LedgerError::DecodeError, "getReceiptByTxHash callback empty receipt");
//                 _onGetProof(error, nullptr);
//                 return;
//             }
//             auto blockNumber = receipt->blockNumber();
//             getTxs(blockNumber, [this, blockNumber, _onGetProof, _txHash](
//                                     Error::Ptr _error, TransactionsPtr _txs) {
//                 if (_error && _error->errorCode() != CommonError::SUCCESS)
//                 {
//                     LEDGER_LOG(ERROR)
//                         << LOG_BADGE("getTxProof") << LOG_DESC("getTxs callback error")
//                         << LOG_KV("errorCode", _error->errorCode())
//                         << LOG_KV("errorMsg", _error->errorMessage());
//                     _onGetProof(_error, nullptr);
//                     return;
//                 }
//                 if (!_txs)
//                 {
//                     LEDGER_LOG(ERROR)
//                         << LOG_BADGE("getTxProof") << LOG_DESC("get txs error")
//                         << LOG_KV("blockNumber", blockNumber) << LOG_KV("txHash", _txHash.hex());
//                     auto error = std::make_shared<Error>(
//                         LedgerError::CallbackError, "getTxs callback empty txs");
//                     _onGetProof(error, nullptr);
//                     return;
//                 }
//                 auto merkleProofPtr = std::make_shared<MerkleProof>();
//                 auto parent2ChildList = m_merkleProofUtility->getParent2ChildListByTxsProofCache(
//                     blockNumber, _txs, m_blockFactory->cryptoSuite());
//                 auto child2Parent = m_merkleProofUtility->getChild2ParentCacheByTransaction(
//                     parent2ChildList, blockNumber);
//                 m_merkleProofUtility->getMerkleProof(
//                     _txHash, *parent2ChildList, *child2Parent, *merkleProofPtr);
//                 LEDGER_LOG(TRACE) << LOG_BADGE("getTxProof") << LOG_DESC("get merkle proof
//                 success")
//                                   << LOG_KV("blockNumber", blockNumber)
//                                   << LOG_KV("txHash", _txHash.hex());
//                 _onGetProof(nullptr, merkleProofPtr);
//             });
//         });
// }

// void Ledger::getReceiptProof(protocol::TransactionReceipt::Ptr _receipt,
//     std::function<void(Error::Ptr, MerkleProofPtr)> _onGetProof)
// {
//     if (!_receipt)
//     {
//         _onGetProof(nullptr, nullptr);
//         return;
//     }
//     getStorageGetter()->getTxsFromStorage(_receipt->blockNumber(), getMemoryTableFactory(0),
//         [this, _onGetProof, _receipt](Error::Ptr _error, bcos::storage::Entry::Ptr _blockEntry) {
//             if (_error && _error->errorCode() != CommonError::SUCCESS)
//             {
//                 LEDGER_LOG(ERROR) << LOG_BADGE("getReceiptProof")
//                                   << LOG_DESC("getTxsFromStorage callback error")
//                                   << LOG_KV("errorCode", _error->errorCode())
//                                   << LOG_KV("errorMsg", _error->errorMessage());
//                 _onGetProof(_error, nullptr);
//                 return;
//             }
//             if (!_blockEntry)
//             {
//                 LEDGER_LOG(ERROR) << LOG_BADGE("getReceiptProof")
//                                   << LOG_DESC("getTxsFromStorage callback null entry")
//                                   << LOG_KV("blockNumber", _receipt->blockNumber());
//                 _onGetProof(nullptr, nullptr);
//                 return;
//             }
//             auto block = decodeBlock(m_blockFactory, _blockEntry->getField(SYS_VALUE));
//             if (!block)
//             {
//                 LEDGER_LOG(ERROR) << LOG_BADGE("getReceiptProof")
//                                   << LOG_DESC("getTxsFromStorage callback empty block txs");
//                 auto error = std::make_shared<Error>(LedgerError::DecodeError, "empty txs");
//                 _onGetProof(error, nullptr);
//                 return;
//             }
//             auto txHashList = blockTxHashListGetter(block);
//             getStorageGetter()->getBatchReceiptsByHashList(txHashList, getMemoryTableFactory(0),
//                 getReceiptFactory(),
//                 [this, _onGetProof, _receipt](Error::Ptr _error, ReceiptsPtr receipts) {
//                     if (_error && _error->errorCode() != CommonError::SUCCESS)
//                     {
//                         LEDGER_LOG(ERROR) << LOG_BADGE("getReceiptProof")
//                                           << LOG_DESC("getBatchReceiptsByHashList callback
//                                           error")
//                                           << LOG_KV("errorCode", _error->errorCode())
//                                           << LOG_KV("errorMsg", _error->errorMessage());
//                         _onGetProof(_error, nullptr);
//                         return;
//                     }
//                     if (!receipts || receipts->empty())
//                     {
//                         LEDGER_LOG(ERROR)
//                             << LOG_BADGE("getReceiptProof")
//                             << LOG_DESC("getBatchReceiptsByHashList callback empty receipts");
//                         auto error =
//                             std::make_shared<Error>(LedgerError::CallbackError, "empty
//                             receipts");
//                         _onGetProof(error, nullptr);
//                         return;
//                     }
//                     auto merkleProof = std::make_shared<MerkleProof>();
//                     auto parent2ChildList =
//                         m_merkleProofUtility->getParent2ChildListByReceiptProofCache(
//                             _receipt->blockNumber(), receipts, m_blockFactory->cryptoSuite());
//                     auto child2Parent = m_merkleProofUtility->getChild2ParentCacheByReceipt(
//                         parent2ChildList, _receipt->blockNumber());
//                     m_merkleProofUtility->getMerkleProof(
//                         _receipt->hash(), *parent2ChildList, *child2Parent, *merkleProof);
//                     LEDGER_LOG(INFO)
//                         << LOG_BADGE("getReceiptProof") << LOG_DESC("call back receipt and
//                         proof");
//                     _onGetProof(nullptr, merkleProof);
//                 });
//         });
// }

// void Ledger::asyncGetLedgerConfig(protocol::BlockNumber _number, const crypto::HashType& _hash,
//     std::function<void(Error::Ptr, WrapperLedgerConfig::Ptr)> _onGetLedgerConfig)
// {
//     auto ledgerConfig = std::make_shared<LedgerConfig>();
//     std::atomic_bool asyncRet = {true};
//     ledgerConfig->setBlockNumber(_number);
//     ledgerConfig->setHash(_hash);

//     auto wrapperLedgerConfig = std::make_shared<WrapperLedgerConfig>(ledgerConfig);

//     auto storageGetter = getStorageGetter();
//     auto tableFactory = getMemoryTableFactory(0);
//     auto keys = std::make_shared<std::vector<std::string>>();
//     *keys = {SYSTEM_KEY_CONSENSUS_TIMEOUT, SYSTEM_KEY_TX_COUNT_LIMIT,
//         SYSTEM_KEY_CONSENSUS_LEADER_PERIOD};

//     storageGetter->asyncGetSystemConfigList(*keys, tableFactory, false,
//         [_number, wrapperLedgerConfig, _onGetLedgerConfig](
//             Error::Ptr&& _error, std::vector<Entry::Ptr>&& _entries) {
//             if (_error)
//             {
//                 LEDGER_LOG(WARNING)
//                     << LOG_DESC("asyncGetLedgerConfig failed")
//                     << LOG_KV("code", _error->errorCode()) << LOG_KV("msg",
//                     _error->errorMessage());
//                 _onGetLedgerConfig(_error, nullptr);
//                 return;
//             }
//             try
//             {
//                 auto ledgerConfig = wrapperLedgerConfig->ledgerConfig();

//                 // The param was reset at height getLatestBlockNumber(), and takes effect in
//                 // next block. So we query the status of getLatestBlockNumber() + 1.
//                 auto number = _number + 1;

//                 // parse the configurations
//                 auto consensusTimeout = _entries[0]->getField(SYS_VALUE);
//                 auto txCountLimit = _entries[1]->getField(SYS_VALUE);
//                 auto leaderPeriod = _entries[2]->getField(SYS_VALUE);

//                 // check enable number
//                 auto timeoutEnableNum = boost::lexical_cast<BlockNumber>(
//                     _entries[0]->getField(SYS_CONFIG_ENABLE_BLOCK_NUMBER));
//                 auto limitEnableNum = boost::lexical_cast<BlockNumber>(
//                     _entries[0]->getField(SYS_CONFIG_ENABLE_BLOCK_NUMBER));
//                 auto periodEnableNum = boost::lexical_cast<BlockNumber>(
//                     _entries[0]->getField(SYS_CONFIG_ENABLE_BLOCK_NUMBER));

//                 if (timeoutEnableNum > number || limitEnableNum > number ||
//                     periodEnableNum > number)
//                 {
//                     LEDGER_LOG(FATAL) << LOG_BADGE("asyncGetLedgerConfig")
//                                       << LOG_DESC(
//                                              "asyncGetSystemConfigList error for enable number "
//                                              "higher than number")
//                                       << LOG_KV("consensusTimeout", consensusTimeout)
//                                       << LOG_KV("txCountLimit", txCountLimit)
//                                       << LOG_KV("consensusLeaderPeriod", leaderPeriod)
//                                       << LOG_KV("consensusTimeoutNum", timeoutEnableNum)
//                                       << LOG_KV("txCountLimitNum", limitEnableNum)
//                                       << LOG_KV("consensusLeaderPeriodNum", periodEnableNum)
//                                       << LOG_KV("latestNumber", _number);
//                 }
//                 ledgerConfig->setConsensusTimeout(boost::lexical_cast<uint64_t>(consensusTimeout));
//                 ledgerConfig->setBlockTxCountLimit(boost::lexical_cast<uint64_t>(txCountLimit));
//                 ledgerConfig->setLeaderSwitchPeriod(boost::lexical_cast<uint64_t>(leaderPeriod));

//                 LEDGER_LOG(INFO) << LOG_BADGE("asyncGetLedgerConfig")
//                                  << LOG_DESC("asyncGetSystemConfigList success")
//                                  << LOG_KV("consensusTimeout", consensusTimeout)
//                                  << LOG_KV("txCountLimit", txCountLimit)
//                                  << LOG_KV("consensusLeaderPeriod", leaderPeriod)
//                                  << LOG_KV("consensusTimeoutNum", timeoutEnableNum)
//                                  << LOG_KV("txCountLimitNum", limitEnableNum)
//                                  << LOG_KV("consensusLeaderPeriodNum", periodEnableNum)
//                                  << LOG_KV("latestNumber", _number);
//                 wrapperLedgerConfig->setSysConfigFetched(true);
//                 _onGetLedgerConfig(nullptr, wrapperLedgerConfig);
//             }
//             catch (std::exception const& e)
//             {
//                 auto errorMsg = "asyncGetLedgerConfig:  asyncGetSystemConfigList failed for " +
//                                 boost::diagnostic_information(e);
//                 LEDGER_LOG(ERROR) << LOG_DESC(errorMsg);
//                 _onGetLedgerConfig(
//                     std::make_shared<Error>(LedgerError::CallbackError, errorMsg), nullptr);
//             }
//         });

//     // get the consensusNodeInfo and the observerNodeInfo
//     std::vector<std::string> nodeTypeList = {CONSENSUS_SEALER, CONSENSUS_OBSERVER};
//     getLatestBlockNumber(
//         [=](BlockNumber _number) {
//             // The param was reset at height getLatestBlockNumber(), and takes effect in next
//             // block. So we query the status of getLatestBlockNumber() + 1.
//             auto number = _number + 1;
//             storageGetter->asyncGetConsensusConfigList(nodeTypeList, number, tableFactory,
//                 m_blockFactory->cryptoSuite()->keyFactory(),
//                 [wrapperLedgerConfig, _onGetLedgerConfig](Error::Ptr _error,
//                     std::map<std::string, consensus::ConsensusNodeListPtr> _nodeMap) {
//                     if (_error)
//                     {
//                         LEDGER_LOG(WARNING)
//                             << LOG_DESC("asyncGetLedgerConfig: asyncGetConsensusConfig failed")
//                             << LOG_KV("code", _error->errorCode())
//                             << LOG_KV("msg", _error->errorMessage());
//                         _onGetLedgerConfig(_error, nullptr);
//                         return;
//                     }
//                     auto ledgerConfig = wrapperLedgerConfig->ledgerConfig();
//                     if (_nodeMap.count(CONSENSUS_SEALER) && _nodeMap[CONSENSUS_SEALER])
//                     {
//                         auto consensusNodeList = _nodeMap[CONSENSUS_SEALER];
//                         ledgerConfig->setConsensusNodeList(*consensusNodeList);
//                     }
//                     if (_nodeMap.count(CONSENSUS_OBSERVER) && _nodeMap[CONSENSUS_OBSERVER])
//                     {
//                         auto observerNodeList = _nodeMap[CONSENSUS_OBSERVER];
//                         ledgerConfig->setObserverNodeList(*observerNodeList);
//                     }
//                     LEDGER_LOG(INFO)
//                         << LOG_DESC("asyncGetLedgerConfig: asyncGetConsensusConfig success")
//                         << LOG_KV("consensusNodeSize", ledgerConfig->consensusNodeList().size())
//                         << LOG_KV("observerNodeSize", ledgerConfig->observerNodeList().size());
//                     wrapperLedgerConfig->setConsensusConfigFetched(true);
//                     _onGetLedgerConfig(nullptr, wrapperLedgerConfig);
//                 });
//         });
// }

// void Ledger::checkBlockShouldCommit(const BlockNumber& _blockNumber, const std::string&
// _parentHash,
//     std::function<void(bool)> _onGetResult)
// {
//     auto self = std::weak_ptr<Ledger>(std::dynamic_pointer_cast<Ledger>(shared_from_this()));
//     getLatestBlockNumber(
//         [self, _blockNumber, _parentHash, _onGetResult](protocol::BlockNumber _number) {
//             auto ledger = self.lock();
//             if (!ledger)
//             {
//                 _onGetResult(false);
//                 return;
//             }
//             ledger->getLatestBlockHash(_number, [_number, _parentHash, _blockNumber,
//             _onGetResult](
//                                                     std::string_view _hashStr) {
//                 if (_blockNumber == _number + 1 && _parentHash == std::string(_hashStr))
//                 {
//                     _onGetResult(true);
//                     return;
//                 }
//                 LEDGER_LOG(WARNING)
//                     << LOG_BADGE("checkBlockShouldCommit")
//                     << LOG_DESC("incorrect block number or incorrect parent hash")
//                     << LOG_KV("needNumber", _number + 1) << LOG_KV("committedNumber",
//                     _blockNumber)
//                     << LOG_KV("lastBlockHash", _hashStr)
//                     << LOG_KV("committedParentHash", _parentHash);
//                 _onGetResult(false);
//             });
//         });
// }

// void Ledger::writeNumber(
//     const BlockNumber& blockNumber, const bcos::storage::TableStorage::Ptr& _tableFactory)
// {
//     bool ret = getStorageSetter()->setCurrentState(
//         _tableFactory, SYS_KEY_CURRENT_NUMBER, boost::lexical_cast<std::string>(blockNumber));
//     if (!ret)
//     {
//         LEDGER_LOG(DEBUG) << LOG_BADGE("writeNumber")
//                           << LOG_DESC("Write row in SYS_CURRENT_STATE error")
//                           << LOG_KV("blockNumber", blockNumber);
//     }
// }

// void Ledger::writeNumber2Nonces(
//     const Block::Ptr& block, const bcos::storage::TableStorage::Ptr& _tableFactory)
// {
//     auto blockNumberStr = boost::lexical_cast<std::string>(block->blockHeader()->number());
//     auto emptyBlock = m_blockFactory->createBlock();
//     emptyBlock->setNonceList(block->nonceList());

//     std::shared_ptr<bytes> nonceData = std::make_shared<bytes>();
//     emptyBlock->encode(*nonceData);

//     auto nonceStr = asString(*nonceData);
//     bool ret = getStorageSetter()->setNumber2Nonces(_tableFactory, blockNumberStr, nonceStr);
//     if (!ret)
//     {
//         LEDGER_LOG(DEBUG) << LOG_BADGE("WriteNoncesToBlock")
//                           << LOG_DESC("Write row in SYS_BLOCK_NUMBER_2_NONCES error")
//                           << LOG_KV("blockNumber", blockNumberStr)
//                           << LOG_KV("hash", block->blockHeader()->hash().abridged());
//     }
// }

// void Ledger::writeHash2Number(
//     const BlockHeader::Ptr& header, const bcos::storage::TableStorage::Ptr& _tableFactory)
// {
//     bool ret = getStorageSetter()->setHash2Number(
//         _tableFactory, header->hash().hex(), boost::lexical_cast<std::string>(header->number()));
//     ret = ret && getStorageSetter()->setNumber2Hash(_tableFactory,
//                      boost::lexical_cast<std::string>(header->number()), header->hash().hex());
//     if (!ret)
//     {
//         LEDGER_LOG(DEBUG) << LOG_BADGE("WriteHash2Number")
//                           << LOG_DESC("Write row in SYS_HASH_2_NUMBER error")
//                           << LOG_KV("number", header->number())
//                           << LOG_KV("blockHash", header->hash().abridged());
//     }
// }

// void Ledger::writeNumber2BlockHeader(
//     const BlockHeader::Ptr& _header, const bcos::storage::TableStorage::Ptr& _tableFactory)
// {
//     auto encodedBlockHeader = std::make_shared<bytes>();
//     auto header = _header;
//     _header->encode(*encodedBlockHeader);

//     bool ret = getStorageSetter()->setNumber2Header(_tableFactory,
//         boost::lexical_cast<std::string>(_header->number()), asString(*encodedBlockHeader));
//     if (!ret)
//     {
//         LEDGER_LOG(DEBUG) << LOG_BADGE("WriteNumber2Header")
//                           << LOG_DESC("Write row in SYS_NUMBER_2_BLOCK_HEADER error")
//                           << LOG_KV("blockNumber", _header->number())
//                           << LOG_KV("hash", _header->hash().abridged());
//     }
// }
// void Ledger::writeTotalTransactionCount(
//     const Block::Ptr& block, const bcos::storage::TableStorage::Ptr& _tableFactory)
// {
//     // empty block
//     if (block->transactionsSize() == 0 && block->receiptsSize() == 0)
//     {
//         LEDGER_LOG(WARNING) << LOG_BADGE("writeTotalTransactionCount")
//                             << LOG_DESC("Empty block, stop update total tx count")
//                             << LOG_KV("blockNumber", block->blockHeader()->number());
//         return;
//     }
//     auto self = std::weak_ptr<Ledger>(std::dynamic_pointer_cast<Ledger>(shared_from_this()));
//     getStorageGetter()->getCurrentState(SYS_KEY_TOTAL_TRANSACTION_COUNT, _tableFactory,
//         [self, block, _tableFactory](Error::Ptr _error, bcos::storage::Entry::Ptr _totalTxEntry)
//         {
//             if (_error && _error->errorCode() != CommonError::SUCCESS)
//             {
//                 LEDGER_LOG(ERROR) << LOG_BADGE("writeTotalTransactionCount")
//                                   << LOG_DESC("Get SYS_KEY_TOTAL_TRANSACTION_COUNT error")
//                                   << LOG_KV("blockNumber", block->blockHeader()->number());
//                 return;
//             }
//             auto ledger = self.lock();
//             if (!ledger)
//                 return;
//             int64_t totalTxCount = 0;
//             auto totalTxStr = _totalTxEntry->getField(SYS_VALUE);
//             if (!totalTxStr.empty())
//             {
//                 totalTxCount += boost::lexical_cast<int64_t>(totalTxStr);
//             }
//             totalTxCount += block->transactionsSize();
//             ledger->getStorageSetter()->setCurrentState(_tableFactory,
//                 SYS_KEY_TOTAL_TRANSACTION_COUNT, boost::lexical_cast<std::string>(totalTxCount));
//         });

//     getStorageGetter()->getCurrentState(SYS_KEY_TOTAL_FAILED_TRANSACTION, _tableFactory,
//         [self, _tableFactory, block](
//             Error::Ptr _error, bcos::storage::Entry::Ptr _totalFailedTxsEntry) {
//             if (_error && _error->errorCode() != CommonError::SUCCESS)
//             {
//                 LEDGER_LOG(ERROR) << LOG_BADGE("writeTotalTransactionCount")
//                                   << LOG_DESC("Get SYS_KEY_TOTAL_FAILED_TRANSACTION error")
//                                   << LOG_KV("blockNumber", block->blockHeader()->number());
//                 return;
//             }
//             auto ledger = self.lock();
//             if (!ledger)
//                 return;
//             auto receipts = blockReceiptListGetter(block);
//             int64_t failedTransactions = 0;
//             for (auto& receipt : *receipts)
//             {
//                 // TODO: check receipt status
//                 if (receipt->status() != 0)
//                 {
//                     ++failedTransactions;
//                 }
//             }
//             auto totalFailedTxsStr = _totalFailedTxsEntry->getField(SYS_VALUE);
//             if (!totalFailedTxsStr.empty())
//             {
//                 failedTransactions += boost::lexical_cast<int64_t>(totalFailedTxsStr);
//             }
//             ledger->getStorageSetter()->setCurrentState(_tableFactory,
//                 SYS_KEY_TOTAL_FAILED_TRANSACTION,
//                 boost::lexical_cast<std::string>(failedTransactions));
//         });
// }
// void Ledger::writeNumber2Transactions(
//     const Block::Ptr& _block, const TableStorage::Ptr& _tableFactory)
// {
//     if (_block->transactionsSize() == 0)
//     {
//         LEDGER_LOG(TRACE) << LOG_BADGE("WriteNumber2Txs") << LOG_DESC("empty txs in block")
//                           << LOG_KV("blockNumber", _block->blockHeader()->number());
//         return;
//     }
//     auto encodeBlock = std::make_shared<bytes>();
//     auto emptyBlock = m_blockFactory->createBlock();
//     auto number = _block->blockHeader()->number();
//     for (size_t i = 0; i < _block->transactionsSize(); i++)
//     {
//         // Note: in some cases(block sync), the transactionHash fields maybe empty
//         auto tx = _block->transaction(i);
//         emptyBlock->appendTransactionHash(tx->hash());
//     }

//     emptyBlock->encode(*encodeBlock);
//     bool ret = getStorageSetter()->setNumber2Txs(
//         _tableFactory, boost::lexical_cast<std::string>(number), asString(*encodeBlock));
//     if (!ret)
//     {
//         LEDGER_LOG(DEBUG) << LOG_BADGE("WriteNumber2Txs")
//                           << LOG_DESC("Write row in SYS_NUMBER_2_TXS error")
//                           << LOG_KV("blockNumber", _block->blockHeader()->number())
//                           << LOG_KV("hash", _block->blockHeader()->hash().abridged());
//     }
// }
// void Ledger::writeHash2Receipt(
//     const bcos::protocol::Block::Ptr& _block, const TableStorage::Ptr& _tableFactory)
// {
//     tbb::parallel_for(tbb::blocked_range<size_t>(0, _block->transactionsSize()),
//         [&](const tbb::blocked_range<size_t>& range) {
//             for (size_t i = range.begin(); i < range.end(); ++i)
//             {
//                 auto tx = _block->transaction(i);
//                 // Note: in the tars-service, call receipt(_index) will make a new object, must
//                 // add the receipt here to maintain the lifetime, and in case of encodeReceipt
//                 // be released
//                 auto receipt = _block->receipt(i);
//                 auto encodeReceipt = receipt->encode();
//                 auto ret = getStorageSetter()->setHashToReceipt(
//                     _tableFactory, tx->hash().hex(), asString(encodeReceipt));
//                 if (!ret)
//                 {
//                     LEDGER_LOG(DEBUG)
//                         << LOG_BADGE("writeHash2Receipt")
//                         << LOG_DESC("Write row in SYS_HASH_2_RECEIPT error")
//                         << LOG_KV("txHash", tx->hash().hex())
//                         << LOG_KV("blockNumber", _block->blockHeader()->number())
//                         << LOG_KV("blockHash", _block->blockHeader()->hash().abridged());
//                 }
//             }
//         });
// }

// void Ledger::notifyCommittedBlockNumber(protocol::BlockNumber _blockNumber)
// {
//     if (!m_committedBlockNotifier)
//         return;
//     m_committedBlockNotifier(_blockNumber, [_blockNumber](Error::Ptr _error) {
//         if (!_error)
//         {
//             return;
//         }
//         LEDGER_LOG(WARNING) << LOG_BADGE("notifyCommittedBlockNumber")
//                             << LOG_DESC("notify the block number failed")
//                             << LOG_KV("blockNumber", _blockNumber);
//     });
// }

// bool Ledger::buildGenesisBlock(LedgerConfig::Ptr _ledgerConfig, const std::string& _groupId,
//     size_t _gasLimit, const std::string& _genesisData)
// {
//     LEDGER_LOG(INFO) << LOG_DESC("[#buildGenesisBlock]");
//     if (_ledgerConfig->consensusTimeout() > SYSTEM_CONSENSUS_TIMEOUT_MAX ||
//         _ledgerConfig->consensusTimeout() < SYSTEM_CONSENSUS_TIMEOUT_MIN)
//     {
//         LEDGER_LOG(ERROR) << LOG_BADGE("buildGenesisBlock")
//                           << LOG_DESC("consensus timeout set error, return false")
//                           << LOG_KV("consensusTimeout", _ledgerConfig->consensusTimeout());
//         return false;
//     }
//     if (_gasLimit < TX_GAS_LIMIT_MIN)
//     {
//         LEDGER_LOG(ERROR) << LOG_BADGE("buildGenesisBlock")
//                           << LOG_DESC("gas limit too low, return false")
//                           << LOG_KV("gasLimit", _gasLimit)
//                           << LOG_KV("gasLimitMin", TX_GAS_LIMIT_MIN);
//         return false;
//     }
//     if (!getStorageGetter()->checkTableExist(SYS_NUMBER_2_BLOCK_HEADER,
//     getMemoryTableFactory(0)))
//     {
//         LEDGER_LOG(INFO) << LOG_BADGE("buildGenesisBlock")
//                          << LOG_DESC(
//                                 std::string(SYS_NUMBER_2_BLOCK_HEADER) + " table does not
//                                 exist");
//         getStorageSetter()->createTables(getMemoryTableFactory(0), _groupId);
//     };
//     BlockHeader::Ptr header = nullptr;
//     std::promise<BlockHeader::Ptr> headerPromise;
//     getBlockHeader(0, [&headerPromise](Error::Ptr _error, BlockHeader::Ptr _header) {
//         if (!_error)
//         {
//             // header is nullptr means need build a genesis block
//             headerPromise.set_value(_header);
//             LEDGER_LOG(INFO) << LOG_BADGE(
//                 "buildGenesisBlock, get the genesis block header success");
//             return;
//         }
//         headerPromise.set_value(nullptr);
//         LEDGER_LOG(INFO) << LOG_BADGE("buildGenesisBlock")
//                          << LOG_DESC("get genesis block header callback error")
//                          << LOG_KV("errorCode", _error->errorCode())
//                          << LOG_KV("errorMsg", _error->errorMessage());
//     });
//     auto headerFuture = headerPromise.get_future();
//     if (std::future_status::ready == headerFuture.wait_for(std::chrono::milliseconds(m_timeout)))
//     {
//         header = headerFuture.get();
//     }
//     // to build genesis block
//     if (header == nullptr)
//     {
//         auto txLimit = _ledgerConfig->blockTxCountLimit();
//         LEDGER_LOG(INFO) << LOG_DESC("Commit the genesis block") << LOG_KV("txLimit", txLimit);
//         auto tableFactory = getMemoryTableFactory(0);
//         // build a block
//         header = getBlockHeaderFactory()->createBlockHeader();
//         header->setNumber(0);
//         header->setExtraData(asBytes(_genesisData));
//         try
//         {
//             tbb::parallel_invoke(
//                 [this, tableFactory, header]() { writeHash2Number(header, tableFactory); },
//                 [this, tableFactory, _ledgerConfig]() {
//                     getStorageSetter()->setSysConfig(tableFactory, SYSTEM_KEY_TX_COUNT_LIMIT,
//                         boost::lexical_cast<std::string>(_ledgerConfig->blockTxCountLimit()),
//                         "0");
//                 },
//                 [this, tableFactory, _gasLimit]() {
//                     getStorageSetter()->setSysConfig(tableFactory, SYSTEM_KEY_TX_GAS_LIMIT,
//                         boost::lexical_cast<std::string>(_gasLimit), "0");
//                 },
//                 [this, tableFactory, _ledgerConfig]() {
//                     getStorageSetter()->setSysConfig(tableFactory,
//                         SYSTEM_KEY_CONSENSUS_LEADER_PERIOD,
//                         boost::lexical_cast<std::string>(_ledgerConfig->leaderSwitchPeriod()),
//                         "0");
//                 },
//                 [this, tableFactory, _ledgerConfig]() {
//                     getStorageSetter()->setSysConfig(tableFactory, SYSTEM_KEY_CONSENSUS_TIMEOUT,
//                         boost::lexical_cast<std::string>(_ledgerConfig->consensusTimeout()),
//                         "0");
//                 });
//             tbb::parallel_invoke(
//                 [this, tableFactory, _ledgerConfig]() {
//                     getStorageSetter()->setConsensusConfig(
//                         tableFactory, CONSENSUS_SEALER, _ledgerConfig->consensusNodeList(), "0");
//                 },
//                 [this, tableFactory, _ledgerConfig]() {
//                     getStorageSetter()->setConsensusConfig(
//                         tableFactory, CONSENSUS_OBSERVER, _ledgerConfig->observerNodeList(),
//                         "0");
//                 },
//                 [this, tableFactory, header]() { writeNumber2BlockHeader(header, tableFactory);
//                 }, [this, tableFactory]() {
//                     getStorageSetter()->setCurrentState(tableFactory, SYS_KEY_CURRENT_NUMBER,
//                     "0");
//                 },
//                 [this, tableFactory]() {
//                     getStorageSetter()->setCurrentState(
//                         tableFactory, SYS_KEY_TOTAL_TRANSACTION_COUNT, "0");
//                 },
//                 [this, tableFactory]() {
//                     getStorageSetter()->setCurrentState(
//                         tableFactory, SYS_KEY_TOTAL_FAILED_TRANSACTION, "0");
//                 });

//             /*
//             // db sync commit
//             LEDGER_LOG(INFO) << LOG_DESC("[buildGenesisBlock]commit all the table data");
//             auto retPair = tableFactory->commit();
//             if ((!retPair.second || retPair.second->errorCode() == CommonError::SUCCESS) &&
//                 retPair.first > 0)
//             {
//                 LEDGER_LOG(INFO) << LOG_DESC("[buildGenesisBlock]Storage commit success")
//                                  << LOG_KV("commitSize", retPair.first);
//                 return true;
//             }
//             else
//             {
//                 LEDGER_LOG(ERROR) << LOG_DESC("[#buildGenesisBlock]Storage commit error");
//                 return false;
//             }
//             */
//         }
//         catch (OpenSysTableFailed const& e)
//         {
//             LEDGER_LOG(FATAL) << LOG_DESC(
//                                      "[#buildGenesisBlock]System meets error when try to write "
//                                      "block to storage")
//                               << LOG_KV("EINFO", boost::diagnostic_information(e));
//             raise(SIGTERM);
//             BOOST_THROW_EXCEPTION(
//                 OpenSysTableFailed() << errinfo_comment(" write block to storage failed."));
//         }
//     }
//     else
//     {
//         LEDGER_LOG(INFO) << LOG_BADGE("buildGenesisBlock") << LOG_DESC("Already have the 0th
//         block")
//                          << LOG_KV("hash", header->hash().abridged())
//                          << LOG_KV("number", header->number());
//         LEDGER_LOG(INFO) << LOG_BADGE("buildGenesisBlock")
//                          << LOG_DESC("Load genesis config from extraData");
//         return header->extraData().toString() == _genesisData;
//     }
//     return true;
// }
