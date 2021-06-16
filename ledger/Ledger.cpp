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
#include "utilities/BlockUtilities.h"
#include <bcos-framework/libprotocol/ParallelMerkleProof.h>
#include <bcos-framework/interfaces/protocol/CommonError.h>
#include <boost/lexical_cast.hpp>
#include <tbb/parallel_invoke.h>
#include <tbb/parallel_for.h>

using namespace bcos;
using namespace bcos::ledger;
using namespace bcos::protocol;
using namespace bcos::storage;
using namespace bcos::crypto;

void Ledger::asyncCommitBlock(bcos::protocol::BlockHeader::Ptr _header,
    std::function<void(Error::Ptr, LedgerConfig::Ptr)> _onCommitBlock)
{
    if (_header == nullptr)
    {
        LEDGER_LOG(FATAL) << LOG_BADGE("asyncCommitBlock") <<LOG_DESC("Header is nullptr");
        // TODO: add error code
        auto error = std::make_shared<Error>(-1, "[#asyncCommitBlock] Header is nullptr.");
        _onCommitBlock(error, nullptr);
        return;
    }
    // default parent block hash located in parentInfo[0]
    if (!isBlockShouldCommit(_header->number(), _header->parentInfo().at(0).blockHash.hex()))
    {
        // TODO: add error code
        auto error = std::make_shared<Error>(
            -1, "[#asyncCommitBlock] Wrong block number of wrong parent hash");
        _onCommitBlock(error, nullptr);
        return;
    }
    auto blockNumber = _header->number();
    auto tableFactory = getMemoryTableFactory(blockNumber);

    auto ledgerConfig = getLedgerConfig(blockNumber, _header->hash());
    try
    {
        tbb::parallel_invoke(
            [this, _header, tableFactory]() { writeNumber(_header->number(), tableFactory); },
            [this, _header, tableFactory]() { writeHash2Number(_header, tableFactory); },
            [this, _header, tableFactory]() { writeNumber2BlockHeader(_header, tableFactory); });

        auto self = std::weak_ptr<Ledger>(std::dynamic_pointer_cast<Ledger>(shared_from_this()));
        tableFactory->asyncCommit([blockNumber, _header, _onCommitBlock, ledgerConfig, self](
                                      Error::Ptr _error, size_t _commitSize) {
            if ((_error && _error->errorCode() != CommonError::SUCCESS) || _commitSize < 1)
            {
                LEDGER_LOG(ERROR) << LOG_DESC("Commit Block failed in storage")
                                  << LOG_KV("number", blockNumber);
                // TODO: add error code
                auto error = std::make_shared<Error>(_error->errorCode(),
                    "[#asyncCommitBlock] Commit block error in storage" + _error->errorMessage());
                _onCommitBlock(error, nullptr);
                return;
            }
            auto ledger = self.lock();
            if (!ledger)
            {
                // TODO: add error code
                auto error = std::make_shared<Error>(-1, "");
                _onCommitBlock(error, nullptr);
                return;
            }
            LEDGER_LOG(INFO) << LOG_BADGE("asyncCommitBlock") << LOG_DESC("commit block success")
                             << LOG_KV("blockNumber", blockNumber);
            ledger->m_blockHeaderCache.add(blockNumber, _header);
            _onCommitBlock(nullptr, ledgerConfig);
        });
    }
    catch (OpenSysTableFailed const& e)
    {
        LEDGER_LOG(FATAL) << LOG_BADGE("asyncCommitBlock")
                          << LOG_DESC("System meets error when try to write block to storage")
                          << LOG_KV("EINFO", boost::diagnostic_information(e));
        raise(SIGTERM);
        BOOST_THROW_EXCEPTION(
            OpenSysTableFailed() << errinfo_comment(" write block to storage failed."));
    }
}

void Ledger::asyncStoreTransactions(std::shared_ptr<std::vector<bytesPointer>> _txToStore,
    crypto::HashListPtr _txHashList, std::function<void(Error::Ptr)> _onTxStored)
{
    if (!_txToStore || !_txHashList || _txHashList->size() != _txToStore->size())
    {
        // TODO: add error code
        auto error = std::make_shared<Error>(-1, "[#asyncStoreTransactions] error parameters");
        _onTxStored(error);
        return;
    }
    auto self = std::weak_ptr<Ledger>(std::dynamic_pointer_cast<Ledger>(shared_from_this()));
    getLatestBlockNumber([self, _txHashList, _txToStore, _onTxStored](protocol::BlockNumber _number) {
        auto ledger = self.lock();
        auto tableFactory = ledger->getMemoryTableFactory(_number + 1);
        if (!tableFactory)
        {
            LEDGER_LOG(ERROR) << LOG_BADGE("asyncStoreTransactions")
                              << LOG_DESC("get a null tableFactory in state cache");
            // TODO: add error code
            auto error = std::make_shared<Error>(
                -1, "[#asyncStoreTransactions] get a null tableFactory in state cache");
            _onTxStored(error);
            return;
        }
        try
        {
            for (size_t i = 0; i < _txHashList->size(); ++i)
            {
                auto txHashHex = _txHashList->at(i).hex();
                ledger->getStorageSetter()->setHashToTx(
                    tableFactory, txHashHex, asString(*(_txToStore->at(i))));
                LEDGER_LOG(TRACE) << LOG_BADGE("setHashToTx") << LOG_DESC("write HASH_2_TX success")
                                  << LOG_KV("txHashHex", txHashHex);
            }
            tableFactory->asyncCommit([_onTxStored](Error::Ptr _error, size_t _commitSize) {
                if (!_error || _error->errorCode() == CommonError::SUCCESS)
                {
                    LEDGER_LOG(TRACE)
                        << LOG_BADGE("asyncStoreTransactions") << LOG_DESC("write db success")
                        << LOG_KV("commitSize", _commitSize);
                    _onTxStored(nullptr);
                }
                else
                {
                    LEDGER_LOG(ERROR)
                        << LOG_BADGE("asyncStoreTransactions") << LOG_DESC("table commit failed");
                    // TODO: add error code and msg
                    auto error = std::make_shared<Error>(_error->errorCode(),
                        "[#asyncStoreTransactions] table commit failed" + _error->errorMessage());
                    _onTxStored(error);
                }
            });
        }
        catch (OpenSysTableFailed const& e)
        {
            LEDGER_LOG(FATAL) << LOG_BADGE("asyncStoreTransactions")
                              << LOG_DESC("System meets error when try to write tx to storage")
                              << LOG_KV("EINFO", boost::diagnostic_information(e));
            raise(SIGTERM);
            BOOST_THROW_EXCEPTION(
                OpenSysTableFailed() << errinfo_comment(" write block to storage failed."));
        }
    });
}

void Ledger::asyncStoreReceipts(storage::TableFactoryInterface::Ptr _tableFactory,
    protocol::Block::Ptr _block, std::function<void(Error::Ptr)> _onReceiptStored)
{
    if (_block == nullptr || _tableFactory == nullptr)
    {
        LEDGER_LOG(FATAL) << LOG_BADGE("asyncStoreReceipts")
                          << LOG_DESC("Error parameters");
        // TODO: add error code
        auto error = std::make_shared<Error>(-1, "block or tableFactory is null");
        _onReceiptStored(error);
        return;
    }
    auto blockNumber = _block->blockHeader()->number();
    try
    {
        tbb::parallel_invoke(
            [this, _block, _tableFactory]() { writeTotalTransactionCount(_block, _tableFactory); },
            [this, _block, _tableFactory]() { writeNumber2Nonces(_block, _tableFactory); },
            [this, _block, _tableFactory]() { writeNumber2Transactions(_block, _tableFactory); },
            [this, _block, _tableFactory]() { writeHash2Receipt(_block, _tableFactory); });

        auto self = std::weak_ptr<Ledger>(std::dynamic_pointer_cast<Ledger>(shared_from_this()));
        getStorage()->asyncAddStateCache(blockNumber, _tableFactory,
            [_block, _onReceiptStored, blockNumber, self](Error::Ptr _error) {
                auto ledger = self.lock();
                if (!_error || _error->errorCode() == CommonError::SUCCESS)
                {
                    ledger->m_transactionsCache.add(blockNumber, blockTransactionListGetter(_block));
                    ledger->m_receiptCache.add(blockNumber, blockReceiptListGetter(_block));
                    _onReceiptStored(nullptr);
                }
                else
                {
                    LEDGER_LOG(ERROR)
                        << LOG_BADGE("asyncStoreReceipts") << LOG_DESC("add state cache failed")
                        << LOG_KV("errorCode", _error->errorCode())
                        << LOG_KV("errorMsg", _error->errorMessage())
                        << LOG_KV("number", blockNumber);
                    // TODO: add error code
                    auto error =
                        std::make_shared<Error>(_error->errorCode(), "add state cache failed" + _error->errorMessage());
                    _onReceiptStored(error);
                }
            });
    }
    catch (OpenSysTableFailed const& e)
    {
        LEDGER_LOG(FATAL) << LOG_BADGE("asyncStoreReceipts")
                          << LOG_DESC("System meets error when try to write data to storage")
                          << LOG_KV("EINFO", boost::diagnostic_information(e));
        raise(SIGTERM);
        BOOST_THROW_EXCEPTION(
            OpenSysTableFailed() << errinfo_comment(" write block data to storage failed."));
    }
}

void Ledger::asyncGetBlockDataByNumber(bcos::protocol::BlockNumber _blockNumber, int32_t _blockFlag,
    std::function<void(Error::Ptr, bcos::protocol::Block::Ptr)> _onGetBlock)
{
    getLatestBlockNumber(
        [this, _blockNumber, _blockFlag, _onGetBlock](protocol::BlockNumber _number) {
            if (_blockNumber < 0 || _blockNumber > _number)
            {
                LEDGER_LOG(FATAL) << LOG_BADGE("asyncGetBlockDataByNumber")
                                  << LOG_DESC("Error parameters");
                // TODO: to add errorCode
                auto error = std::make_shared<Error>(-1, "error block number");
                _onGetBlock(error, nullptr);
                return;
            }
            getBlock(_blockNumber, _blockFlag,
                [_blockNumber, _onGetBlock](Error::Ptr _error, protocol::Block::Ptr _block) {
                    if (!_error || _error->errorCode() == CommonError::SUCCESS)
                    {
                        if (_block)
                        {
                            _onGetBlock(nullptr, _block);
                        }
                        else
                        {
                            LEDGER_LOG(ERROR) << LOG_BADGE("asyncGetBlockDataByNumber")
                                              << LOG_DESC("Get a null block")
                                              << LOG_KV("blockNumber", _blockNumber);
                            // TODO: to add errorCode and message
                            auto error = std::make_shared<Error>(-1, "get block return null");
                            _onGetBlock(error, nullptr);
                        }
                    }
                    else
                    {
                        LEDGER_LOG(ERROR) << LOG_BADGE("asyncGetBlockDataByNumber")
                                          << LOG_DESC("callback error when get block")
                                          << LOG_KV("errorCode", _error->errorCode())
                                          << LOG_KV("errorMsg", _error->errorMessage())
                                          << LOG_KV("blockNumber", _blockNumber);
                        // TODO: to add errorCode and message
                        auto error = std::make_shared<Error>(_error->errorCode(),
                            "callback error in getBlock" + _error->errorMessage());
                        _onGetBlock(error, nullptr);
                    }
                });
        });
}

void Ledger::asyncGetBlockNumber(
    std::function<void(Error::Ptr, bcos::protocol::BlockNumber)> _onGetBlock)
{
    getLatestBlockNumber([_onGetBlock](BlockNumber _number) {
      if (_number == -1)
      {
          // TODO: to add errorCode
          auto error = std::make_shared<Error>(-1, "getLatestBlock error, callback -1");
          _onGetBlock(error, -1);
          return;
      }
      _onGetBlock(nullptr, _number);
    });
}

void Ledger::asyncGetBlockHashByNumber(bcos::protocol::BlockNumber _blockNumber,
    std::function<void(Error::Ptr, const bcos::crypto::HashType&)> _onGetBlock)
{
    if (_blockNumber < 0 )
    {
        LEDGER_LOG(FATAL) << LOG_BADGE("asyncGetBlockHashByNumber")
                          << LOG_DESC("Error parameters");
        // TODO: to add errorCode
        auto error = std::make_shared<Error>(-1, "wrong block number, callback empty hash");
        _onGetBlock(error, HashType(""));
        return;
    }
    getLatestBlockNumber([this, _onGetBlock, _blockNumber](BlockNumber _number) {
        if(_blockNumber > _number) {
            // TODO: to add errorCode
            auto error = std::make_shared<Error>(-1, "too large block number, callback empty hash");
            _onGetBlock(error, HashType(""));
            return;
        }
        getStorageGetter()->getBlockHashByNumber(_blockNumber, getMemoryTableFactory(0),
            [_onGetBlock, _blockNumber](Error::Ptr _error, std::shared_ptr<std::string> _hash) {
                if (!_error || _error->errorCode() == CommonError::SUCCESS)
                {
                    if (_hash && !_hash->empty())
                    {
                        _onGetBlock(nullptr, HashType(*_hash));
                    }
                    else
                    {
                        LEDGER_LOG(ERROR)
                            << LOG_BADGE("asyncGetBlockHashByNumber")
                            << LOG_DESC("get a empty hash") << LOG_KV("blockNumber", _blockNumber);

                        // TODO: add error code
                        auto error =
                            std::make_shared<Error>(-1, "getBlockHashByNumber callback empty hash");
                        _onGetBlock(error, HashType(""));
                    }
                }
                else
                {
                    LEDGER_LOG(ERROR) << LOG_BADGE("asyncGetBlockHashByNumber")
                                      << LOG_DESC("error happened in open table or get entry")
                                      << LOG_KV("errorCode", _error->errorCode())
                                      << LOG_KV("errorMsg", _error->errorMessage())
                                      << LOG_KV("blockNumber", _blockNumber);
                    // TODO: add error code and msg
                    auto error = std::make_shared<Error>(_error->errorCode(),
                        "getBlockHashByNumber callback error" + _error->errorMessage());
                    _onGetBlock(error, HashType(""));
                }
            });
    });
}

void Ledger::asyncGetBlockNumberByHash(const crypto::HashType& _blockHash,
    std::function<void(Error::Ptr, bcos::protocol::BlockNumber)> _onGetBlock)
{
    if (_blockHash == HashType(""))
    {
        LEDGER_LOG(FATAL) << LOG_BADGE("asyncGetBlockNumberByHash")
                          << LOG_DESC("Error parameters");
        // TODO: add error code
        auto error = std::make_shared<Error>(-1, "empty hash in parameter");
        _onGetBlock(error, -1);
        return;
    }
    getStorageGetter()->getBlockNumberByHash(_blockHash.hex(), getMemoryTableFactory(0),
        [_blockHash, _onGetBlock](Error::Ptr _error, std::shared_ptr<std::string> _numberStr) {
            if ((!_error || _error->errorCode() == CommonError::SUCCESS))
            {
                if (_numberStr && !_numberStr->empty())
                {
                    _onGetBlock(nullptr, boost::lexical_cast<BlockNumber>(*_numberStr));
                }
                else
                {
                    LEDGER_LOG(WARNING) << LOG_BADGE("asyncGetBlockNumberByHash")
                                        << LOG_DESC("get number error, number is null or empty")
                                        << LOG_KV("blockHash", _blockHash.hex());
                    // TODO: add error code
                    auto error = std::make_shared<Error>(-1, "get number error");
                    _onGetBlock(error, -1);
                }
            }
            else
            {
                LEDGER_LOG(ERROR) << LOG_BADGE("asyncGetBlockNumberByHash")
                                  << LOG_DESC("error happened in open table or get entry")
                                  << LOG_KV("errorCode", _error->errorCode())
                                  << LOG_KV("errorMsg", _error->errorMessage())
                                  << LOG_KV("blockHash", _blockHash.hex());

                // TODO: add error code
                auto error = std::make_shared<Error>(_error->errorCode(),
                    "getBlockNumberByHash callback error" + _error->errorMessage());
                _onGetBlock(error, -1);
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
        LEDGER_LOG(FATAL) << LOG_BADGE("asyncGetBatchTxsByHashList")
                          << LOG_DESC("Error parameters");
        // TODO: add error code
        auto error = std::make_shared<Error>(-1, "nullptr in parameters");
        _onGetTx(error, nullptr, nullptr);
        return;
    }
    auto txHashStrList = std::make_shared<std::vector<std::string>>();
    for (auto& txHash : *_txHashList)
    {
        txHashStrList->emplace_back(txHash.hex());
    }
    getStorageGetter()->getBatchTxByHashList(txHashStrList, getMemoryTableFactory(0),
        getTransactionFactory(),
        [this, _txHashList, _withProof, _onGetTx](Error::Ptr _error, TransactionsPtr _txList) {
            if (!_error || _error->errorCode() == CommonError::SUCCESS)
            {
                if (_txList && _txHashList->size() == _txList->size())
                {
                    if (_withProof)
                    {
                        getBatchTxProof(_txHashList,
                            [_txHashList, _onGetTx, _txList](Error::Ptr _error,
                                std::shared_ptr<std::map<std::string, MerkleProofPtr>> _proof) {
                                if (_error && _error->errorCode() != CommonError::SUCCESS)
                                {
                                    LEDGER_LOG(ERROR) << LOG_BADGE("asyncGetBatchTxsByHashList")
                                                      << LOG_DESC("fetch tx proof error")
                                                      << LOG_KV("errorCode", _error->errorCode())
                                                      << LOG_KV("errorMsg", _error->errorMessage());
                                    // TODO: add error code
                                    auto error = std::make_shared<Error>(_error->errorCode(),
                                        "getBatchTxProof callback error" + _error->errorMessage());
                                    _onGetTx(error, _txList, nullptr);
                                    return;
                                }
                                if (!_proof)
                                {
                                    LEDGER_LOG(ERROR)
                                        << LOG_BADGE("asyncGetBatchTxsByHashList")
                                        << LOG_DESC("fetch proof is null")
                                        << LOG_KV("txHashListSize", _txHashList->size());
                                    auto error = std::make_shared<Error>(-1, "empty txs");
                                    _onGetTx(error, _txList, nullptr);
                                    return;
                                }
                                LEDGER_LOG(INFO) << LOG_BADGE("asyncGetBatchTxsByHashList")
                                                 << LOG_DESC("get tx list and proofMap complete")
                                                 << LOG_KV("txHashListSize", _txHashList->size());
                                _onGetTx(nullptr, _txList, _proof);
                            });
                    }
                    else
                    {
                        LEDGER_LOG(INFO) << LOG_BADGE("asyncGetBatchTxsByHashList")
                                         << LOG_DESC("get tx list compelete")
                                         << LOG_KV("txHashListSize", _txHashList->size())
                                         << LOG_KV("withProof", _withProof);
                        _onGetTx(nullptr, _txList, nullptr);
                    }
                }
                else
                {
                    // TODO: add error code
                    LEDGER_LOG(ERROR) << LOG_BADGE("asyncGetBatchTxsByHashList")
                                      << LOG_DESC("getBatchTxByHashList callback tx list is wrong")
                                      << LOG_KV("txHashListSize", _txHashList->size())
                                      << LOG_KV("withProof", _withProof);
                    auto error =
                        std::make_shared<Error>(-1, "getBatchTxByHashList callback error txList");
                    _onGetTx(error, nullptr, nullptr);
                    return;
                }
            }
            else
            {
                LEDGER_LOG(ERROR) << LOG_BADGE("asyncGetBatchTxsByHashList")
                                  << LOG_DESC("getBatchTxByHashList callback error")
                                  << LOG_KV("errorCode", _error->errorCode())
                                  << LOG_KV("errorMsg", _error->errorMessage());
                auto error = std::make_shared<Error>(_error->errorCode(),
                    "getBatchTxByHashList callback error" + _error->errorMessage());
                _onGetTx(error, nullptr, nullptr);
            }
        });
}

void Ledger::asyncGetTransactionReceiptByHash(bcos::crypto::HashType const& _txHash,
    bool _withProof,
    std::function<void(Error::Ptr, bcos::protocol::TransactionReceipt::ConstPtr, MerkleProofPtr)>
        _onGetTx)
{
    if (_txHash == HashType(""))
    {
        LEDGER_LOG(FATAL) << LOG_BADGE("asyncGetTransactionReceiptByHash")
                          << LOG_DESC("Error parameters");
        // TODO: add error code
        auto error = std::make_shared<Error>(-1, "empty hash in parameter");
        _onGetTx(error, nullptr, nullptr);
        return;
    }
    getStorageGetter()->getReceiptByTxHash(_txHash.hex(), getMemoryTableFactory(0),
        [this, _txHash, _withProof, _onGetTx](Error::Ptr _error, std::shared_ptr<std::string> _receiptStr) {
            if (_error && _error->errorCode() != CommonError::SUCCESS)
            {
                LEDGER_LOG(ERROR) << LOG_BADGE("asyncGetTransactionReceiptByHash")
                                  << LOG_DESC("getReceiptByTxHash callback error")
                                  << LOG_KV("errorCode", _error->errorCode())
                                  << LOG_KV("errorMsg", _error->errorMessage());
                // TODO: add error code
                auto error = std::make_shared<Error>(_error->errorCode(),
                                                     "getReceiptByTxHash callback error" + _error->errorMessage());
                _onGetTx(error, nullptr, nullptr);
                return;
            }
            if (!_receiptStr || _receiptStr->empty())
            {
                LEDGER_LOG(ERROR) << LOG_BADGE("asyncGetTransactionReceiptByHash")
                                  << LOG_DESC("getReceiptByTxHash callback empty receipt")
                                  << LOG_KV("txHash", _txHash.hex());
                // TODO: add error code
                auto error = std::make_shared<Error>(-1, "empty receipt");
                _onGetTx(error, nullptr, nullptr);
                return;
            }
            auto receipt = decodeReceipt(getReceiptFactory(), *_receiptStr);
            if (_withProof)
            {
                getReceiptProof(
                    receipt, [receipt, _onGetTx](Error::Ptr _error, MerkleProofPtr _proof) {
                        if (_error && _error->errorCode() != CommonError::SUCCESS)
                        {
                            LEDGER_LOG(ERROR) << LOG_BADGE("asyncGetTransactionReceiptByHash")
                                              << LOG_DESC("getTxsFromStorage callback error")
                                              << LOG_KV("errorCode", _error->errorCode())
                                              << LOG_KV("errorMsg", _error->errorMessage());
                            // TODO: add error code
                            auto error = std::make_shared<Error>(_error->errorCode(),
                                "getTxsFromStorage callback error" + _error->errorMessage());
                            _onGetTx(error, receipt, nullptr);
                            return;
                        }
                        if (!_proof)
                        {
                            // TODO: add error code
                            LEDGER_LOG(ERROR) << LOG_BADGE("asyncGetTransactionReceiptByHash")
                                              << LOG_DESC("getReceiptProof callback empty proof");
                            auto error = std::make_shared<Error>(-1, "null proof");
                            _onGetTx(error, receipt, nullptr);
                            return;
                        }
                        _onGetTx(nullptr, receipt, _proof);
                    });
            }
            else
            {
                LEDGER_LOG(TRACE) << LOG_BADGE("asyncGetTransactionReceiptByHash")
                                  << LOG_DESC("call back receipt");
                _onGetTx(nullptr, receipt, nullptr);
            }
        });
}

void Ledger::asyncGetTotalTransactionCount(
    std::function<void(Error::Ptr, int64_t, int64_t, bcos::protocol::BlockNumber)> _callback)
{
    std::promise<int64_t> countPromise;
    std::promise<int64_t> failedPromise;
    std::promise<BlockNumber> numberPromise;
    auto countFuture = countPromise.get_future();
    auto failedFuture = failedPromise.get_future();
    auto numberFuture = numberPromise.get_future();
    getStorageGetter()->getCurrentState(SYS_KEY_TOTAL_TRANSACTION_COUNT, getMemoryTableFactory(0),
        [&countPromise](Error::Ptr _error, std::shared_ptr<std::string> totalCountStr) {
            if ((!_error || _error->errorCode() == CommonError::SUCCESS) && totalCountStr && !totalCountStr->empty())
            {
                auto totalCount = boost::lexical_cast<int64_t>(*totalCountStr);
                countPromise.set_value(totalCount);
                return;
            }
            LEDGER_LOG(ERROR) << LOG_BADGE("asyncGetTransactionReceiptByHash")
                              << LOG_DESC("error happened in get SYS_KEY_TOTAL_TRANSACTION_COUNT");
            countPromise.set_value(-1);
        });
    getStorageGetter()->getCurrentState(SYS_KEY_TOTAL_FAILED_TRANSACTION, getMemoryTableFactory(0),
        [&failedPromise](
            Error::Ptr _error, std::shared_ptr<std::string> totalFailedStr) {
            if ((!_error || _error->errorCode() == CommonError::SUCCESS) && totalFailedStr && !totalFailedStr->empty())
            {
                auto totalFailed = boost::lexical_cast<int64_t>(*totalFailedStr);
                failedPromise.set_value(totalFailed);
                return;
            }
            LEDGER_LOG(ERROR) << LOG_BADGE("asyncGetTransactionReceiptByHash")
                              << LOG_DESC("error happened in get SYS_KEY_TOTAL_FAILED_TRANSACTION");
            failedPromise.set_value(-1);
        });
    getLatestBlockNumber([&numberPromise](BlockNumber _number) {
        numberPromise.set_value(_number);
    });
    auto totalCount = countFuture.get();
    auto totalFailed = failedFuture.get();
    auto number = numberFuture.get();
    if (totalCount == -1 || totalFailed == -1 || number == -1)
    {
        LEDGER_LOG(ERROR) << LOG_BADGE("asyncGetTransactionReceiptByHash")
                          << LOG_DESC("error happened in get total tx count");
        // TODO: add error code
        auto error = std::make_shared<Error>(
            -1, "timeout to fetch all data in asyncGetTotalTransactionCount");
        _callback(error, -1, -1, -1);
        return;
    }
    _callback(nullptr, totalCount, totalFailed, number);
}

void Ledger::asyncGetSystemConfigByKey(const std::string& _key,
    std::function<void(Error::Ptr, std::string, bcos::protocol::BlockNumber)> _onGetConfig)
{
    getStorageGetter()->getSysConfig(_key, getMemoryTableFactory(0),
        [_key, _onGetConfig](Error::Ptr _error, std::string _value, std::string _number) {
            if (!_error || _error->errorCode() == CommonError::SUCCESS)
            {
                if (!_value.empty() && !_number.empty())
                {
                    LEDGER_LOG(TRACE)
                        << LOG_BADGE("asyncGetSystemConfigByKey") << LOG_DESC("get config in db")
                        << LOG_KV("key", _key) << LOG_KV("value", _value);
                    _onGetConfig(
                        nullptr, _value, boost::lexical_cast<BlockNumber>(_number));
                }
                else
                {
                    LEDGER_LOG(ERROR)
                        << LOG_BADGE("asyncGetSystemConfigByKey")
                        << LOG_DESC("Null pointer of getSysConfig") << LOG_KV("key", _key);
                    // TODO: add error code
                    auto error = std::make_shared<Error>(-1, "get null config");
                    _onGetConfig(error, "", -1);
                }
            }
            else
            {
                LEDGER_LOG(ERROR) << LOG_BADGE("asyncGetSystemConfigByKey")
                                  << LOG_DESC("getSysConfig callback error")
                                  << LOG_KV("errorCode", _error->errorCode())
                                  << LOG_KV("errorMsg", _error->errorMessage());
                // TODO: add error code
                auto error = std::make_shared<Error>(
                    _error->errorCode(), "getSysConfig callback error" + _error->errorMessage());
                _onGetConfig(error, "", -1);
            }
        });
}

void Ledger::asyncGetNonceList(bcos::protocol::BlockNumber _startNumber, int64_t _offset,
    std::function<void(Error::Ptr, std::shared_ptr<std::map<protocol::BlockNumber, protocol::NonceListPtr>>)>
        _onGetList)
{
    getLatestBlockNumber([_startNumber, _offset, _onGetList, this](protocol::BlockNumber _number) {
      if (_startNumber < 0 || _offset < 0 || _startNumber > _number)
      {
          LEDGER_LOG(FATAL) << LOG_BADGE("asyncGetNonceList")
                            << LOG_DESC("Error parameters");
          // TODO: to add errorCode
          auto error = std::make_shared<Error>(-1, "error parameter");
          _onGetList(error, nullptr);
          return;
      }
      auto endNumber =
          (_startNumber + _offset > _number) ? _number : (_startNumber + _offset);
        getStorageGetter()->getNoncesBatchFromStorage(_startNumber, endNumber,
            getMemoryTableFactory(0), m_blockFactory,
            [_startNumber, endNumber, _onGetList](Error::Ptr _error,
                std::shared_ptr<std::map<protocol::BlockNumber, protocol::NonceListPtr>> _nonceMap) {
              if (!_error || _error->errorCode() == CommonError::SUCCESS)
              {
                  if (_nonceMap && _nonceMap->size() == size_t(endNumber - _startNumber + 1))
                  {
                      LEDGER_LOG(TRACE)
                          << LOG_BADGE("asyncGetNonceList") << LOG_DESC("get nonceList enough")
                          << LOG_KV("listSize", endNumber - _startNumber + 1);
                      _onGetList(nullptr, _nonceMap);
                  }
                  else
                  {
                      LEDGER_LOG(ERROR)
                          << LOG_BADGE("asyncGetNonceList") << LOG_DESC("not get enough nonceLists")
                          << LOG_KV("startBlockNumber", _startNumber)
                          << LOG_KV("endBlockNumber", endNumber);
                      // TODO: add error code
                      auto error = std::make_shared<Error>(-1, "not get enough nonce list");
                      _onGetList(error, nullptr);
                  }
              }
              else
              {
                  LEDGER_LOG(ERROR) << LOG_BADGE("asyncGetNonceList")
                                    << LOG_DESC("error happened in open table or get entry");
                  auto error = std::make_shared<Error>(_error->errorCode(),
                      "getNoncesBatchFromStorage callback error" + _error->errorMessage());
                  _onGetList(error, nullptr);
              }
          });
    });
}

void Ledger::asyncGetNodeListByType(const std::string& _type,
    std::function<void(Error::Ptr, consensus::ConsensusNodeListPtr)> _onGetConfig)
{
    if (_type != CONSENSUS_SEALER && _type != CONSENSUS_OBSERVER)
    {
        LEDGER_LOG(FATAL) << LOG_BADGE("asyncGetNodeListByType")
                          << LOG_DESC("Error parameters");
        // TODO: to add errorCode
        auto error = std::make_shared<Error>(-1, "error type");
        _onGetConfig(error, nullptr);
        return;
    }
    getLatestBlockNumber([this, _type, _onGetConfig](BlockNumber _number) {
        getStorageGetter()->getConsensusConfig(_type, _number, getMemoryTableFactory(0),
            m_blockFactory->cryptoSuite()->keyFactory(),
            [_number, _onGetConfig](Error::Ptr _error, consensus::ConsensusNodeListPtr _nodeList) {
                if (!_error || _error->errorCode() == CommonError::SUCCESS)
                {
                    if (_nodeList && !_nodeList->empty())
                    {
                        _onGetConfig(nullptr, _nodeList);
                    }
                    else
                    {
                        LEDGER_LOG(ERROR) << LOG_BADGE("asyncGetNodeListByType")
                                          << LOG_DESC("getConsensusConfig error, null node list");
                        // TODO: add error code
                        auto error = std::make_shared<Error>(-1, "null node list");
                        _onGetConfig(error, nullptr);
                    }
                }
                else
                {
                    LEDGER_LOG(ERROR) << LOG_BADGE("asyncGetNodeListByType")
                                      << LOG_DESC("error happened in open table or get entry")
                                      << LOG_KV("errorCode", _error->errorCode())
                                      << LOG_KV("errorMsg", _error->errorMessage())
                                      << LOG_KV("blockNumber", _number);
                    auto error = std::make_shared<Error>(_error->errorCode(),
                        "getConsensusConfig callback error" + _error->errorMessage());
                    _onGetConfig(error, nullptr);
                }
            });
    });
}

void Ledger::getBlock(const BlockNumber& _blockNumber, int32_t _blockFlag,
    std::function<void(Error::Ptr, protocol::Block::Ptr)> _onGetBlock)
{
    auto cachedBlock = m_blockCache.get(_blockNumber);
    /// flag of whether get a single part of block
    /// if true, then it can callback immediately
    bool singlePartFlag = (_blockFlag & (_blockFlag - 1)) == 0;

    if (bool(cachedBlock.second))
    {
        LEDGER_LOG(TRACE) << LOG_BADGE("getBlock") << LOG_DESC("Cache hit, read from cache")
                          << LOG_KV("blockNumber", _blockNumber);
        _onGetBlock(nullptr, cachedBlock.second);
        return;
    }
    LEDGER_LOG(TRACE) << LOG_BADGE("getBlock") << LOG_DESC("Cache missed, read from storage")
                      << LOG_KV("blockNumber", _blockNumber);
    auto block = m_blockFactory->createBlock();
    int32_t fetchFlag = 0;
    std::promise<bool> headerPromise;
    std::promise<bool> txsPromise;
    std::promise<bool> receiptsPromise;
    auto headerFuture = headerPromise.get_future();
    auto txsFuture = txsPromise.get_future();
    auto receiptsFuture = receiptsPromise.get_future();
    if (_blockFlag & HEADER)
    {
        getBlockHeader(_blockNumber, [&headerPromise, _onGetBlock, _blockNumber, singlePartFlag,
                                         block](Error::Ptr _error, BlockHeader::Ptr _header) {
            if ((!_error || _error->errorCode() == CommonError::SUCCESS) && _header)
            {
                block->setBlockHeader(_header);
                if (singlePartFlag)
                {
                    _onGetBlock(nullptr, block);
                }
                else
                {
                    headerPromise.set_value(true);
                }
                return;
            }
            LEDGER_LOG(ERROR) << LOG_BADGE("getBlock")
                              << LOG_DESC("Can't find the header, callback error")
                              << LOG_KV("errorCode", _error->errorCode())
                              << LOG_KV("errorMsg", _error->errorMessage())
                              << LOG_KV("blockNumber", _blockNumber);
            if (singlePartFlag)
            {
                auto error = std::make_shared<Error>(
                    _error->errorCode(), "getBlockHeader callback error " + _error->errorMessage());
                _onGetBlock(error, nullptr);
                return;
            }
            headerPromise.set_value(false);
        });
    }
    if (_blockFlag & TRANSACTIONS)
    {
        getTxs(_blockNumber, [_blockNumber, &txsPromise, _onGetBlock, block, singlePartFlag](
                                 Error::Ptr _error, bcos::protocol::TransactionsPtr _txs) {
            if ((!_error || _error->errorCode() == CommonError::SUCCESS) && _txs)
            {
                auto insertSize = blockTransactionListSetter(block, _txs);
                LEDGER_LOG(TRACE) << LOG_BADGE("getBlock") << LOG_DESC("insert block transactions")
                                  << LOG_KV("txsSize", _txs->size())
                                  << LOG_KV("insertSize", insertSize)
                                  << LOG_KV("blockNumber", _blockNumber);
                if (singlePartFlag)
                {
                    _onGetBlock(nullptr, block);
                }
                else
                {
                    txsPromise.set_value(true);
                }
                return;
            }
            LEDGER_LOG(ERROR) << LOG_BADGE("getBlock")
                              << LOG_DESC("Can't find the Txs, callback error")
                              << LOG_KV("errorCode", _error->errorCode())
                              << LOG_KV("errorMsg", _error->errorMessage())
                              << LOG_KV("blockNumber", _blockNumber);
            if (singlePartFlag)
            {
                auto error = std::make_shared<Error>(
                    _error->errorCode(), "getBlockTxs callback error " + _error->errorMessage());
                _onGetBlock(error, nullptr);
                return;
            }
            txsPromise.set_value(false);
        });
    }
    if (_blockFlag & RECEIPTS)
    {
        getReceipts(_blockNumber, [_blockNumber, &receiptsPromise, _onGetBlock, block, singlePartFlag](
                                      Error::Ptr _error, protocol::ReceiptsPtr _receipts) {
            if ((!_error || _error->errorCode() == CommonError::SUCCESS) && _receipts)
            {
                auto insertSize = blockReceiptListSetter(block, _receipts);
                LEDGER_LOG(TRACE) << LOG_BADGE("getBlock")
                                  << LOG_DESC("insert block receipts")
                                  << LOG_KV("txsSize", _receipts->size())
                                  << LOG_KV("insertSize", insertSize)
                                  << LOG_KV("blockNumber", _blockNumber);
                if (singlePartFlag)
                {
                    _onGetBlock(nullptr, block);
                }
                else
                {
                    receiptsPromise.set_value(true);
                }
                return;
            }
            LEDGER_LOG(ERROR) << LOG_BADGE("getBlock") << LOG_DESC("Can't find the Receipts")
                              << LOG_KV("errorCode", _error->errorCode())
                              << LOG_KV("errorMsg", _error->errorMessage())
                              << LOG_KV("blockNumber", _blockNumber);
            if (singlePartFlag)
            {
                auto error = std::make_shared<Error>(
                    _error->errorCode(), "getBlockReceipts callback error " + _error->errorMessage());
                _onGetBlock(error, nullptr);
                return;
            }
            receiptsPromise.set_value(false);
        });
    }
    // it means _blockFlag has multiple 1 in binary
    // should wait for all datum callback
    if (!singlePartFlag)
    {
        bool headerFlag = true;
        bool txsFlag = true;
        bool receiptFlag = true;
        if (_blockFlag & HEADER)
        {
            headerFlag = std::future_status::ready ==
                                 headerFuture.wait_for(std::chrono::milliseconds(m_timeout)) ?
                             headerFuture.get() :
                             false;
        }
        if (_blockFlag & TRANSACTIONS)
        {
            txsFlag = std::future_status::ready ==
                              txsFuture.wait_for(std::chrono::milliseconds(m_timeout)) ?
                          txsFuture.get() :
                          false;
        }
        if (_blockFlag & RECEIPTS)
        {
            receiptFlag = std::future_status::ready ==
                                  receiptsFuture.wait_for(std::chrono::milliseconds(m_timeout)) ?
                              receiptsFuture.get() :
                              false;
        }
        if (headerFlag && txsFlag && receiptFlag)
        {
            if (!(_blockFlag ^ FULL_BLOCK))
            {
                // get full block data
                LEDGER_LOG(TRACE) << LOG_BADGE("getBlock") << LOG_DESC("Write to cache");
                m_blockCache.add(_blockNumber, block);
            }
            _onGetBlock(nullptr, block);
            return;
        }
        auto error = std::make_shared<Error>(-1, "some data fetch failed");
            _onGetBlock(error, nullptr);
    }
}

void Ledger::getLatestBlockNumber(std::function<void(protocol::BlockNumber)> _onGetNumber)
{
    getStorageGetter()->getCurrentState(SYS_KEY_CURRENT_NUMBER, getMemoryTableFactory(0),
        [_onGetNumber](Error::Ptr _error, std::shared_ptr<std::string> _currentNumber) {
            if (!_error || _error->errorCode() == CommonError::SUCCESS)
            {
                if (_currentNumber && !_currentNumber->empty())
                {
                    auto number = boost::lexical_cast<BlockNumber>(*_currentNumber);
                    _onGetNumber(number);
                }
                else
                {
                    LEDGER_LOG(ERROR)
                        << LOG_BADGE("getLatestBlockNumber") << LOG_DESC("get a empty number")
                        << LOG_KV("time", utcTime());
                    _onGetNumber(-1);
                }
            }
            else
            {
                LEDGER_LOG(ERROR) << LOG_BADGE("getLatestBlockNumber")
                                  << LOG_DESC("Get number from storage error")
                                  << LOG_KV("errorCode", _error->errorCode())
                                  << LOG_KV("errorMsg", _error->errorMessage());
                _onGetNumber(-1);
            }
        });
}

void Ledger::getBlockHeader(const bcos::protocol::BlockNumber& _blockNumber,
    std::function<void(Error::Ptr, BlockHeader::Ptr)> _onGetHeader)
{
    auto start_time = utcTime();
    auto record_time = utcTime();
    auto cachedBlock = m_blockCache.get(_blockNumber);
    auto cachedHeader = m_blockHeaderCache.get(_blockNumber);
    auto getCache_time_cost = utcTime() - record_time;
    record_time = utcTime();

    if (bool(cachedHeader.second))
    {
        LEDGER_LOG(TRACE) << LOG_BADGE("getBlockHeader")
                          << LOG_DESC("CacheHeader hit, read from cache")
                          << LOG_KV("blockNumber", _blockNumber);
        _onGetHeader(nullptr, cachedHeader.second);
    }
    else
    {
        LEDGER_LOG(TRACE) << LOG_BADGE("getBlockHeader")
                          << LOG_DESC("Cache missed, read from storage")
                          << LOG_KV("blockNumber", _blockNumber);
        getStorageGetter()->getBlockHeaderFromStorage(_blockNumber, getMemoryTableFactory(0),
            [this, _onGetHeader, _blockNumber](Error::Ptr _error, std::shared_ptr<std::string> _headerStr) {
                if (!_error || _error->errorCode() == CommonError::SUCCESS)
                {
                    if (_headerStr && !_headerStr->empty())
                    {
                        auto headerPtr = decodeBlockHeader(getBlockHeaderFactory(), *_headerStr);
                        LEDGER_LOG(TRACE) << LOG_BADGE("getBlockHeader")
                                          << LOG_DESC("Get header from storage")
                                          << LOG_KV("blockNumber", _blockNumber);
                        LEDGER_LOG(TRACE)
                            << LOG_BADGE("getBlockHeader") << LOG_DESC("Write to cache");
                        m_blockHeaderCache.add(_blockNumber, headerPtr);
                        _onGetHeader(nullptr, headerPtr);
                    }
                    else
                    {
                        LEDGER_LOG(ERROR)
                            << LOG_BADGE("getBlockHeader")
                            << LOG_DESC("Get header from storage is empty")
                            << LOG_KV("blockNumber", _blockNumber);
                        // TODO: add error code
                        auto error = std::make_shared<Error>(
                            -1, "getBlockHeaderFromStorage callback null header");
                        _onGetHeader(error, nullptr);
                    }
                }
                else
                {
                    LEDGER_LOG(ERROR) << LOG_BADGE("getBlockHeader")
                                      << LOG_DESC("Get header from storage error")
                                      << LOG_KV("errorCode", _error->errorCode())
                                      << LOG_KV("errorMsg", _error->errorMessage())
                                      << LOG_KV("blockNumber", _blockNumber);
                    auto error = std::make_shared<Error>(_error->errorCode(),
                        "getBlockHeaderFromStorage callback error" + _error->errorMessage());
                    _onGetHeader(error, nullptr);
                }
            });
        auto storage_getter_time = utcTime() - record_time;
        record_time = utcTime();
        LEDGER_LOG(DEBUG) << LOG_BADGE("getBlockHeader") << LOG_DESC("Get Header from db")
                          << LOG_KV("getCacheTimeCost", getCache_time_cost)
                          << LOG_KV("storageGetterTimeCost", storage_getter_time)
                          << LOG_KV("totalTimeCost", utcTime() - start_time);
    }
}

void Ledger::getTxs(const bcos::protocol::BlockNumber& _blockNumber,
    std::function<void(Error::Ptr, bcos::protocol::TransactionsPtr)> _onGetTxs)
{
    auto start_time = utcTime();
    auto record_time = utcTime();
    auto cachedBlock = m_blockCache.get(_blockNumber);
    auto cachedTransactions = m_transactionsCache.get(_blockNumber);
    auto getCache_time_cost = utcTime() - record_time;
    record_time = utcTime();
    if (bool(cachedTransactions.second))
    {
        LEDGER_LOG(TRACE) << LOG_BADGE("getTxs") << LOG_DESC("CacheTxs hit, read from cache")
                          << LOG_KV("blockNumber", _blockNumber);
        _onGetTxs(nullptr, cachedTransactions.second);
    }
    else
    {
        LEDGER_LOG(TRACE) << LOG_BADGE("getTxs") << LOG_DESC("Cache missed, read from storage")
                          << LOG_KV("blockNumber", _blockNumber);
        // block with tx hash list
        getStorageGetter()->getTxsFromStorage(_blockNumber, getMemoryTableFactory(0),
            [this, _onGetTxs, _blockNumber](Error::Ptr _error, std::shared_ptr<std::string> _blockStr) {
                if (!_error || _error->errorCode() == CommonError::SUCCESS)
                {
                    if(_blockStr && !_blockStr->empty())
                    {
                        auto block = decodeBlock(m_blockFactory, *_blockStr);
                        auto txHashList = blockTxHashListGetter(block);
                        getStorageGetter()->getBatchTxByHashList(txHashList,
                            getMemoryTableFactory(0), getTransactionFactory(),
                            [this, _onGetTxs, _blockNumber, txHashList](Error::Ptr _error, protocol::TransactionsPtr _txs) {
                                if (!_error || _error->errorCode() == CommonError::SUCCESS)
                                {
                                    if(_txs && txHashList->size() == _txs->size())
                                    {
                                        LEDGER_LOG(TRACE)
                                            << LOG_BADGE("getTxs")
                                            << LOG_DESC("Get txs from storage")
                                            << LOG_KV("txsSize", _txs->size());
                                        LEDGER_LOG(TRACE)
                                            << LOG_BADGE("getTxs") << LOG_DESC("Write to cache");
                                        m_transactionsCache.add(_blockNumber, _txs);
                                        _onGetTxs(nullptr, _txs);
                                    }
                                    else
                                    {
                                        LEDGER_LOG(ERROR)
                                            << LOG_BADGE("getTxs")
                                            << LOG_DESC("getBatchTxByHashList get error txs")
                                            << LOG_KV("txHashListSize", txHashList->size());
                                        // TODO: add error code and msg
                                        auto error = std::make_shared<Error>(-1, "getBatchTxByHashList error ");
                                        _onGetTxs(error, nullptr);
                                    }
                                }
                                else
                                {
                                    LEDGER_LOG(ERROR) << LOG_BADGE("getTxs")
                                                      << LOG_DESC("Get txs from storage error")
                                                      << LOG_KV("errorCode", _error->errorCode())
                                                      << LOG_KV("errorMsg", _error->errorMessage())
                                                      << LOG_KV("txsSize", _txs->size());
                                    auto error = std::make_shared<Error>(
                                        _error->errorCode(), "getBatchTxByHashList callback error" + _error->errorMessage());
                                    _onGetTxs(error, nullptr);
                                }
                            });
                    }
                    else
                    {
                        LEDGER_LOG(ERROR) << LOG_BADGE("getTxs")
                                          << LOG_DESC("getTxsFromStorage get error block")
                                          << LOG_KV("blockNumber", _blockNumber);
                        // TODO: add error code
                        auto error =
                            std::make_shared<Error>(-1, "getTxsFromStorage get error block");
                        _onGetTxs(error, nullptr);
                    }
                }
                else
                {
                    LEDGER_LOG(ERROR) << LOG_BADGE("getTxs")
                                      << LOG_DESC("Get txHashList from storage error")
                                      << LOG_KV("errorCode", _error->errorCode())
                                      << LOG_KV("errorMsg", _error->errorMessage())
                                      << LOG_KV("blockNumber", _blockNumber);
                    auto error = std::make_shared<Error>(_error->errorCode(),
                        "getTxsFromStorage callback error" + _error->errorMessage());
                    _onGetTxs(error, nullptr);
                }
            });
        auto decode_txs_time_cost = utcTime() - record_time;
        LEDGER_LOG(DEBUG) << LOG_BADGE("getTxs") << LOG_DESC("Get Txs from db")
                          << LOG_KV("getCacheTimeCost", getCache_time_cost)
                          << LOG_KV("decodeTxsTimeCost", decode_txs_time_cost)
                          << LOG_KV("totalTimeCost", utcTime() - start_time);
    }
}

void Ledger::getReceipts(const bcos::protocol::BlockNumber& _blockNumber,
    std::function<void(Error::Ptr, bcos::protocol::ReceiptsPtr)> _onGetReceipts)
{
    auto start_time = utcTime();
    auto record_time = utcTime();
    auto cachedBlock = m_blockCache.get(_blockNumber);
    auto cachedReceipts = m_receiptCache.get(_blockNumber);
    auto getCache_time_cost = utcTime() - record_time;
    record_time = utcTime();
    if (bool(cachedReceipts.second))
    {
        LEDGER_LOG(TRACE) << LOG_BADGE("getReceipts")
                          << LOG_DESC("Cache Receipts hit, read from cache")
                          << LOG_KV("blockNumber", _blockNumber);
        _onGetReceipts(nullptr, cachedReceipts.second);
    }
    else
    {
        LEDGER_LOG(TRACE) << LOG_BADGE("getReceipts")
                          << LOG_DESC("Cache missed, read from storage")
                          << LOG_KV("blockNumber", _blockNumber);
        // block with tx hash list
        getStorageGetter()->getTxsFromStorage(_blockNumber, getMemoryTableFactory(0),
            [this, _onGetReceipts, _blockNumber](Error::Ptr _error, std::shared_ptr<std::string> _blockStr) {
                if (!_error || _error->errorCode() == CommonError::SUCCESS)
                {
                    if (_blockStr && !_blockStr->empty())
                    {
                        auto block = decodeBlock(m_blockFactory, *_blockStr);

                        auto txHashList = blockTxHashListGetter(block);
                        getStorageGetter()->getBatchReceiptsByHashList(txHashList,
                            getMemoryTableFactory(0), getReceiptFactory(),
                            [=](Error::Ptr _error, ReceiptsPtr _receipts) {
                                if (!_error || _error->errorCode() == CommonError::SUCCESS)
                                {
                                    if(_receipts && _receipts->size() == txHashList->size())
                                    {
                                        LEDGER_LOG(TRACE)
                                            << LOG_BADGE("getReceipts")
                                            << LOG_DESC("Get receipts from storage")
                                            << LOG_KV("receiptSize", _receipts->size());

                                        LEDGER_LOG(TRACE) << LOG_BADGE("getReceipts")
                                                          << LOG_DESC("Write to cache");
                                        m_receiptCache.add(_blockNumber, _receipts);
                                        _onGetReceipts(nullptr, _receipts);
                                    }
                                    else
                                    {
                                        LEDGER_LOG(ERROR)
                                            << LOG_BADGE("getReceipts")
                                            << LOG_DESC("receipts is null or not enough")
                                            << LOG_KV("txHashListSize", txHashList->size());
                                        // TODO: add error code
                                        auto error = std::make_shared<Error>(-1, "get receipts is null or not enough");
                                        _onGetReceipts(error, nullptr);
                                    }
                                }
                                else
                                {
                                    LEDGER_LOG(ERROR) << LOG_BADGE("getReceipts")
                                                      << LOG_DESC("Get receipts from storage error")
                                                      << LOG_KV("errorCode", _error->errorCode())
                                                      << LOG_KV("errorMsg", _error->errorMessage())
                                                      << LOG_KV("blockNumber", _blockNumber);
                                    auto error = std::make_shared<Error>(_error->errorCode(),
                                        "getBatchReceiptsByHashList callback error" +
                                            _error->errorMessage());
                                    _onGetReceipts(error, nullptr);
                                }
                            });
                    }
                    else
                    {
                        LEDGER_LOG(ERROR)
                            << LOG_BADGE("getReceipts")
                            << LOG_DESC("getTxsFromStorage get txHashList error")
                            << LOG_KV("blockNumber", _blockNumber);
                        // TODO: add error code
                        auto error =
                            std::make_shared<Error>(-1, "getTxsFromStorage get empty block");
                        _onGetReceipts(error, nullptr);
                    }
                }
                else
                {
                    LEDGER_LOG(ERROR) << LOG_BADGE("getReceipts")
                                      << LOG_DESC("Get receipts from storage error")
                                      << LOG_KV("errorCode", _error->errorCode())
                                      << LOG_KV("errorMsg", _error->errorMessage())
                                      << LOG_KV("blockNumber", _blockNumber);
                    auto error = std::make_shared<Error>(_error->errorCode(),
                        "getTxsFromStorage callback error" + _error->errorMessage());
                    _onGetReceipts(error, nullptr);
                }
            });
        auto decode_receipts_time_cost = utcTime() - record_time;
        LEDGER_LOG(DEBUG) << LOG_BADGE("getReceipts") << LOG_DESC("Get Receipts from db")
                          << LOG_KV("getCacheTimeCost", getCache_time_cost)
                          << LOG_KV("decodeTxsTimeCost", decode_receipts_time_cost)
                          << LOG_KV("totalTimeCost", utcTime() - start_time);
    }
}

void Ledger::getTxProof(
    const HashType& _txHash, std::function<void(Error::Ptr, MerkleProofPtr)> _onGetProof)
{
    getStorageGetter()->getReceiptByTxHash(_txHash.hex(), getMemoryTableFactory(0),
        [this, _txHash, _onGetProof](Error::Ptr _error, std::shared_ptr<std::string> _receiptStr) {
            if (!_error || _error->errorCode() == CommonError::SUCCESS)
            {
                if (_receiptStr && !_receiptStr->empty())
                {
                    auto receipt = decodeReceipt(getReceiptFactory(), *_receiptStr);
                    auto blockNumber = receipt->blockNumber();
                    getTxs(blockNumber, [this, blockNumber, _onGetProof, _txHash](
                                            Error::Ptr _error, TransactionsPtr _txs) {
                        if (!_error || _error->errorCode() == CommonError::SUCCESS)
                        {
                            auto merkleProofPtr = std::make_shared<MerkleProof>();
                            if (_txs != nullptr && !_txs->empty())
                            {
                                auto parent2ChildList =
                                    m_merkleProofUtility->getParent2ChildListByTxsProofCache(
                                        blockNumber, _txs, m_blockFactory->cryptoSuite());
                                auto child2Parent = m_merkleProofUtility->getChild2ParentCacheByTransaction(
                                    parent2ChildList, blockNumber);
                                m_merkleProofUtility->getMerkleProof(
                                    _txHash, *parent2ChildList, *child2Parent, *merkleProofPtr);
                                LEDGER_LOG(TRACE) << LOG_BADGE("getTxProof")
                                                  << LOG_DESC("get merkle proof success")
                                                  << LOG_KV("blockNumber", blockNumber)
                                                  << LOG_KV("txHash", _txHash.hex());
                                _onGetProof(nullptr, merkleProofPtr);
                            }
                            else
                            {
                                LEDGER_LOG(ERROR)
                                    << LOG_BADGE("getTxProof") << LOG_DESC("get txs error")
                                    << LOG_KV("blockNumber", blockNumber)
                                    << LOG_KV("txHash", _txHash.hex());
                                // TODO: add error code
                                auto error =
                                    std::make_shared<Error>(-1, "getTxs callback empty txs");
                                _onGetProof(error, nullptr);
                            }
                        }
                        else
                        {
                            // TODO: add error msg
                            LEDGER_LOG(ERROR)
                                << LOG_BADGE("getTxProof") << LOG_DESC("getTxs callback error")
                                << LOG_KV("errorCode", _error->errorCode())
                                << LOG_KV("errorMsg", _error->errorMessage());
                            auto error = std::make_shared<Error>(_error->errorCode(),
                                "getTxs callback error" + _error->errorMessage());
                            _onGetProof(error, nullptr);
                        }
                    });
                }
                else
                {
                    LEDGER_LOG(TRACE) << LOG_BADGE("getTxProof")
                                      << LOG_DESC("receipt is null or empty")
                                      << LOG_KV("txHash", _txHash.hex());
                    // TODO: add error code
                    auto error =
                        std::make_shared<Error>(-1, "getReceiptByTxHash callback empty receipt");
                    _onGetProof(error, nullptr);
                }
            }
            else
            {
                LEDGER_LOG(ERROR) << LOG_BADGE("getTxProof")
                                  << LOG_DESC("getReceiptByTxHash from storage error")
                                  << LOG_KV("errorCode", _error->errorCode())
                                  << LOG_KV("errorMsg", _error->errorMessage())
                                  << LOG_KV("txHash", _txHash.hex());
                auto error = std::make_shared<Error>(_error->errorCode(),
                    "getReceiptByTxHash callback error" + _error->errorMessage());
                _onGetProof(error, nullptr);
            }
        });
}

void Ledger::getBatchTxProof(crypto::HashListPtr _txHashList,
    std::function<void(Error::Ptr, std::shared_ptr<std::map<std::string, MerkleProofPtr>>)>
        _onGetProof)
{
    std::atomic_bool fetchFlag = true;
    auto con_proofMap = std::make_shared<tbb::concurrent_unordered_map<std::string, MerkleProofPtr>>();
    tbb::parallel_for(tbb::blocked_range<size_t>(0, _txHashList->size()),
        [&](const tbb::blocked_range<size_t>& range) {
            for (size_t i = range.begin(); i < range.end(); ++i)
            {
                std::promise<bool> p;
                auto future = p.get_future();
                auto txHash = _txHashList->at(i);
                getTxProof(
                    txHash, [con_proofMap, txHash, &p](Error::Ptr _error, MerkleProofPtr _proof) {
                        if ((!_error || _error->errorCode() == CommonError::SUCCESS) && _proof)
                        {
                            con_proofMap->insert(std::make_pair(txHash.hex(), _proof));
                            p.set_value(true);
                        }
                    });
                if (std::future_status::ready !=
                    future.wait_until(
                        std::chrono::system_clock::now() + std::chrono::milliseconds(m_timeout)))
                {
                    LEDGER_LOG(ERROR)
                        << LOG_BADGE("getBatchTxProof") << LOG_DESC("getTxProof timeout");
                    fetchFlag = false;
                }
            }
        });
    if (!fetchFlag)
    {
        LEDGER_LOG(ERROR)
            << LOG_BADGE("getBatchTxProof")
            << LOG_DESC("fetch tx proof timeout")
            << LOG_KV("txHashListSize", _txHashList->size());
        // TODO: add error code and msg
        auto error = std::make_shared<Error>(-1, "");
        _onGetProof(error, nullptr);
        return;
    }
    auto proofMap = std::make_shared<std::map<std::string, MerkleProofPtr>>(
        con_proofMap->begin(), con_proofMap->end());
    LEDGER_LOG(INFO) << LOG_BADGE("asyncGetBatchTxsByHashList")
                     << LOG_DESC("get tx list and proofMap complete")
                     << LOG_KV("txHashListSize", _txHashList->size());
    _onGetProof(nullptr, proofMap);
}

void Ledger::getReceiptProof(
    protocol::TransactionReceipt::Ptr _receipt, std::function<void(Error::Ptr, MerkleProofPtr)> _onGetProof)
{
    getStorageGetter()->getTxsFromStorage(_receipt->blockNumber(), getMemoryTableFactory(0),
        [this, _onGetProof, _receipt](
            Error::Ptr _error, std::shared_ptr<std::string> _blockStr) {
            if (_error && _error->errorCode() != CommonError::SUCCESS)
            {
                LEDGER_LOG(ERROR) << LOG_BADGE("getReceiptProof")
                                  << LOG_DESC("getTxsFromStorage callback error")
                                  << LOG_KV("errorCode", _error->errorCode())
                                  << LOG_KV("errorMsg", _error->errorMessage());
                // TODO: add error code
                auto error = std::make_shared<Error>(_error->errorCode(),
                    "getTxsFromStorage callback error" + _error->errorMessage());
                _onGetProof(error, nullptr);
                return;
            }
            if (!_blockStr || _blockStr->empty())
            {
                // TODO: add error code
                LEDGER_LOG(ERROR) << LOG_BADGE("getReceiptProof")
                                  << LOG_DESC("getTxsFromStorage callback empty block txs");
                auto error = std::make_shared<Error>(-1, "empty txs");
                _onGetProof(error, nullptr);
                return;
            }
            auto block = decodeBlock(m_blockFactory, *_blockStr);
            auto txHashList = blockTxHashListGetter(block);
            getStorageGetter()->getBatchReceiptsByHashList(txHashList, getMemoryTableFactory(0),
                getReceiptFactory(),
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
                    LEDGER_LOG(INFO) << LOG_BADGE("getReceiptProof")
                                     << LOG_DESC("call back receipt and proof");
                    _onGetProof(nullptr, merkleProof);
                });
        });
}

LedgerConfig::Ptr Ledger::getLedgerConfig(protocol::BlockNumber _number, const crypto::HashType& _hash){
    auto ledgerConfig = std::make_shared<LedgerConfig>();
    std::atomic_bool asyncRet = {true};
    ledgerConfig->setBlockNumber(_number);
    ledgerConfig->setHash(_hash);
    std::promise<std::string> timeoutPromise;
    std::promise<std::string> countLimitPromise;
    std::promise<consensus::ConsensusNodeListPtr> sealerPromise;
    std::promise<consensus::ConsensusNodeListPtr> observerPromise;
    auto timeoutFuture = timeoutPromise.get_future();
    auto countLimitFuture = countLimitPromise.get_future();
    auto sealerFuture = sealerPromise.get_future();
    auto observerFuture = observerPromise.get_future();

    asyncGetSystemConfigByKey(SYSTEM_KEY_CONSENSUS_TIMEOUT,
        [&timeoutPromise](Error::Ptr _error, std::string _value, BlockNumber) {
            if (_error && _error->errorCode() != CommonError::SUCCESS)
            {
                LEDGER_LOG(ERROR) << LOG_BADGE("getLedgerConfig")
                                  << LOG_DESC("asyncGetSystemConfigByKey callback error")
                                  << LOG_KV("getKey", SYSTEM_KEY_CONSENSUS_TIMEOUT)
                                  << LOG_KV("errorCode", _error->errorCode())
                                  << LOG_KV("errorMsg", _error->errorMessage());
                timeoutPromise.set_value("");
                return;
            }
            timeoutPromise.set_value(_value);
        });
    asyncGetSystemConfigByKey(SYSTEM_KEY_TX_COUNT_LIMIT,
        [&countLimitPromise](Error::Ptr _error, std::string _value, BlockNumber) {
            if (_error && _error->errorCode() != CommonError::SUCCESS)
            {
                LEDGER_LOG(ERROR) << LOG_BADGE("getLedgerConfig")
                                  << LOG_DESC("asyncGetSystemConfigByKey callback error")
                                  << LOG_KV("getKey", SYSTEM_KEY_TX_COUNT_LIMIT)
                                  << LOG_KV("errorCode", _error->errorCode())
                                  << LOG_KV("errorMsg", _error->errorMessage());
                countLimitPromise.set_value("");
                return;
            }
            countLimitPromise.set_value(_value);
        });
    asyncGetNodeListByType(
        CONSENSUS_SEALER, [&sealerPromise](Error::Ptr _error, consensus::ConsensusNodeListPtr _nodeList) {
            if (_error && _error->errorCode() != CommonError::SUCCESS)
            {
                LEDGER_LOG(ERROR) << LOG_BADGE("getLedgerConfig")
                                  << LOG_DESC("asyncGetNodeListByType callback error")
                                  << LOG_KV("getKey", CONSENSUS_SEALER)
                                  << LOG_KV("errorCode", _error->errorCode())
                                  << LOG_KV("errorMsg", _error->errorMessage());
                sealerPromise.set_value(nullptr);
                return;
            }
            if(!_nodeList)
            {
                LEDGER_LOG(ERROR) << LOG_DESC("get null sealer nodes")
                                  << LOG_KV("getKey", CONSENSUS_SEALER);
                sealerPromise.set_value(nullptr);
                return;
            }
            sealerPromise.set_value(_nodeList);
        });
    asyncGetNodeListByType(
        CONSENSUS_OBSERVER, [&observerPromise](Error::Ptr _error, consensus::ConsensusNodeListPtr _nodeList) {
            if (_error && _error->errorCode() != CommonError::SUCCESS)
            {
                LEDGER_LOG(ERROR) << LOG_BADGE("getLedgerConfig")
                                  << LOG_DESC("asyncGetNodeListByType callback error")
                                  << LOG_KV("getKey", CONSENSUS_OBSERVER)
                                  << LOG_KV("errorCode", _error->errorCode())
                                  << LOG_KV("errorMsg", _error->errorMessage());
                observerPromise.set_value(nullptr);
                return;
            }
            if (!_nodeList)
            {
                LEDGER_LOG(ERROR) << LOG_DESC("get null observer nodes")
                                  << LOG_KV("getKey", CONSENSUS_OBSERVER);
                observerPromise.set_value(nullptr);
                return;
            }
            observerPromise.set_value(_nodeList);
        });

    auto consensusTimeout = timeoutFuture.get();
    auto txLimit = countLimitFuture.get();
    auto sealerList = sealerFuture.get();
    auto observerList = observerFuture.get();

    if (consensusTimeout.empty() || txLimit.empty() || !sealerList || !observerList)
    {
        LEDGER_LOG(ERROR) << LOG_BADGE("getLedgerConfig")
                          << LOG_DESC("Get ledgerConfig from db error");
        return nullptr;
    }
    ledgerConfig->setConsensusTimeout(boost::lexical_cast<uint64_t>(consensusTimeout));
    ledgerConfig->setBlockTxCountLimit(boost::lexical_cast<uint64_t>(txLimit));
    ledgerConfig->setConsensusNodeList(*sealerList);
    ledgerConfig->setObserverNodeList(*observerList);
    return ledgerConfig;
}

bool Ledger::isBlockShouldCommit(const BlockNumber& _blockNumber, const std::string& _parentHash)
{
    std::promise<std::string> hashPromise;
    std::promise<BlockNumber> numberPromise;
    auto hashFuture = hashPromise.get_future();
    auto numberFuture = numberPromise.get_future();
    getLatestBlockNumber([this, &numberPromise, &hashPromise, _blockNumber](protocol::BlockNumber _number) {
        numberPromise.set_value(_number);
        getStorageGetter()->getBlockHashByNumber(_number, getMemoryTableFactory(0),
            [&hashPromise, _number, _blockNumber](Error::Ptr _error, std::shared_ptr<std::string> _hash) {
                if ((!_error || _error->errorCode() == CommonError::SUCCESS) && _hash)
                {
                    hashPromise.set_value(std::move(*_hash));
                    return;
                }
                LEDGER_LOG(ERROR) << LOG_BADGE("isBlockShouldCommit")
                                  << LOG_DESC("Get block hash error")
                                  << LOG_KV("needNumber", _number + 1)
                                  << LOG_KV("committedNumber", _blockNumber);
                hashPromise.set_value("");
            });
    });
    auto number = numberFuture.get();
    auto hash = hashFuture.get();
    if (_blockNumber == number + 1 && _parentHash == hash)
    {
        return true;
    }
    LEDGER_LOG(WARNING) << LOG_BADGE("isBlockShouldCommit")
                        << LOG_DESC("incorrect block number or incorrect parent hash")
                        << LOG_KV("needNumber", number + 1)
                        << LOG_KV("committedNumber", _blockNumber);
    return false;
}

void Ledger::writeNumber(
    const BlockNumber& blockNumber, const bcos::storage::TableFactoryInterface::Ptr& _tableFactory)
{
    bool ret = getStorageSetter()->setCurrentState(
        _tableFactory, SYS_KEY_CURRENT_NUMBER, boost::lexical_cast<std::string>(blockNumber));
    if(!ret){
        LEDGER_LOG(DEBUG) << LOG_BADGE("writeNumber")
                          << LOG_DESC("Write row in SYS_CURRENT_STATE error")
                          << LOG_KV("blockNumber", blockNumber);
    }
}

void Ledger::writeNumber2Nonces(
    const Block::Ptr& block, const bcos::storage::TableFactoryInterface::Ptr& _tableFactory)
{
    auto blockNumberStr = boost::lexical_cast<std::string>(block->blockHeader()->number());
    auto emptyBlock = m_blockFactory->createBlock();
    emptyBlock->setNonceList(block->nonceList());

    std::shared_ptr<bytes> nonceData = std::make_shared<bytes>();
    emptyBlock->encode(*nonceData);
    bool ret =
        getStorageSetter()->setNumber2Nonces(_tableFactory, blockNumberStr, asString(*nonceData));
    if(!ret){
        LEDGER_LOG(DEBUG) << LOG_BADGE("WriteNoncesToBlock")
                          << LOG_DESC("Write row in SYS_BLOCK_NUMBER_2_NONCES error")
                          << LOG_KV("blockNumber", blockNumberStr);
    }
}

void Ledger:: writeHash2Number(
    const BlockHeader::Ptr& header, const bcos::storage::TableFactoryInterface::Ptr& _tableFactory)
{
    bool ret = getStorageSetter()->setHash2Number(_tableFactory, header->hash().hex(),
        boost::lexical_cast<std::string>(header->number()));
    ret = ret && getStorageSetter()->setNumber2Hash(_tableFactory,
                     boost::lexical_cast<std::string>(header->number()),
                     header->hash().hex());
    if(!ret){
        LEDGER_LOG(DEBUG) << LOG_BADGE("WriteHash2Number")
                          << LOG_DESC("Write row in SYS_HASH_2_NUMBER error")
                          << LOG_KV("blockHash", header->hash().hex());
    }
}

void Ledger::writeNumber2BlockHeader(
    const BlockHeader::Ptr& _header, const bcos::storage::TableFactoryInterface::Ptr& _tableFactory)
{
    auto encodedBlockHeader = std::make_shared<bytes>();
    auto emptyBlock = m_blockFactory->createBlock();
    emptyBlock->setBlockHeader(_header);
    emptyBlock->blockHeader()->encode(*encodedBlockHeader);

    bool ret = getStorageSetter()->setNumber2Header(_tableFactory,
        boost::lexical_cast<std::string>(_header->number()),
        asString(*encodedBlockHeader));
    if(!ret){
        LEDGER_LOG(DEBUG) << LOG_BADGE("WriteNumber2Header")
                          << LOG_DESC("Write row in SYS_NUMBER_2_BLOCK_HEADER error")
                          << LOG_KV("blockNumber", _header->number());
    }
}
void Ledger::writeTotalTransactionCount(
    const Block::Ptr& block, const bcos::storage::TableFactoryInterface::Ptr& _tableFactory)
{
    getStorageGetter()->getCurrentState(SYS_KEY_TOTAL_TRANSACTION_COUNT, _tableFactory,
        [this, block, _tableFactory](Error::Ptr _error, std::shared_ptr<std::string> _totalTxStr) {
            if (!_error || _error->errorCode() == CommonError::SUCCESS)
            {
                int64_t totalTxCount = 0;
                if(_totalTxStr && !_totalTxStr->empty()){
                    totalTxCount += boost::lexical_cast<int64_t>(*_totalTxStr);
                }
                totalTxCount += block->transactionsSize();
                auto ret = getStorageSetter()->setCurrentState(_tableFactory,
                    SYS_KEY_TOTAL_TRANSACTION_COUNT,
                    boost::lexical_cast<std::string>(totalTxCount));
                if(!ret){
                    LEDGER_LOG(DEBUG) << LOG_BADGE("writeTotalTransactionCount")
                                      << LOG_DESC("Write SYS_KEY_TOTAL_TRANSACTION_COUNT error")
                                      << LOG_KV("blockNumber", block->blockHeader()->number());
                    return;
                }
            }
            else
            {
                LEDGER_LOG(ERROR) << LOG_BADGE("writeTotalTransactionCount")
                                  << LOG_DESC("Get SYS_KEY_TOTAL_TRANSACTION_COUNT error")
                                  << LOG_KV("blockNumber", block->blockHeader()->number());
                return;
            }
        });

    getStorageGetter()->getCurrentState(SYS_KEY_TOTAL_FAILED_TRANSACTION, _tableFactory,
        [this, _tableFactory, block](Error::Ptr _error, std::shared_ptr<std::string> _totalFailedTxsStr) {
            auto receipts = blockReceiptListGetter(block);
            int64_t failedTransactions = 0;
            for (auto& receipt : *receipts)
            {
                // TODO: check receipt status
                if (receipt->status() != 0)
                {
                    ++failedTransactions;
                }
            }
            if (!_error || _error->errorCode() == CommonError::SUCCESS)
            {
                if(_totalFailedTxsStr && !_totalFailedTxsStr->empty()){
                    failedTransactions += boost::lexical_cast<int64_t>(*_totalFailedTxsStr);
                }
                auto ret = getStorageSetter()->setCurrentState(_tableFactory,
                    SYS_KEY_TOTAL_FAILED_TRANSACTION,
                    boost::lexical_cast<std::string>(failedTransactions));
                if(!ret){
                    LEDGER_LOG(DEBUG) << LOG_BADGE("writeTotalTransactionCount")
                                      << LOG_DESC("Write SYS_KEY_TOTAL_FAILED_TRANSACTION error")
                                      << LOG_KV("blockNumber", block->blockHeader()->number());
                }
                return;
            }
            LEDGER_LOG(ERROR) << LOG_BADGE("writeTotalTransactionCount")
                              << LOG_DESC("Get SYS_KEY_TOTAL_FAILED_TRANSACTION error")
                              << LOG_KV("blockNumber", block->blockHeader()->number());
        });
}
void Ledger::writeNumber2Transactions(
    const Block::Ptr& _block, const TableFactoryInterface::Ptr& _tableFactory)
{
    auto encodeBlock = std::make_shared<bytes>();
    auto emptyBlock = m_blockFactory->createBlock();
    auto number = _block->blockHeader()->number();
    for (size_t i = 0; i < _block->transactionsSize(); i++)
    {
        emptyBlock->appendTransactionHash(_block->transactionHash(i));
    }

    emptyBlock->encode(*encodeBlock);
    bool ret = getStorageSetter()->setNumber2Txs(
        _tableFactory, boost::lexical_cast<std::string>(number), asString(*encodeBlock));
    if(!ret){
        LEDGER_LOG(DEBUG) << LOG_BADGE("WriteNumber2Txs")
                          << LOG_DESC("Write row in SYS_NUMBER_2_TXS error")
                          << LOG_KV("blockNumber", number);
    }
}
void Ledger::writeHash2Receipt(const bcos::protocol::Block::Ptr& _block,
    const TableFactoryInterface::Ptr& _tableFactory)
{
    tbb::parallel_for(tbb::blocked_range<size_t>(0, _block->transactionsHashSize()),
        [&](const tbb::blocked_range<size_t>& range) {
            for (size_t i = range.begin(); i < range.end(); ++i)
            {
                auto encodeReceipt = _block->receipt(i)->encode();
                auto ret = getStorageSetter()->setHashToReceipt(
                    _tableFactory, _block->transactionHash(i).hex(), asString(encodeReceipt));
                if (!ret)
                {
                    LEDGER_LOG(DEBUG) << LOG_BADGE("writeHash2Receipt")
                                      << LOG_DESC("Write row in SYS_HASH_2_RECEIPT error")
                                      << LOG_KV("txHash", _block->transactionHash(i).hex())
                                      << LOG_KV("blockNumber", _block->blockHeader()->number());
                }
            }
        });
}

bool Ledger::buildGenesisBlock(
    LedgerConfig::Ptr _ledgerConfig, size_t _gasLimit, std::string _genesisData)
{
    LEDGER_LOG(INFO) << LOG_DESC("[#buildGenesisBlock]");
    if(_ledgerConfig->consensusTimeout()>SYSTEM_CONSENSUS_TIMEOUT_MAX || _ledgerConfig->consensusTimeout()<SYSTEM_CONSENSUS_TIMEOUT_MIN)
    {
        LEDGER_LOG(ERROR) << LOG_BADGE("buildGenesisBlock")
                          << LOG_DESC("consensus timeout set error, return false")
                          << LOG_KV("consensusTimeout", _ledgerConfig->consensusTimeout());
        return false;
    }
    if(_gasLimit < TX_GAS_LIMIT_MIN)
    {
        LEDGER_LOG(ERROR) << LOG_BADGE("buildGenesisBlock")
                          << LOG_DESC("gas limit too low, return false")
                          << LOG_KV("gasLimit", _gasLimit)
                          << LOG_KV("gasLimitMin", TX_GAS_LIMIT_MIN);
        return false;
    }
    if (!getStorageGetter()->checkTableExist(SYS_NUMBER_2_BLOCK_HEADER, getMemoryTableFactory(0)))
    {
        LEDGER_LOG(TRACE) << LOG_BADGE("buildGenesisBlock")
                          << LOG_DESC(
                                 std::string(SYS_NUMBER_2_BLOCK_HEADER) + " table does not exist");
        getStorageSetter()->createTables(getMemoryTableFactory(0));
    }
    Block::Ptr block = nullptr;
    std::promise<Block::Ptr> blockPromise;
    auto blockFuture = blockPromise.get_future();
    getBlock(0, HEADER, [=, &blockPromise](Error::Ptr _error, Block::Ptr _block) {
        if (!_error || _error->errorCode() == CommonError::SUCCESS)
        {
            // block is nullptr means need build a genesis block
            blockPromise.set_value(_block);
            return;
        }
        blockPromise.set_value(nullptr);
        LEDGER_LOG(ERROR) << LOG_BADGE("buildGenesisBlock")
                          << LOG_DESC("Get header from storage error")
                          << LOG_KV("errorCode", _error->errorCode())
                          << LOG_KV("errorMsg", _error->errorMessage()) << LOG_KV("blockNumber", 0);
    });
    if (std::future_status::ready == blockFuture.wait_for(std::chrono::milliseconds(m_timeout)))
    {
        block = blockFuture.get();
    }
    // to build genesis block
    if (block == nullptr)
    {
        auto txLimit = _ledgerConfig->blockTxCountLimit();
        LEDGER_LOG(TRACE) << LOG_DESC("test") << LOG_KV("txLimit", txLimit);
        auto tableFactory = getMemoryTableFactory(0);
        // build a block
        block = m_blockFactory->createBlock();
        auto header = getBlockHeaderFactory()->createBlockHeader();
        header->setNumber(0);
        header->setExtraData(asBytes(_genesisData));
        block->setBlockHeader(header);
        try
        {
            tbb::parallel_invoke(
                [this, tableFactory, header]() { writeHash2Number(header, tableFactory); },
                [this, tableFactory, _ledgerConfig]() {
                    getStorageSetter()->setSysConfig(tableFactory, SYSTEM_KEY_TX_COUNT_LIMIT,
                        boost::lexical_cast<std::string>(_ledgerConfig->blockTxCountLimit()), "0");
                },
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
            // db sync commit
            auto retPair = tableFactory->commit();
            if ((!retPair.second || retPair.second->errorCode() == CommonError::SUCCESS) &&
                retPair.first > 0)
            {
                LEDGER_LOG(TRACE) << LOG_DESC("[#buildGenesisBlock]Storage commit success")
                                  << LOG_KV("commitSize", retPair.first);
                return true;
            }
            else
            {
                LEDGER_LOG(ERROR) << LOG_DESC("[#buildGenesisBlock]Storage commit error");
                return false;
            }
        }
        catch (OpenSysTableFailed const& e)
        {
            LEDGER_LOG(FATAL)
                << LOG_DESC(
                       "[#buildGenesisBlock]System meets error when try to write block to storage")
                << LOG_KV("EINFO", boost::diagnostic_information(e));
            raise(SIGTERM);
            BOOST_THROW_EXCEPTION(
                OpenSysTableFailed() << errinfo_comment(" write block to storage failed."));
        }
    }
    else
    {
        LEDGER_LOG(INFO) << LOG_BADGE("buildGenesisBlock")
                         << LOG_DESC("Already have the 0th block");
        auto header = block->blockHeader();
        LEDGER_LOG(INFO)
            << LOG_BADGE("buildGenesisBlock")
            << LOG_DESC("Load genesis config from extraData");
        if (header->extraData().toString() == _genesisData)
        {
            return true;
        }
        return false;
    }
    return true;
}
