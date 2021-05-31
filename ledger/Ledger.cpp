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
    auto start_time = utcTime();
    // default parent block hash located in parentInfo[0]
    if (!isBlockShouldCommit(_header->number(), _header->parentInfo().at(0).blockHash.hex()))
    {
        // TODO: add error code
        auto error = std::make_shared<Error>(
            -1, "[#asyncCommitBlock] Wrong block number of wrong parent hash");
        _onCommitBlock(error, nullptr);
        return;
    }

    auto ledgerConfig = getLedgerConfig(_header->number(), _header->hash());
    try
    {
        auto blockNumber = _header->number();

        auto tableFactory = getMemoryTableFactory(blockNumber);

        tbb::parallel_invoke(
            [this, _header, tableFactory]() { writeNumber(_header->number(), tableFactory); },
            [this, _header, tableFactory]() { writeHash2Number(_header, tableFactory); },
            [this, _header, tableFactory]() { writeNumber2BlockHeader(_header, tableFactory); });

        tableFactory->asyncCommit([blockNumber, _header, _onCommitBlock, ledgerConfig, this](
                                      Error::Ptr _error, size_t _commitSize) {
            if ((!_error || _error->errorCode() == CommonError::SUCCESS) && _commitSize > 0)
            {
                m_blockHeaderCache.add(blockNumber, _header);
                _onCommitBlock(nullptr, ledgerConfig);
            }
            else
            {
                LEDGER_LOG(ERROR) << LOG_DESC("Commit Block failed in storage")
                                  << LOG_KV("number", blockNumber);
                // TODO: add error code
                auto error = std::make_shared<Error>(_error->errorCode(),
                    "[#asyncCommitBlock] Commit block error in storage" + _error->errorMessage());
                _onCommitBlock(error, nullptr);
            }
        });
        LEDGER_LOG(DEBUG) << LOG_BADGE("asyncCommitBlock") << LOG_DESC("Commit block time record")
                          << LOG_KV("totalTimeCost", utcTime() - start_time);
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
    auto start_time = utcTime();
    getLatestBlockNumber([this, _txHashList, _txToStore, _onTxStored, start_time](protocol::BlockNumber _number) {
        try
        {
            auto write_record_time = utcTime();
            auto tableFactory = getMemoryTableFactory(_number + 1);
            if (tableFactory)
            {
                for (size_t i = 0; i < _txHashList->size(); ++i)
                {
                    auto txHashHex = _txHashList->at(i).hex();
                    getStorageSetter()->setHashToTx(
                        tableFactory, txHashHex, asString(*(_txToStore->at(i))));
                    LEDGER_LOG(TRACE)
                        << LOG_BADGE("setHashToTx") << LOG_DESC("write HASH_2_TX success")
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
                        LEDGER_LOG(ERROR) << LOG_BADGE("asyncStoreTransactions")
                                          << LOG_DESC("table commit failed");
                        // TODO: add error code and msg
                        auto error = std::make_shared<Error>(
                            _error->errorCode(), "[#asyncStoreTransactions] table commit failed" +
                                                     _error->errorMessage());
                        _onTxStored(error);
                    }
                });
            }
            else
            {
                LEDGER_LOG(ERROR) << LOG_BADGE("asyncStoreTransactions")
                                  << LOG_DESC("get a null tableFactory in state cache");
                // TODO: add error code
                auto error = std::make_shared<Error>(
                    -1, "[#asyncStoreTransactions] get a null tableFactory in state cache");
                _onTxStored(error);
            }
            auto write_table_time = utcTime() - write_record_time;
            LEDGER_LOG(DEBUG) << LOG_BADGE("asyncStoreTransactions")
                              << LOG_DESC("Store Txs time record")
                              << LOG_KV("writeTableTime", write_table_time)
                              << LOG_KV("totalTimeCost", utcTime() - start_time);
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
    auto start_time = utcTime();

    try
    {
        tbb::parallel_invoke(
            [this, _block, _tableFactory]() { writeTotalTransactionCount(_block, _tableFactory); },
            [this, _block, _tableFactory]() { writeNumber2Nonces(_block, _tableFactory); },
            [this, _block, _tableFactory]() { writeNumber2Transactions(_block, _tableFactory); },
            [this, _block, _tableFactory]() { writeHash2Receipt(_block, _tableFactory); });

        getStorage()->asyncAddStateCache(blockNumber, _tableFactory,
            [_block, _onReceiptStored, blockNumber, this](Error::Ptr _error) {
                if (!_error || _error->errorCode() == CommonError::SUCCESS)
                {
                    m_transactionsCache.add(blockNumber, blockTransactionListGetter(_block));
                    m_receiptCache.add(blockNumber, blockReceiptListGetter(_block));
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
                        std::make_shared<Error>(_error->errorCode(), "add state cache faild" + _error->errorMessage());
                    _onReceiptStored(error);
                }
            });

        LEDGER_LOG(DEBUG) << LOG_BADGE("asyncStoreReceipts") << LOG_DESC("Store Receipts time record")
                          << LOG_KV("totalTimeCost", utcTime() - start_time);
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
            [_blockNumber, _onGetBlock](
                Error::Ptr _error, protocol::Block::Ptr _block) {
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
                    LEDGER_LOG(ERROR)
                        << LOG_BADGE("asyncGetBlockDataByNumber")
                        << LOG_DESC("callback error when get block")
                        << LOG_KV("errorCode", _error->errorCode())
                        << LOG_KV("errorMsg", _error->errorMessage())
                        << LOG_KV("blockNumber", _blockNumber);
                    // TODO: to add errorCode and message
                    auto error = std::make_shared<Error>(
                        _error->errorCode(), "callback error in getBlock" + _error->errorMessage());
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
    tbb::concurrent_unordered_map<std::string, MerkleProofPtr> con_proofMap;
    for (auto& txHash : *_txHashList)
    {
        txHashStrList->emplace_back(txHash.hex());
    }
    getStorageGetter()->getBatchTxByHashList(txHashStrList, getMemoryTableFactory(0),
        getTransactionFactory(),
        [this, _txHashList, _withProof, _onGetTx, &con_proofMap](Error::Ptr _error, TransactionsPtr _txList) {
            if (!_error || _error->errorCode() == CommonError::SUCCESS)
            {
                if (_txList && _txHashList->size() == _txList->size())
                {
                    if (_withProof)
                    {
                        tbb::parallel_for(tbb::blocked_range<size_t>(0, _txHashList->size()),
                            [&](const tbb::blocked_range<size_t>& range) {
                                for (size_t i = range.begin(); i < range.end(); ++i)
                                {
                                    auto txHash = _txHashList->at(i);
                                    getTxProof(
                                        txHash, [&con_proofMap, &txHash](Error::Ptr _error, MerkleProofPtr _proof) {
                                            if ((!_error ||
                                                    _error->errorCode() == CommonError::SUCCESS) &&
                                                _proof)
                                            {
                                                con_proofMap.emplace(
                                                    std::make_pair(txHash.hex(), _proof));
                                            }
                                        });
                                }
                            });
                        auto proofMap = std::make_shared<std::map<std::string, MerkleProofPtr>>(
                            con_proofMap.begin(), con_proofMap.end());
                        if (proofMap->size() != _txHashList->size())
                        {
                            LEDGER_LOG(ERROR)
                                << LOG_BADGE("asyncGetBatchTxsByHashList")
                                << LOG_DESC("proof size does not match tx hash list size")
                                << LOG_KV("proofSize", proofMap->size())
                                << LOG_KV("txHashListSize", _txHashList->size())
                                << LOG_KV("withProof", _withProof);
                            // TODO: add error code and msg
                            auto error = std::make_shared<Error>(-1, "");
                            _onGetTx(error, nullptr, nullptr);
                        }
                        else
                        {
                            LEDGER_LOG(INFO) << LOG_BADGE("asyncGetBatchTxsByHashList")
                                             << LOG_DESC("get tx list and proofMap complete")
                                             << LOG_KV("txHashListSize", _txHashList->size())
                                             << LOG_KV("withProof", _withProof);
                            _onGetTx(nullptr, _txList, proofMap);
                        }
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
            if (!_error || _error->errorCode() == CommonError::SUCCESS)
            {
                if(_receiptStr && !_receiptStr->empty())
                {
                    auto receipt = decodeReceipt(getReceiptFactory(), *_receiptStr);
                    if (_withProof)
                    {
                        auto merkleProof = std::make_shared<MerkleProof>();
                        auto number = receipt->blockNumber();
                        getStorageGetter()->getTxsFromStorage(number, getMemoryTableFactory(0),
                            [this, _onGetTx, &merkleProof, number, receipt](
                                Error::Ptr _error, std::shared_ptr<std::string> _blockStr) {
                                if (!_error || _error->errorCode() == CommonError::SUCCESS)
                                {
                                    if (_blockStr && !_blockStr->empty())
                                    {
                                        auto block = decodeBlock(m_blockFactory, *_blockStr);
                                        auto txHashList = blockTxHashListGetter(block);
                                        getStorageGetter()->getBatchReceiptsByHashList(txHashList,
                                            getMemoryTableFactory(0), getReceiptFactory(),
                                            [this, _onGetTx, &merkleProof, number, receipt](
                                                Error::Ptr _error, ReceiptsPtr receipts) {
                                                if ((!_error || _error->errorCode() ==
                                                                    CommonError::SUCCESS))
                                                {
                                                    if (receipts && !receipts->empty())
                                                    {
                                                        auto parent2ChildList =
                                                            m_merkleProofUtility
                                                                ->getParent2ChildListByReceiptProofCache(
                                                                    number, receipts,
                                                                    m_blockFactory->cryptoSuite());
                                                        auto child2Parent =
                                                            m_merkleProofUtility->getChild2ParentCacheByReceipt(
                                                                parent2ChildList, number);
                                                        m_merkleProofUtility->getMerkleProof(receipt->hash(),
                                                            *parent2ChildList, *child2Parent,
                                                            *merkleProof);
                                                        LEDGER_LOG(INFO)
                                                            << LOG_BADGE(
                                                                   "asyncGetTransactionReceiptByHash")
                                                            << LOG_DESC(
                                                                   "call back receipt and proof");
                                                        _onGetTx(nullptr, receipt, merkleProof);
                                                    }
                                                    else
                                                    {
                                                        // TODO: add error code
                                                        LEDGER_LOG(ERROR)
                                                            << LOG_BADGE(
                                                                   "asyncGetTransactionReceiptByHash")
                                                            << LOG_DESC(
                                                                   "getBatchReceiptsByHashList callback empty receipts");
                                                        auto error = std::make_shared<Error>(
                                                            -1, "empty receipts");
                                                        _onGetTx(error, nullptr, nullptr);
                                                    }
                                                }
                                                else
                                                {
                                                    LEDGER_LOG(ERROR)
                                                        << LOG_BADGE(
                                                               "asyncGetTransactionReceiptByHash")
                                                        << LOG_DESC(
                                                               "getBatchReceiptsByHashList callback error")
                                                        << LOG_KV("errorCode", _error->errorCode())
                                                        << LOG_KV("errorMsg", _error->errorMessage());
                                                    // TODO: add error code and message
                                                    auto error =
                                                        std::make_shared<Error>(_error->errorCode(),
                                                            "getBatchReceiptsByHashList callback "
                                                            "error" + _error->errorMessage());
                                                    _onGetTx(error, nullptr, nullptr);
                                                }
                                            });
                                    }
                                    else
                                    {
                                        // TODO: add error code
                                        LEDGER_LOG(ERROR)
                                            << LOG_BADGE("asyncGetTransactionReceiptByHash")
                                            << LOG_DESC(
                                                   "getTxsFromStorage callback empty block txs");
                                        auto error = std::make_shared<Error>(-1, "empty txs");
                                        _onGetTx(error, nullptr, nullptr);
                                    }
                                }
                                else
                                {
                                    LEDGER_LOG(ERROR)
                                        << LOG_BADGE("asyncGetTransactionReceiptByHash")
                                        << LOG_DESC("getTxsFromStorage callback error")
                                        << LOG_KV("errorCode", _error->errorCode())
                                        << LOG_KV("errorMsg", _error->errorMessage());
                                    // TODO: add error code
                                    auto error = std::make_shared<Error>(
                                        _error->errorCode(), "getTxsFromStorage callback error" +
                                                                 _error->errorMessage());
                                    _onGetTx(error, nullptr, nullptr);
                                }
                            });
                    }
                    else
                    {
                        LEDGER_LOG(TRACE) << LOG_BADGE("asyncGetTransactionReceiptByHash")
                                          << LOG_DESC("call back receipt");
                        _onGetTx(nullptr, receipt, nullptr);
                    }
                }
                else
                {
                    LEDGER_LOG(ERROR) << LOG_BADGE("asyncGetTransactionReceiptByHash")
                                      << LOG_DESC("getReceiptByTxHash callback empty receipt")
                                      << LOG_KV("txHash", _txHash.hex());
                    // TODO: add error code
                    auto error = std::make_shared<Error>(-1, "empty receipt");
                    _onGetTx(error, nullptr, nullptr);
                }
            }
            else
            {
                LEDGER_LOG(ERROR) << LOG_BADGE("asyncGetTransactionReceiptByHash")
                                  << LOG_DESC("getReceiptByTxHash callback error")
                                  << LOG_KV("errorCode", _error->errorCode())
                                  << LOG_KV("errorMsg", _error->errorMessage());
                // TODO: add error code
                auto error = std::make_shared<Error>(_error->errorCode(),
                    "getReceiptByTxHash callback error" + _error->errorMessage());
                _onGetTx(error, nullptr, nullptr);
            }
        });
}

void Ledger::asyncGetTotalTransactionCount(
    std::function<void(Error::Ptr, int64_t, int64_t, bcos::protocol::BlockNumber)> _callback)
{
    int64_t totalCount = -2;
    int64_t totalFailed = -2;
    BlockNumber number = -2;
    getStorageGetter()->getCurrentState(SYS_KEY_TOTAL_TRANSACTION_COUNT, getMemoryTableFactory(0),
        [&totalCount, this](Error::Ptr _error, std::shared_ptr<std::string> totalCountStr) {
            if ((!_error || _error->errorCode() == CommonError::SUCCESS) && totalCountStr && !totalCountStr->empty())
            {
                totalCount = boost::lexical_cast<int64_t>(*totalCountStr);
            }
            else
            {
                LEDGER_LOG(ERROR) << LOG_BADGE("asyncGetTransactionReceiptByHash")
                                  << LOG_DESC("error happened in get SYS_KEY_TOTAL_TRANSACTION_COUNT");
                totalCount = -1;
            }
          m_signalled.notify_one();
        });
    getStorageGetter()->getCurrentState(SYS_KEY_TOTAL_FAILED_TRANSACTION, getMemoryTableFactory(0),
        [&totalFailed, this](
            Error::Ptr _error, std::shared_ptr<std::string> totalFailedStr) {
            if ((!_error || _error->errorCode() == CommonError::SUCCESS) && totalFailedStr && !totalFailedStr->empty())
            {
                totalFailed = boost::lexical_cast<int64_t>(*totalFailedStr);
            }
            else
            {
                LEDGER_LOG(ERROR) << LOG_BADGE("asyncGetTransactionReceiptByHash")
                                  << LOG_DESC("error happened in get SYS_KEY_TOTAL_FAILED_TRANSACTION");
                totalFailed = -1;
            }
            m_signalled.notify_one();
        });
    getLatestBlockNumber([&number, this](BlockNumber _number) {
        number = _number;
        m_signalled.notify_one();
    });
    auto startT = utcSteadyTime();
    auto fetchSuccess = false;
    while (utcSteadyTime() - startT < m_timeout)
    {
        if (totalFailed != -2 && totalCount != -2 && number != -2)
        {
            fetchSuccess = true;
            break;
        }
        boost::unique_lock<boost::mutex> l(x_signalled);
        m_signalled.wait_for(l, boost::chrono::milliseconds(10));
    }
    if (!fetchSuccess)
    {
        LEDGER_LOG(ERROR) << LOG_BADGE("asyncGetTransactionReceiptByHash")
                          << LOG_DESC("timeout happened in get total tx count");
        // TODO: add error code
        auto error = std::make_shared<Error>(
            -1, "timeout to fetch all data in asyncGetTotalTransactionCount");
        _callback(error, -1, -1, -1);
    }
    else
    {
        if (totalCount > -1 && totalFailed > -1 && number > -1)
        {
            _callback(nullptr, totalCount, totalFailed, number);
        }
        else
        {
            auto error = std::make_shared<Error>(-1, "some data get failed, please check");
            _callback(error, totalCount, totalFailed, number);
        }
    }
}

void Ledger::asyncGetSystemConfigByKey(const std::string& _key,
    std::function<void(Error::Ptr, std::string, bcos::protocol::BlockNumber)> _onGetConfig)
{
    getStorageGetter()->getSysConfig(_key, getMemoryTableFactory(0),
        [_key, _onGetConfig](Error::Ptr _error, std::shared_ptr<stringsPair> _config) {
            if (!_error || _error->errorCode() == CommonError::SUCCESS)
            {
                if (_config && !_config->second.empty())
                {
                    LEDGER_LOG(TRACE)
                        << LOG_BADGE("asyncGetSystemConfigByKey") << LOG_DESC("get config in db")
                        << LOG_KV("key", _key) << LOG_KV("value", _config->first);
                    _onGetConfig(
                        nullptr, _config->first, boost::lexical_cast<BlockNumber>(_config->second));
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
    getLatestBlockNumber([&](protocol::BlockNumber _number) {
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
    auto start_time = utcTime();
    auto record_time = utcTime();
    auto cachedBlock = m_blockCache.get(_blockNumber);
    auto getCache_time_cost = utcTime() - record_time;
    record_time = utcTime();

    if (bool(cachedBlock.second))
    {
        LEDGER_LOG(TRACE) << LOG_BADGE("getBlock") << LOG_DESC("Cache hit, read from cache")
                          << LOG_KV("blockNumber", _blockNumber);
        _onGetBlock(nullptr, cachedBlock.second);
    }
    else
    {
        LEDGER_LOG(TRACE) << LOG_BADGE("getBlock") << LOG_DESC("Cache missed, read from storage")
                          << LOG_KV("blockNumber", _blockNumber);
        auto block = m_blockFactory->createBlock();
        int32_t fetchFlag = 0;
        std::atomic_bool fetchResult = {true};
        if(_blockFlag & HEADER)
        {
            getBlockHeader(_blockNumber, [this, &fetchFlag, &fetchResult, _blockNumber, block](
                                             Error::Ptr _error, BlockHeader::Ptr _header) {
                if ((!_error || _error->errorCode() == CommonError::SUCCESS) && _header)
                {
                    block->setBlockHeader(_header);
                }
                else
                {
                    LEDGER_LOG(ERROR) << LOG_BADGE("getBlock")
                                      << LOG_DESC("Can't find the header, callback error")
                                      << LOG_KV("errorCode", _error->errorCode())
                                      << LOG_KV("errorMsg", _error->errorMessage())
                                      << LOG_KV("blockNumber", _blockNumber);
                    fetchResult = false;
                }
                fetchFlag += HEADER;
                m_signalled.notify_all();
            });
        }
        if(_blockFlag & TRANSACTIONS)
        {
            getTxs(_blockNumber, [this, _blockNumber, &fetchFlag, &fetchResult, block](
                                     Error::Ptr _error, bcos::protocol::TransactionsPtr _txs) {
                if ((!_error || _error->errorCode() == CommonError::SUCCESS) && _txs)
                {
                    auto insertSize = blockTransactionListSetter(block, _txs);
                    LEDGER_LOG(TRACE)
                        << LOG_BADGE("getBlock") << LOG_DESC("insert block transactions")
                        << LOG_KV("txsSize", _txs->size()) << LOG_KV("insertSize", insertSize)
                        << LOG_KV("blockNumber", _blockNumber);
                }
                else
                {
                    LEDGER_LOG(ERROR)
                        << LOG_BADGE("getBlock") << LOG_DESC("Can't find the Txs, callback error")
                        << LOG_KV("errorCode", _error->errorCode())
                        << LOG_KV("errorMsg", _error->errorMessage())
                        << LOG_KV("blockNumber", _blockNumber);
                    fetchResult = false;
                }
                fetchFlag += TRANSACTIONS;
                m_signalled.notify_all();
            });
        }
        if(_blockFlag & RECEIPTS)
        {
            getReceipts(_blockNumber, [this, _blockNumber, &fetchFlag, &fetchResult, block](
                                          Error::Ptr _error, protocol::ReceiptsPtr _receipts) {
                if ((!_error || _error->errorCode() == CommonError::SUCCESS) && _receipts)
                {
                    auto insertSize = blockReceiptListSetter(block, _receipts);
                    LEDGER_LOG(TRACE)
                        << LOG_BADGE("getBlock") << LOG_DESC("insert block receipts error")
                        << LOG_KV("txsSize", _receipts->size()) << LOG_KV("insertSize", insertSize)
                        << LOG_KV("blockNumber", _blockNumber);
                }
                else
                {
                    LEDGER_LOG(ERROR)
                        << LOG_BADGE("getBlock") << LOG_DESC("Can't find the Receipts")
                        << LOG_KV("errorCode", _error->errorCode())
                        << LOG_KV("errorMsg", _error->errorMessage())
                        << LOG_KV("blockNumber", _blockNumber);
                    fetchResult = false;
                }
                fetchFlag += RECEIPTS;
                m_signalled.notify_all();
            });
        }
        if(!fetchResult)
        {
            auto error = std::make_shared<Error>(-1, "some data fetch failed");
            _onGetBlock(error, nullptr);
            return;
        }
        else
        {
            // it means _blockFlag has multiple 1 in binary
            // should wait for all datum callback
            if (_blockFlag & (_blockFlag - 1))
            {
                auto start_fetch_time = utcSteadyTime();
                auto fetchSuccess = false;
                while (utcSteadyTime() - start_fetch_time < m_timeout)
                {
                    // if fetch FULL_BLOCK, then fetchFlag should be 8+4+2
                    // if not, then fetchFlag should have multiple 1 in binary
                    if ((!(_blockFlag ^ FULL_BLOCK) &&
                         (fetchFlag & (HEADER | TRANSACTIONS | RECEIPTS))) ||
                        ((_blockFlag ^ FULL_BLOCK) && (fetchFlag & (fetchFlag - 1))))
                    {
                        fetchSuccess = true;
                        break;
                    }
                    boost::unique_lock<boost::mutex> l(x_signalled);
                    m_signalled.wait_for(l, boost::chrono::milliseconds(10));
                }
                if (!fetchSuccess)
                {
                    LEDGER_LOG(ERROR)
                        << LOG_BADGE("getBlock") << LOG_DESC("Get block from db timeout")
                        << LOG_KV("blockFlag", _blockFlag);
                    // TODO: add error code
                    auto error = std::make_shared<Error>(-1, "timeout for getBlock");
                    _onGetBlock(error, nullptr);
                    return;
                }
                else
                {
                    if (!(_blockFlag ^ FULL_BLOCK))
                    {
                        // get full block data
                        LEDGER_LOG(TRACE)
                            << LOG_BADGE("getBlock") << LOG_DESC("Write to cache");
                        m_blockCache.add(_blockNumber, block);
                    }
                    _onGetBlock(nullptr, block);
                }
            }
            else
            {
                auto start_fetch_time = utcSteadyTime();
                auto fetchSuccess = false;
                while (utcSteadyTime() - start_fetch_time < m_timeout)
                {
                    if (fetchFlag > 0)
                    {
                        fetchSuccess = true;
                        break;
                    }
                }
                if (!fetchSuccess)
                {
                    LEDGER_LOG(ERROR)
                        << LOG_BADGE("getBlock") << LOG_DESC("Get block data from db timeout")
                        << LOG_KV("blockFlag", _blockFlag);
                    // TODO: add error code
                    auto error = std::make_shared<Error>(-1, "timeout for get block data");
                    _onGetBlock(error, nullptr);
                    return;
                }
                else
                {
                    _onGetBlock(nullptr, block);
                }
            }
        }
        auto assemble_block = utcTime() - record_time;
        LEDGER_LOG(DEBUG) << LOG_BADGE("getBlock") << LOG_DESC("Get block from db")
                          << LOG_KV("getCacheTimeCost", getCache_time_cost)
                          << LOG_KV("constructBlockTimeCost", assemble_block)
                          << LOG_KV("totalTimeCost", utcTime() - start_time);
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
                            [this, _onGetTxs, _blockNumber, &txHashList](Error::Ptr _error, protocol::TransactionsPtr _txs) {
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
                            [&](Error::Ptr _error, ReceiptsPtr _receipts) {
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
                auto merkleProofPtr = std::make_shared<MerkleProof>();
                if (_receiptStr && !_receiptStr->empty())
                {
                    auto receipt = decodeReceipt(getReceiptFactory(), *_receiptStr);
                    auto blockNumber = receipt->blockNumber();
                    getTxs(blockNumber, [this, blockNumber, &merkleProofPtr, _onGetProof, _txHash](
                                            Error::Ptr _error, TransactionsPtr _txs) {
                        if (!_error || _error->errorCode() == CommonError::SUCCESS)
                        {
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

LedgerConfig::Ptr Ledger::getLedgerConfig(protocol::BlockNumber _number, const crypto::HashType& _hash){
    auto ledgerConfig = std::make_shared<LedgerConfig>();
    std::atomic_bool asyncRet = {true};
    ledgerConfig->setBlockNumber(_number);
    ledgerConfig->setHash(_hash);
    asyncGetSystemConfigByKey(SYSTEM_KEY_CONSENSUS_TIMEOUT,
        [this, &ledgerConfig, &asyncRet](Error::Ptr _error, std::string _value, BlockNumber) {
            if (!_error || _error->errorCode() == CommonError::SUCCESS)
            {
                ledgerConfig->setConsensusTimeout(boost::lexical_cast<uint64_t>(_value));
            }
            else
            {
                LEDGER_LOG(ERROR) << LOG_BADGE("getLedgerConfig")
                                  << LOG_DESC("asyncGetSystemConfigByKey callback error")
                                  << LOG_KV("getKey", SYSTEM_KEY_CONSENSUS_TIMEOUT)
                                  << LOG_KV("errorCode", _error->errorCode())
                                  << LOG_KV("errorMsg", _error->errorMessage());
                asyncRet = false;
            }
            m_signalled.notify_all();
        });
    asyncGetSystemConfigByKey(
        SYSTEM_KEY_TX_COUNT_LIMIT, [this, &ledgerConfig, &asyncRet](Error::Ptr _error, std::string _value, BlockNumber) {
            if (!_error || _error->errorCode() == CommonError::SUCCESS)
            {
                ledgerConfig->setBlockTxCountLimit(boost::lexical_cast<uint64_t>(_value));
            }
            else
            {
                LEDGER_LOG(ERROR) << LOG_BADGE("getLedgerConfig")
                                  << LOG_DESC("asyncGetSystemConfigByKey callback error")
                                  << LOG_KV("getKey", SYSTEM_KEY_TX_COUNT_LIMIT)
                                  << LOG_KV("errorCode", _error->errorCode())
                                  << LOG_KV("errorMsg", _error->errorMessage());
                asyncRet = false;
            }
          m_signalled.notify_all();
        });
    asyncGetNodeListByType(
        CONSENSUS_SEALER, [this, &ledgerConfig, &asyncRet](Error::Ptr _error, consensus::ConsensusNodeListPtr _nodeList) {
            if (!_error || _error->errorCode() == CommonError::SUCCESS)
            {
                if(_nodeList)
                {
                    ledgerConfig->setConsensusNodeList(*_nodeList);
                }
                else
                {
                    LEDGER_LOG(ERROR)
                        << LOG_DESC("get null sealer nodes") << LOG_KV("getKey", CONSENSUS_SEALER);
                    asyncRet = false;
                }
            }
            else
            {
                LEDGER_LOG(ERROR) << LOG_BADGE("getLedgerConfig")
                                  << LOG_DESC("asyncGetNodeListByType callback error")
                                  << LOG_KV("getKey", CONSENSUS_SEALER)
                                  << LOG_KV("errorCode", _error->errorCode())
                                  << LOG_KV("errorMsg", _error->errorMessage());
                asyncRet = false;
            }
            m_signalled.notify_all();
        });
    asyncGetNodeListByType(
        CONSENSUS_OBSERVER, [this, &ledgerConfig, &asyncRet](Error::Ptr _error, consensus::ConsensusNodeListPtr _nodeList) {
            if (!_error || _error->errorCode() == CommonError::SUCCESS)
            {
                if(_nodeList)
                {
                    ledgerConfig->setObserverNodeList(*_nodeList);
                }
                else
                {
                    LEDGER_LOG(ERROR)
                        << LOG_BADGE("getLedgerConfig") << LOG_DESC("get null observer nodes")
                        << LOG_KV("getKey", CONSENSUS_OBSERVER);
                    asyncRet = false;
                }
            }
            else
            {
                LEDGER_LOG(ERROR) << LOG_BADGE("getLedgerConfig")
                                  << LOG_DESC("asyncGetNodeListByType callback error")
                                  << LOG_KV("getKey", CONSENSUS_OBSERVER)
                                  << LOG_KV("errorCode", _error->errorCode())
                                  << LOG_KV("errorMsg", _error->errorMessage());
                asyncRet = false;
            }
            m_signalled.notify_all();
        });
    auto start_fetch_time = utcSteadyTime();
    auto fetchSuccess = false;
    while (utcSteadyTime() - start_fetch_time < m_timeout)
    {
        // if fetch FULL_BLOCK, then fetchFlag should be 8+4+2
        // if not, then fetchFlag should have multiple 1 in binary
        if (ledgerConfig->blockTxCountLimit() != 0 && ledgerConfig->consensusTimeout() != 0 &&
            !ledgerConfig->consensusNodeList().empty() && !ledgerConfig->observerNodeList().empty())
        {
            fetchSuccess = true;
            break;
        }
        if (!asyncRet)
        {
            fetchSuccess = false;
            break;
        }
        boost::unique_lock<boost::mutex> l(x_signalled);
        m_signalled.wait_for(l, boost::chrono::milliseconds(10));
    }
    if (!fetchSuccess)
    {
        LEDGER_LOG(ERROR) << LOG_BADGE("getLedgerConfig")
                          << LOG_DESC("Get ledgerConfig from db timeout");
        return nullptr;
    }
    return ledgerConfig;
}

bool Ledger::isBlockShouldCommit(const BlockNumber& _blockNumber, const std::string& _parentHash)
{
    BlockNumber number = -2;
    std::string hash;
    bool ret = true;
    getLatestBlockNumber([this, &hash, &number, &ret, _blockNumber](protocol::BlockNumber _number) {
        number = _number;
        getStorageGetter()->getBlockHashByNumber(_number, getMemoryTableFactory(0),
            [&](Error::Ptr _error, std::shared_ptr<std::string> _hash) {
                if ((!_error || _error->errorCode() == CommonError::SUCCESS) && _hash)
                {
                    hash = std::move(*_hash);
                }
                else
                {
                    LEDGER_LOG(ERROR) << LOG_BADGE("isBlockShouldCommit")
                                      << LOG_DESC("Get block hash error")
                                      << LOG_KV("needNumber", _number + 1)
                                      << LOG_KV("committedNumber", _blockNumber);
                    ret = false;
                }
                m_signalled.notify_all();
            });
    });
    auto start_time = utcSteadyTime();
    while (utcSteadyTime() - start_time < m_timeout)
    {
        if (number != -2 && !hash.empty())
        {
            break;
        }
        boost::unique_lock<boost::mutex> l(x_signalled);
        m_signalled.wait_for(l, boost::chrono::milliseconds(10));
    }
    if (_blockNumber == number + 1 && _parentHash == hash && ret)
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
    int64_t totalTxCount = 0;
    getStorageGetter()->getCurrentState(SYS_KEY_TOTAL_TRANSACTION_COUNT, _tableFactory,
        [this, &totalTxCount, block, _tableFactory](Error::Ptr _error, std::shared_ptr<std::string> _totalTxStr) {
            if (!_error || _error->errorCode() == CommonError::SUCCESS)
            {
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

    getStorageGetter()->getCurrentState(SYS_KEY_TOTAL_FAILED_TRANSACTION, _tableFactory,
        [this, &failedTransactions, _tableFactory, block](Error::Ptr _error, std::shared_ptr<std::string> _totalFailedTxsStr) {
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
                    return;
                }
            }
            else
            {
                LEDGER_LOG(ERROR) << LOG_BADGE("writeTotalTransactionCount")
                                  << LOG_DESC("Get SYS_KEY_TOTAL_FAILED_TRANSACTION error")
                                  << LOG_KV("blockNumber", block->blockHeader()->number());
            }
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
bool Ledger::buildGenesisBlock(LedgerConfig::Ptr _ledgerConfig)
{
    LEDGER_LOG(INFO) << LOG_DESC("[#buildGenesisBlock]");
    // TODO: to check NUMBER_2_HEADER table is created
    // TODO: creat tables
    // FIXME: use getBlock
    Block::Ptr block = nullptr;
    // to build genesis block
    if(block == nullptr)
    {
        auto txLimit = _ledgerConfig->blockTxCountLimit();
        LEDGER_LOG(TRACE) << LOG_DESC("test") << LOG_KV("txLimit", txLimit);
        auto tableFactory = getStorage()->getStateCache(0);
        // build a block
        block = m_blockFactory->createBlock();
        auto header = getBlockHeaderFactory()->createBlockHeader();
        header->setNumber(0);
        // TODO: add genesisMark
        header->setExtraData(asBytes(""));
        block->setBlockHeader(header);
        try
        {
            // TODO: concurrent write these
            // TODO: set header cache
            tbb::parallel_invoke(
                [this, tableFactory, header]() { writeHash2Number(header, tableFactory); },
                [this, tableFactory, _ledgerConfig]() {
                    getStorageSetter()->setSysConfig(tableFactory, SYSTEM_KEY_TX_COUNT_LIMIT,
                        boost::lexical_cast<std::string>(_ledgerConfig->blockTxCountLimit()), "0");
                },
                [this, tableFactory, _ledgerConfig]() {
                    getStorageSetter()->setSysConfig(tableFactory, SYSTEM_KEY_CONSENSUS_TIMEOUT,
                        boost::lexical_cast<std::string>(_ledgerConfig->consensusTimeout()), "0");
                },
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
            if ((!retPair.second || retPair.second->errorCode() == CommonError::SUCCESS) && retPair.first > 0)
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
        catch (OpenSysTableFailed const& e){
            LEDGER_LOG(FATAL)
                    << LOG_DESC("[#buildGenesisBlock]System meets error when try to write block to storage")
                    << LOG_KV("EINFO", boost::diagnostic_information(e));
            raise(SIGTERM);
            BOOST_THROW_EXCEPTION(
                OpenSysTableFailed() << errinfo_comment(" write block to storage failed."));
        }
    }
    else{
        // TODO: check 0th block
        LEDGER_LOG(INFO) << LOG_DESC(
            "[#buildGenesisBlock]Already have the 0th block");
        return true;
    }
    return true;
}
