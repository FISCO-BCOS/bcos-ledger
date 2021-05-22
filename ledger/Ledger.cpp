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
#include "utilities/MerkleProofUtility.h"
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
        LEDGER_LOG(FATAL) << LOG_DESC("Header is nullptr");
        // TODO: add error code and msg
        auto error = std::make_shared<Error>(-1, "");
        _onCommitBlock(error, nullptr);
        return;
    }
    auto blockNumber = _header->number();
    auto start_time = utcTime();

    // default parent block hash located in parentInfo[0]
    if (!isBlockShouldCommit(blockNumber, _header->parentInfo().at(0).blockHash.hex()))
    {
        // TODO: add error code and msg
        auto error = std::make_shared<Error>(-1, "error number");
        _onCommitBlock(error, nullptr);
        return;
    }

    auto ledgerConfig = getLedgerConfig(blockNumber, _header->hash());
    try
    {
        // TODO: check this lambda capture []
        getState()->asyncGetStateCache(blockNumber,
            [&](Error::Ptr _error, std::shared_ptr<TableFactoryInterface> _tableFactory) {
                if (!_error || _error->errorCode() == CommonError::SUCCESS)
                {
                    tbb::parallel_invoke(
                        [this, blockNumber, _tableFactory]() { writeNumber(blockNumber, _tableFactory); },
                        [this, _header, _tableFactory]() { writeHash2Number(_header, _tableFactory); },
                        [this, _header, _tableFactory]() { writeNumber2BlockHeader(_header, _tableFactory); });

                    _tableFactory->asyncCommit([blockNumber, _header, _onCommitBlock, ledgerConfig,
                                                   this](Error::Ptr _error, size_t _commitSize) {
                        if ((!_error || _error->errorCode() == CommonError::SUCCESS) && _commitSize > 0)
                        {
                            m_blockHeaderCache.add(blockNumber, _header);
                            // TODO: add success code and msg
                            auto success = std::make_shared<Error>(0, "");
                            _onCommitBlock(success, ledgerConfig);
                        }
                        else
                        {
                            LEDGER_LOG(ERROR)
                                << LOG_DESC("Commit Block failed") << LOG_KV("number", blockNumber);
                            // TODO: add error code and error msg
                            auto error = std::make_shared<Error>(
                                _error->errorCode(), "" + _error->errorMessage());
                            _onCommitBlock(error, nullptr);
                        }
                    });
                }
                else
                {
                    // TODO: add error code and error msg
                    LEDGER_LOG(ERROR) << LOG_DESC("async get state cache failed")
                                      << LOG_KV("number", blockNumber);
                    auto error =
                        std::make_shared<Error>(_error->errorCode(), "" + _error->errorMessage());
                    _onCommitBlock(error, nullptr);
                    //                return;
                }
            });

        LEDGER_LOG(DEBUG) << LOG_BADGE("Commit") << LOG_DESC("Commit block time record")
                          << LOG_KV("totalTimeCost", utcTime() - start_time);
    }
    catch (OpenSysTableFailed const& e)
    {
        LEDGER_LOG(FATAL)
            << LOG_DESC("[commitBlock]System meets error when try to write block to storage")
            << LOG_KV("EINFO", boost::diagnostic_information(e));
        raise(SIGTERM);
        BOOST_THROW_EXCEPTION(
            OpenSysTableFailed() << errinfo_comment(" write block to storage failed."));
    }
}

void Ledger::asyncStoreTransactions(std::shared_ptr<std::vector<bytesPointer>> _txToStore,
    crypto::HashListPtr _txHashList, std::function<void(Error::Ptr)> _onTxStored)
{
    if (_txHashList->size() != _txToStore->size())
    {
        // TODO: add error code and msg
        auto error = std::make_shared<Error>(-1, "");
        _onTxStored(error);
        return;
    }
    auto start_time = utcTime();
    getLatestBlockNumber([&](protocol::BlockNumber _number) {
      for (size_t i = 0; i < _txHashList->size(); ++i)
      {
          try
          {
              auto write_record_time = utcTime();
              getState()->asyncGetStateCache(
                  _number, [&](Error::Ptr _error, TableFactoryInterface::Ptr _tableFactory) {
                    if (!_error || _error->errorCode() == CommonError::SUCCESS)
                    {
                        auto txHashHex = _txHashList->at(i).hex();
                        getStorageSetter()->setHashToTx(
                            _tableFactory, txHashHex, asString(*(_txToStore->at(i))));

                        _tableFactory->asyncCommit(
                            [txHashHex, _onTxStored](Error::Ptr _error, size_t _commitSize) {
                              if ((!_error || _error->errorCode() == CommonError::SUCCESS) && _commitSize > 0)
                              {
                                  // TODO: add success code and msg
                                  auto success = std::make_shared<Error>(0, "");
                                  _onTxStored(success);
                              }
                              else
                              {
                                  LEDGER_LOG(ERROR)
                                          << LOG_DESC("[#asyncStoreTransactions] write db failed")
                                          << LOG_KV("txHash", txHashHex);
                                  // TODO: add error code and msg
                                  auto error = std::make_shared<Error>(
                                      _error->errorCode(), "" + _error->errorMessage());
                                  _onTxStored(error);
                              }
                            });
                    }
                    else
                    {
                        LEDGER_LOG(ERROR) << LOG_DESC("get state cache failed")
                                          << LOG_KV("txHash", _txHashList->at(i).hex());
                        // TODO: add error code and msg
                        auto error = std::make_shared<Error>(
                            _error->errorCode(), "" + _error->errorMessage());
                        _onTxStored(error);
                    }
                  });
              auto write_table_time = utcTime() - write_record_time;
              LEDGER_LOG(DEBUG) << LOG_BADGE("PreStoreTx") << LOG_DESC("PreStore Txs time record")
                                << LOG_KV("writeTableTime", write_table_time)
                                << LOG_KV("totalTimeCost", utcTime() - start_time);
          }
          catch (OpenSysTableFailed const& e)
          {
              LEDGER_LOG(FATAL)
                      << LOG_DESC(
                          "[#asyncPreStoreTransaction]System meets error when try to write "
                          "block to storage")
                      << LOG_KV("EINFO", boost::diagnostic_information(e));
              raise(SIGTERM);
              BOOST_THROW_EXCEPTION(
                  OpenSysTableFailed() << errinfo_comment(" write block to storage failed."));
          }
      }
    });
}

void Ledger::asyncStoreReceipts(storage::TableFactoryInterface::Ptr _tableFactory,
    protocol::Block::Ptr _block, std::function<void(Error::Ptr)> _onReceiptStored)
{
    if (_block == nullptr || _tableFactory == nullptr)
    {
        LEDGER_LOG(FATAL) << LOG_DESC("Get block null or tableFactory in storage cache");
        // TODO: add error code and msg
        auto error = std::make_shared<Error>(-1, "");
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

        getState()->asyncAddStateCache(blockNumber, _tableFactory,
            [_block, _onReceiptStored, blockNumber, this](Error::Ptr _error) {
                if (!_error || _error->errorCode() == CommonError::SUCCESS)
                {
                    m_transactionsCache.add(blockNumber, blockTransactionListGetter(_block));
                    m_receiptCache.add(blockNumber, blockReceiptListGetter(_block));
                    // TODO: add success code and msg
                    auto success = std::make_shared<Error>(0, "");
                    _onReceiptStored(success);
                }
                else
                {
                    LEDGER_LOG(ERROR)
                        << LOG_DESC("Commit table failed") << LOG_KV("number", blockNumber);
                    // TODO: add error code and error msg
                    auto error =
                        std::make_shared<Error>(_error->errorCode(), "" + _error->errorMessage());
                    _onReceiptStored(error);
                }
            });

        LEDGER_LOG(DEBUG) << LOG_BADGE("Commit") << LOG_DESC("Commit block time record")
                          << LOG_KV("totalTimeCost", utcTime() - start_time);
    }
    catch (OpenSysTableFailed const& e)
    {
        LEDGER_LOG(FATAL)
                << LOG_DESC("[#asyncStoreReceipts]System meets error when try to write data to storage")
                << LOG_KV("EINFO", boost::diagnostic_information(e));
        raise(SIGTERM);
        BOOST_THROW_EXCEPTION(
            OpenSysTableFailed() << errinfo_comment(" write block data to storage failed."));
    }
}

void Ledger::asyncGetBlockDataByNumber(bcos::protocol::BlockNumber _blockNumber, int32_t _blockFlag,
    std::function<void(Error::Ptr, bcos::protocol::Block::Ptr)> _onGetBlock)
{
    getLatestBlockNumber([&](protocol::BlockNumber _number) {
      if (_blockNumber > _number)
      {
          // TODO: to add errorCode and message
          auto error = std::make_shared<Error>(-1, "");
          _onGetBlock(error, nullptr);
          return;
      }
      getBlock(_blockNumber, _blockFlag, [&](Error::Ptr _error, protocol::Block::Ptr _block) {
          if ((!_error || _error->errorCode() == CommonError::SUCCESS) && _block)
          {
              _onGetBlock(nullptr, _block);
          }
          else
          {
              LEDGER_LOG(TRACE) << LOG_DESC("[#asyncGetBlockByNumber]Can't find block, return nullptr")
                                << LOG_KV("blockNumber", _blockNumber);
              // TODO: to add errorCode and message
              auto error =
                  std::make_shared<Error>(_error->errorCode(), "" + _error->errorMessage());
              _onGetBlock(error, nullptr);
          }
      });
    });
}

void Ledger::asyncGetBlockNumber(
    std::function<void(Error::Ptr, bcos::protocol::BlockNumber)> _onGetBlock)
{
    getLatestBlockNumber([&](BlockNumber _number) {
      if (_number == -1)
      {
          // TODO: to add errorCode and message
          auto error = std::make_shared<Error>(-1, "");
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
        // TODO: to add errorCode and message
        auto error = std::make_shared<Error>(-1, "");
        _onGetBlock(error, HashType(""));
        return;
    }
    getLatestBlockNumber([&](BlockNumber _number) {
        if(_blockNumber > _number) {
            // TODO: to add errorCode and message
            auto error = std::make_shared<Error>(-1, "");
            _onGetBlock(error, HashType(""));
            return;
        }
        getStorageGetter()->getBlockHashByNumber(_blockNumber, getMemoryTableFactory(0),
            [&](Error::Ptr _error, std::shared_ptr<std::string> _hash) {
                if ((!_error || (!_error || _error->errorCode() == CommonError::SUCCESS)) && _hash && !_hash->empty())
                {
                    _onGetBlock(nullptr, HashType(*_hash));
                }
                else
                {
                    LEDGER_LOG(ERROR)
                            << LOG_DESC("[#asyncGetBlockHashByNumber] error happened in open table or get entry")
                            << LOG_KV("blockNumber", _blockNumber);

                    // TODO: add error code and msg
                    auto error =
                        std::make_shared<Error>(_error->errorCode(), "" + _error->errorMessage());
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
        // TODO: add error code and msg
        auto error = std::make_shared<Error>(-1, "");
        _onGetBlock(error, -1);
        return;
    }
    getStorageGetter()->getBlockNumberByHash(_blockHash.hex(),
        getMemoryTableFactory(0), [&](Error::Ptr _error, std::shared_ptr<std::string> _numberStr) {
            if ((!_error || _error->errorCode() == CommonError::SUCCESS) && _numberStr && !_numberStr->empty())
            {
                _onGetBlock(nullptr, boost::lexical_cast<BlockNumber>(*_numberStr));
            }
            else
            {
                LEDGER_LOG(ERROR)
                        << LOG_DESC("[#asyncGetBlockHashByNumber] error happened in open table or get entry")
                        << LOG_KV("blockHash", _blockHash.hex());

                // TODO: add error code and msg
                auto error =
                    std::make_shared<Error>(_error->errorCode(), "" + _error->errorMessage());
                _onGetBlock(error, -1);
            }
        });
}

void Ledger::asyncGetBatchTxsByHashList(crypto::HashListPtr _txHashList, bool _withProof,
    std::function<void(Error::Ptr, bcos::protocol::TransactionsPtr,
        std::shared_ptr<std::map<std::string, MerkleProofPtr>>)>
        _onGetTx)
{
    auto txHashStrList = std::make_shared<std::vector<std::string>>();
    tbb::concurrent_unordered_map<std::string, MerkleProofPtr> con_proofMap;
    for (auto& txHash : *_txHashList)
    {
        txHashStrList->emplace_back(txHash.hex());
    }
    getStorageGetter()->getBatchTxByHashList(txHashStrList, getMemoryTableFactory(0),
        getTransactionFactory(), [&](Error::Ptr _error, TransactionsPtr _txList) {
            if((!_error || _error->errorCode() == CommonError::SUCCESS) && _txList)
            {
                if (_txHashList->size() != _txList->size())
                {
                    // TODO: add error code and msg
                    auto error = std::make_shared<Error>(-1, "");
                    _onGetTx(error, nullptr, nullptr);
                    return;
                }
                if (_withProof)
                {
                    tbb::parallel_for(tbb::blocked_range<size_t>(0, _txHashList->size()),
                        [&](const tbb::blocked_range<size_t>& range) {
                            for (size_t i = range.begin(); i < range.end(); ++i)
                            {
                                auto txHash = _txHashList->at(i);
                                getTxProof(txHash, [&](Error::Ptr _error, MerkleProofPtr _proof) {
                                    if ((!_error || _error->errorCode() == CommonError::SUCCESS) && _proof)
                                    {
                                        con_proofMap.emplace(std::make_pair(txHash.hex(), _proof));
                                    }
                                });
                            }
                        });
                    auto proofMap = std::make_shared<std::map<std::string, MerkleProofPtr>>(
                        con_proofMap.begin(), con_proofMap.end());
                    if (proofMap->size() != _txHashList->size())
                    {
                        // TODO: add error code and msg
                        auto error = std::make_shared<Error>(-1, "");
                        _onGetTx(error, nullptr, nullptr);
                    }
                    else
                    {
                        LEDGER_LOG(INFO)
                            << LOG_DESC("") << LOG_KV("txHashListSize", _txHashList->size())
                            << LOG_KV("withProof", _withProof);
                        // TODO: add success code and msg
                        auto success = std::make_shared<Error>(0, "");
                        _onGetTx(success, _txList, proofMap);
                    }
                }
                else
                {
                    LEDGER_LOG(INFO)
                        << LOG_DESC("") << LOG_KV("txHashListSize", _txHashList->size())
                        << LOG_KV("withProof", _withProof);
                    // TODO: add success code and msg
                    auto success = std::make_shared<Error>(0, "");
                    _onGetTx(success, _txList, nullptr);
                }
            }
            else
            {
                // TODO: add error msg
                auto error =
                    std::make_shared<Error>(_error->errorCode(), "" + _error->errorMessage());
                _onGetTx(error, nullptr, nullptr);
            }
        });
}

void Ledger::asyncGetTransactionReceiptByHash(bcos::crypto::HashType const& _txHash,
    bool _withProof,
    std::function<void(Error::Ptr, bcos::protocol::TransactionReceipt::ConstPtr, MerkleProofPtr)>
        _onGetTx)
{
    getStorageGetter()->getReceiptByTxHash(_txHash.hex(), getMemoryTableFactory(0),
        [&](Error::Ptr _error, std::shared_ptr<std::string> _receiptStr) {
            if ((!_error || _error->errorCode() == CommonError::SUCCESS) && _receiptStr && !_receiptStr->empty())
            {
                auto receipt = decodeReceipt(getReceiptFactory(), *_receiptStr);
                if (_withProof)
                {
                    auto merkleProof = std::make_shared<MerkleProof>();
                    auto number = receipt->blockNumber();
                    getStorageGetter()->getTxsFromStorage(number, getMemoryTableFactory(0),
                        [&](Error::Ptr _error, std::shared_ptr<std::string> _blockStr) {
                            if ((!_error || _error->errorCode() == CommonError::SUCCESS) && _blockStr &&
                                !_blockStr->empty())
                            {
                                auto block = decodeBlock(m_blockFactory, *_blockStr);
                                auto txHashList = blockTxHashListGetter(block);
                                auto txHashHexList = std::make_shared<std::vector<std::string>>();
                                for (auto& txHash : *txHashList)
                                {
                                    txHashHexList->emplace_back(txHash.hex());
                                }
                                getStorageGetter()->getBatchReceiptsByHashList(txHashHexList,
                                    getMemoryTableFactory(0), getReceiptFactory(),
                                    [&](Error::Ptr _error, ReceiptsPtr receipts) {
                                        if ((!_error || _error->errorCode() == CommonError::SUCCESS) &&
                                            receipts && !receipts->empty())
                                        {
                                            auto parent2ChildList =
                                                getParent2ChildListByReceiptProofCache(number, receipts);
                                            auto child2Parent =
                                                getChild2ParentCacheByReceipt(parent2ChildList, number);
                                            getMerkleProof(receipt->hash(), *parent2ChildList, *child2Parent,
                                                           *merkleProof);
                                            _onGetTx(nullptr, receipt, merkleProof);
                                        }
                                        else
                                        {
                                            // TODO: add error code and message
                                            auto error = std::make_shared<Error>(
                                                _error->errorCode(), "" + _error->errorMessage());
                                            _onGetTx(error, nullptr, nullptr);
                                        }
                                    });
                            }
                            else
                            {
                                // TODO: add error code and message
                                auto error = std::make_shared<Error>(
                                    _error->errorCode(), "" + _error->errorMessage());
                                _onGetTx(error, nullptr, nullptr);
                            }
                        });
                }
                else
                {
                    // TODO: add success msg
                    auto success = std::make_shared<Error>(CommonError::SUCCESS, "");
                    _onGetTx(success, receipt, nullptr);
                }
            }
            else
            {
                LEDGER_LOG(ERROR) << LOG_DESC("") << LOG_KV("txHash", _txHash.hex());
                // TODO: add error code and message
                auto error =
                    std::make_shared<Error>(_error->errorCode(), "" + _error->errorMessage());
                _onGetTx(error, nullptr, nullptr);
            }
        });
}

void Ledger::asyncGetTotalTransactionCount(
    std::function<void(Error::Ptr, int64_t, int64_t, bcos::protocol::BlockNumber)> _callback)
{
    getStorageGetter()->getCurrentState(SYS_KEY_TOTAL_TRANSACTION_COUNT, getMemoryTableFactory(0),
        [&](Error::Ptr _error, std::shared_ptr<std::string> totalCountStr) {
            if ((!_error || _error->errorCode() == CommonError::SUCCESS) && totalCountStr)
            {
                getStorageGetter()->getCurrentState(SYS_KEY_TOTAL_FAILED_TRANSACTION,
                    getMemoryTableFactory(0),
                    [totalCountStr, _callback, this](
                        Error::Ptr _error, std::shared_ptr<std::string> totalFailedStr) {
                        if ((!_error || _error->errorCode() == CommonError::SUCCESS) && totalFailedStr)
                        {
                            getLatestBlockNumber([totalCountStr, totalFailedStr, _callback](
                                                     BlockNumber _number) {
                                auto totalCount = boost::lexical_cast<int64_t>(totalCountStr);
                                auto totalFailed = boost::lexical_cast<int64_t>(totalFailedStr);

                                _callback(nullptr, totalCount, totalFailed, _number);
                            });
                        }
                        else
                        {
                            LEDGER_LOG(ERROR) << LOG_DESC(
                                "[#asyncGetTotalTransactionCount] error happened in get "
                                "SYS_KEY_TOTAL_FAILED_TRANSACTION");
                            // TODO: add error code and msg
                            auto error = std::make_shared<Error>(
                                _error->errorCode(), "" + _error->errorMessage());
                            _callback(error, -1, -1, -1);
                        }
                    });
            }
            else
            {
                LEDGER_LOG(ERROR) << LOG_DESC(
                    "[#asyncGetTotalTransactionCount] error happened in get "
                    "SYS_KEY_TOTAL_TRANSACTION_COUNT");
                // TODO: add error code and msg
                auto error =
                    std::make_shared<Error>(_error->errorCode(), "" + _error->errorMessage());
                _callback(error, -1, -1, -1);
            }
        });
}

void Ledger::asyncGetSystemConfigByKey(const std::string& _key,
    std::function<void(Error::Ptr, std::string, bcos::protocol::BlockNumber)> _onGetConfig)
{
    getStorageGetter()->getSysConfig(_key, getMemoryTableFactory(0),
        [&](Error::Ptr _error, std::shared_ptr<stringsPair> _config) {
            if ((!_error || _error->errorCode() == CommonError::SUCCESS) && _config && !_config->second.empty())
            {
                LEDGER_LOG(TRACE) << LOG_DESC("[#asyncGetSystemConfigByKey]Data in db")
                                  << LOG_KV("key", _key) << LOG_KV("value", _config->first);
                // TODO: add success code and msg
                auto success = std::make_shared<Error>(0, "");
                _onGetConfig(
                    success, _config->first, boost::lexical_cast<BlockNumber>(_config->second));
            }
            else
            {
                LEDGER_LOG(ERROR)
                    << LOG_DESC("[#asyncGetSystemConfigByKey] Null pointer of getSysConfig")
                    << LOG_KV("key", _key);
                // TODO: add error code and error msg
                auto error = std::make_shared<Error>(-1, "");
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
          // TODO: to add errorCode and message
          auto error = std::make_shared<Error>(-1, "");
          _onGetList(error, nullptr);
          return;
      }
      auto endNumber =
          (_startNumber + _offset > _number) ? _number : (_startNumber + _offset);
        getStorageGetter()->getNoncesBatchFromStorage(_startNumber, endNumber,
            getMemoryTableFactory(0), m_blockFactory,
            [&](Error::Ptr _error,
                std::shared_ptr<std::map<protocol::BlockNumber, protocol::NonceListPtr>> _nonceMap) {
              if ((!_error || _error->errorCode() == CommonError::SUCCESS) && _nonceMap && !_nonceMap->empty())
              {
                  if (_nonceMap->size() == size_t(endNumber - _startNumber + 1))
                  {
                      LEDGER_LOG(TRACE) << LOG_DESC("[#asyncGetBlockHashByNumber] get nonceList enough")
                                        << LOG_KV("listSize", endNumber - _startNumber + 1);

                      // TODO: add success msg
                      auto success = std::make_shared<Error>(CommonError::SUCCESS, "");
                      _onGetList(success, _nonceMap);
                  } else {
                      LEDGER_LOG(ERROR) << LOG_DESC("[#asyncGetBlockHashByNumber] not get enough nonceLists")
                                        << LOG_KV("startBlockNumber", _startNumber)
                                        << LOG_KV("endBlockNumber", endNumber);
                      // TODO: add error code and msg
                      auto error = std::make_shared<Error>(-1, "");
                      _onGetList(error, nullptr);
                  }
              }
              else
              {
                  LEDGER_LOG(ERROR)
                          << LOG_DESC("[#asyncGetNonceList] error happened in open table or get entry");
                  // TODO: add error code and msg
                  auto error = std::make_shared<Error>(-1, "");
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
        // TODO: to add errorCode and message
        auto error = std::make_shared<Error>(-1, "");
        _onGetConfig(error, nullptr);
        return;
    }
    getLatestBlockNumber([&](BlockNumber _number) {
        getStorageGetter()->getConsensusConfig(_type, _number, getMemoryTableFactory(0),
            m_blockFactory->cryptoSuite()->keyFactory(),
            [&](Error::Ptr _error, consensus::ConsensusNodeListPtr _nodeList) {
                if ((!_error || _error->errorCode() == CommonError::SUCCESS) && !_nodeList &&
                    !_nodeList->empty())
                {
                    // TODO: add success code and msg
                    auto success = std::make_shared<Error>(CommonError::SUCCESS, "");
                    _onGetConfig(success, _nodeList);
                }
                else
                {
                    LEDGER_LOG(ERROR)
                            << LOG_DESC("[#asyncGetNodeListByType] error happened in open table or get entry")
                            << LOG_KV("blockNumber", _number);
                    // TODO: add error msg
                    auto error =
                        std::make_shared<Error>(_error->errorCode(), "" + _error->errorMessage());
                    _onGetConfig(error, nullptr);
                }
            });
    });
}

// FIXME: check async
void Ledger::getBlock(const BlockNumber& _blockNumber, int32_t _blockFlag,
    std::function<void(Error::Ptr, protocol::Block::Ptr)> _onGetBlock)
{
    getLatestBlockNumber([&](BlockNumber _number) {
        if(_blockNumber > _number){
            auto error = std::make_shared<Error>(-1, "");
            _onGetBlock(error, nullptr);
        }
    });
    auto start_time = utcTime();
    auto record_time = utcTime();
    auto cachedBlock = m_blockCache.get(_blockNumber);
    auto getCache_time_cost = utcTime() - record_time;
    record_time = utcTime();

    if (bool(cachedBlock.second))
    {
        LEDGER_LOG(TRACE) << LOG_DESC("[#getBlock]Cache hit, read from cache")
                          << LOG_KV("blockNumber", _blockNumber);
        // TODO: add success msg
        auto success = std::make_shared<Error>(CommonError::SUCCESS, "");
        _onGetBlock(success, cachedBlock.second);
    }
    else
    {
        LEDGER_LOG(TRACE) << LOG_DESC("[#getBlock]Cache missed, read from storage")
                          << LOG_KV("blockNumber", _blockNumber);
        auto block = m_blockFactory->createBlock();
        if(_blockFlag & HEADER)
        {
            getBlockHeader(_blockNumber, [&](Error::Ptr _error, BlockHeader::Ptr _header) {
                if ((!_error || _error->errorCode() == CommonError::SUCCESS) && _header)
                {
                    block->setBlockHeader(_header);
                }
                else
                {
                    LEDGER_LOG(ERROR) << LOG_DESC("[#getBlock]Can't find the header")
                                      << LOG_KV("blockNumber", _blockNumber);
                }
            });
        }
        if(_blockFlag & TRANSACTIONS)
        {
            getTxs(_blockNumber, [&](Error::Ptr _error, bcos::protocol::TransactionsPtr _txs) {
              if ((!_error || _error->errorCode() == CommonError::SUCCESS) && _txs)
              {
                  if(_txs->size() != blockTransactionListSetter(block, _txs)){
                      LEDGER_LOG(TRACE) << LOG_DESC("[#getBlock] insert block transactions error")
                                        << LOG_KV("blockNumber", _blockNumber);
                  }
              }
              else
              {
                  LEDGER_LOG(ERROR) << LOG_DESC("[#getBlock]Can't find the Txs")
                                    << LOG_KV("blockNumber", _blockNumber);
              }
            });
        }
        if(_blockFlag & RECEIPTS)
        {
            getReceipts(_blockNumber, [&](Error::Ptr _error, protocol::ReceiptsPtr _receipts) {
              if ((!_error || _error->errorCode() == CommonError::SUCCESS) && _receipts)
              {
                  if(_receipts->size() != blockReceiptListSetter(block, _receipts)){
                      LEDGER_LOG(TRACE) << LOG_DESC("[#getBlock] insert block receipts error")
                                        << LOG_KV("blockNumber", _blockNumber);
                  }
              }
              else
              {
                  LEDGER_LOG(ERROR) << LOG_DESC("[#getBlock]Can't find the Receipts")
                                    << LOG_KV("blockNumber", _blockNumber);
              }
            });
        }
        // it means _blockFlag has multiple 1 in binary
        // should wait for all datum callback
        if(_blockFlag & (_blockFlag - 1)){
//            auto start_time = utcSteadyTime();
//            while (utcSteadyTime() - start_time < m_timeout)
//            {
//                if (number != -2 && !hash.empty())
//                {
//                    break;
//                }
//                if(!ret) {
//                    break;
//                }
//                boost::unique_lock<boost::mutex> l(x_signalled);
//                m_signalled.wait_for(l, boost::chrono::milliseconds(10));
//            }
        }
        if(!(_blockFlag ^ FULL_BLOCK)){
            // get full block data
            LEDGER_LOG(TRACE) << LOG_DESC("[#getBlock]Write to cache");
            auto blockPtr = m_blockCache.add(_blockNumber, block);

            _onGetBlock(nullptr, blockPtr);
            return;
        }
        auto assemble_block = utcTime() - record_time;
        LEDGER_LOG(DEBUG) << LOG_DESC("Get block from db")
                          << LOG_KV("getCacheTimeCost", getCache_time_cost)
                          << LOG_KV("constructBlockTimeCost", assemble_block)
                          << LOG_KV("totalTimeCost", utcTime() - start_time);
        _onGetBlock(nullptr, block);
    }
}

void Ledger::getLatestBlockNumber(std::function<void(protocol::BlockNumber)> _onGetNumber)
{
    getStorageGetter()->getCurrentState(SYS_KEY_CURRENT_NUMBER, getMemoryTableFactory(0),
        [&](Error::Ptr _error, std::shared_ptr<std::string> _currentNumber) {
            if ((!_error || _error->errorCode() == CommonError::SUCCESS) && _currentNumber && !_currentNumber->empty())
            {
                BlockNumber number = boost::lexical_cast<BlockNumber>(*_currentNumber);
                _onGetNumber(number);
            }
            else
            {
                LEDGER_LOG(ERROR) << LOG_DESC("[#getLatestBlockNumber]Get number from storage error")
                                  << LOG_KV("errorCode", _error->errorCode())
                                  << LOG_KV("errorMsg", _error->errorMessage());
                _onGetNumber(-1);
            }
        });
}

void Ledger::getBlockHeader(const bcos::protocol::BlockNumber& _blockNumber,
    std::function<void(Error::Ptr, BlockHeader::Ptr)> _onGetHeader)
{
    if (_blockNumber < 0 )
    {
        // TODO: add error msg and code
        auto error = std::make_shared<Error>(-1, "");
        _onGetHeader(error, nullptr);
        return;
    }
    auto start_time = utcTime();
    auto record_time = utcTime();
    auto cachedBlock = m_blockCache.get(_blockNumber);
    auto cachedHeader = m_blockHeaderCache.get(_blockNumber);
    auto getCache_time_cost = utcTime() - record_time;
    record_time = utcTime();

    if (bool(cachedBlock.second) && bool( cachedBlock.second->blockHeader()))
    {
        LEDGER_LOG(TRACE) << LOG_DESC("[#getBlockHeader]CacheBlock hit, read from cache")
                          << LOG_KV("blockNumber", _blockNumber);
        // TODO: add success msg
        auto success = std::make_shared<Error>(CommonError::SUCCESS, "");
        _onGetHeader(success, cachedBlock.second->blockHeader());
    }
    else if (bool(cachedHeader.second))
    {
        LEDGER_LOG(TRACE) << LOG_DESC("[#getBlockHeader]CacheHeader hit, read from cache")
                          << LOG_KV("blockNumber", _blockNumber);
        // TODO: add success msg
        auto success = std::make_shared<Error>(CommonError::SUCCESS, "");
        _onGetHeader(success, cachedHeader.second);
    }
    else
    {
        LEDGER_LOG(TRACE) << LOG_DESC("[#getBlockHeader]Cache missed, read from storage")
                          << LOG_KV("blockNumber", _blockNumber);
        getStorageGetter()->getBlockHeaderFromStorage(_blockNumber, getMemoryTableFactory(0),
            [&](Error::Ptr _error, std::shared_ptr<std::string> _headerStr) {
                if ((!_error || _error->errorCode() == CommonError::SUCCESS) && _headerStr && !_headerStr->empty())
                {
                    auto headerPtr = decodeBlockHeader(getBlockHeaderFactory(), *_headerStr);
                    LEDGER_LOG(TRACE) << LOG_DESC("[#getBlockHeader]Get header from storage")
                                      << LOG_KV("blockNumber", _blockNumber);
                    // TODO: add success msg
                    auto success = std::make_shared<Error>(CommonError::SUCCESS, "");
                    LEDGER_LOG(TRACE) << LOG_DESC("[#getBlockHeader]Write to cache");
                    m_blockHeaderCache.add(_blockNumber, headerPtr);
                    _onGetHeader(success, headerPtr);
                }
                else
                {
                    LEDGER_LOG(ERROR) << LOG_DESC("[#getBlockHeader]Get header from storage error")
                                      << LOG_KV("errorCode", _error->errorCode())
                                      << LOG_KV("errorMsg", _error->errorMessage())
                                      << LOG_KV("blockNumber", _blockNumber);
                    // TODO: add error code and msg
                    auto error =
                        std::make_shared<Error>(_error->errorCode(), "" + _error->errorMessage());
                    _onGetHeader(error, nullptr);
                }
            });
        auto storage_getter_time = utcTime() - record_time;
        record_time = utcTime();
        LEDGER_LOG(DEBUG) << LOG_DESC("Get Header from db")
                          << LOG_KV("getCacheTimeCost", getCache_time_cost)
                          << LOG_KV("storageGetterTimeCost", storage_getter_time)
                          << LOG_KV("totalTimeCost", utcTime() - start_time);
    }
}

void Ledger::getTxs(const bcos::protocol::BlockNumber& _blockNumber,
    std::function<void(Error::Ptr, bcos::protocol::TransactionsPtr)> _onGetTxs)
{
    if (_blockNumber < 0 )
    {
        // TODO: add error msg and code
        auto error = std::make_shared<Error>(-1, "");
        _onGetTxs(error, nullptr);
        return;
    }
    auto start_time = utcTime();
    auto record_time = utcTime();
    auto cachedBlock = m_blockCache.get(_blockNumber);
    auto cachedTransactions = m_transactionsCache.get(_blockNumber);
    auto getCache_time_cost = utcTime() - record_time;
    record_time = utcTime();

    if (bool(cachedBlock.second) && cachedBlock.second->transactionsSize() != 0)
    {
        LEDGER_LOG(TRACE) << LOG_DESC("[#getTxs]CacheBlock hit, read from cache")
                          << LOG_KV("blockNumber", _blockNumber);
        // TODO: add success msg
        auto success = std::make_shared<Error>(CommonError::SUCCESS, "");
        _onGetTxs(success, blockTransactionListGetter(cachedBlock.second));
    }
    else if (bool(cachedTransactions.second))
    {
        LEDGER_LOG(TRACE) << LOG_DESC("[#getTxs]CacheTxs hit, read from cache")
                          << LOG_KV("blockNumber", _blockNumber);
        // TODO: add success msg
        auto success = std::make_shared<Error>(CommonError::SUCCESS, "");
        _onGetTxs(success, cachedTransactions.second);
    }
    else
    {
        LEDGER_LOG(TRACE) << LOG_DESC("[#getTxs]Cache missed, read from storage")
                          << LOG_KV("blockNumber", _blockNumber);
        // block with tx hash list
        getStorageGetter()->getTxsFromStorage(_blockNumber, getMemoryTableFactory(0),
            [&](Error::Ptr _error, std::shared_ptr<std::string> _blockStr) {
                if ((!_error || _error->errorCode() == CommonError::SUCCESS) && !_blockStr->empty())
                {
                    auto block = decodeBlock(m_blockFactory, *_blockStr);
                    auto txHashList = blockTxHashListGetter(block);
                    auto txHashHexList = std::make_shared<std::vector<std::string>>();
                    for (auto& txHash : *txHashList)
                    {
                        txHashHexList->emplace_back(txHash.hex());
                    }
                    getStorageGetter()->getBatchTxByHashList(txHashHexList,
                        getMemoryTableFactory(0), getTransactionFactory(),
                        [&](Error::Ptr _error, protocol::TransactionsPtr _txs) {
                            if ((!_error || _error->errorCode() == CommonError::SUCCESS) && _txs != nullptr)
                            {
                                LEDGER_LOG(TRACE) << LOG_DESC("[#getTxs]Get txs from storage")
                                                  << LOG_KV("txsSize", _txs->size());
                                // TODO: add success msg
                                auto success = std::make_shared<Error>(CommonError::SUCCESS, "");
                                LEDGER_LOG(TRACE) << LOG_DESC("[#getTxs]Write to cache");
                                m_transactionsCache.add(_blockNumber, _txs);
                                _onGetTxs(success, _txs);
                            }
                            else
                            {
                                LEDGER_LOG(ERROR) << LOG_DESC("[#getTxs]Get txs from storage error")
                                                  << LOG_KV("errorCode", _error->errorCode())
                                                  << LOG_KV("errorMsg", _error->errorMessage())
                                                  << LOG_KV("txsSize", _txs->size());
                                // TODO: add error code and msg
                                auto error = std::make_shared<Error>(-1, "");
                                _onGetTxs(error, nullptr);
                            }
                        });
                }
                else
                {
                    LEDGER_LOG(ERROR) << LOG_DESC("[#getTxs]Get txHashList from storage error")
                                      << LOG_KV("errorCode", _error->errorCode())
                                      << LOG_KV("errorMsg", _error->errorMessage())
                                      << LOG_KV("blockNumber", _blockNumber);
                    // TODO: add error code and msg
                    auto error =
                        std::make_shared<Error>(_error->errorCode(), "" + _error->errorMessage());
                    _onGetTxs(error, nullptr);
                }
            });
        auto decode_txs_time_cost = utcTime() - record_time;
        LEDGER_LOG(DEBUG) << LOG_DESC("Get Txs from db")
                          << LOG_KV("getCacheTimeCost", getCache_time_cost)
                          << LOG_KV("decodeTxsTimeCost", decode_txs_time_cost)
                          << LOG_KV("totalTimeCost", utcTime() - start_time);
    }
}

void Ledger::getReceipts(const bcos::protocol::BlockNumber& _blockNumber,
    std::function<void(Error::Ptr, bcos::protocol::ReceiptsPtr)> _onGetReceipts)
{
    if (_blockNumber < 0 )
    {
        // TODO: add error msg and code
        auto error = std::make_shared<Error>(-1, "");
        _onGetReceipts(error, nullptr);
        return;
    }
    auto start_time = utcTime();
    auto record_time = utcTime();
    auto cachedBlock = m_blockCache.get(_blockNumber);
    auto cachedReceipts = m_receiptCache.get(_blockNumber);
    auto getCache_time_cost = utcTime() - record_time;
    record_time = utcTime();

    if (bool(cachedBlock.second) && cachedBlock.second->receiptsSize() != 0)
    {
        LEDGER_LOG(TRACE) << LOG_DESC("[#getReceipts]CacheBlock hit, read from cache")
                          << LOG_KV("blockNumber", _blockNumber);
        // TODO: add success msg
        auto success = std::make_shared<Error>(CommonError::SUCCESS, "");
        _onGetReceipts(success, blockReceiptListGetter(cachedBlock.second));
    }
    else if (bool(cachedReceipts.second))
    {
        LEDGER_LOG(TRACE) << LOG_DESC("[#getReceipts]Cache Receipts hit, read from cache")
                          << LOG_KV("blockNumber", _blockNumber);
        // TODO: add success msg
        auto success = std::make_shared<Error>(CommonError::SUCCESS, "");
        _onGetReceipts(success, cachedReceipts.second);
    }
    else
    {
        LEDGER_LOG(TRACE) << LOG_DESC("[#getReceipts]Cache missed, read from storage")
                          << LOG_KV("blockNumber", _blockNumber);
        // block with tx hash list
        getStorageGetter()->getTxsFromStorage(_blockNumber, getMemoryTableFactory(0),
            [&](Error::Ptr _error, std::shared_ptr<std::string> _blockStr) {
                if ((!_error || _error->errorCode() == CommonError::SUCCESS) && !_blockStr->empty())
                {
                    auto block = decodeBlock(m_blockFactory, *_blockStr);

                    auto txHashList = blockTxHashListGetter(block);
                    auto txHashHexList = std::make_shared<std::vector<std::string>>();
                    for (auto& txHash : *txHashList)
                    {
                        txHashHexList->emplace_back(txHash.hex());
                    }

                    getStorageGetter()->getBatchReceiptsByHashList(txHashHexList,
                        getMemoryTableFactory(0), getReceiptFactory(),
                        [&](Error::Ptr _error, ReceiptsPtr _receipts) {
                            if (!_error || _error->errorCode() == CommonError::SUCCESS)
                            {
                                LEDGER_LOG(TRACE)
                                        << LOG_DESC("[#getReceipts]Get receipts from storage")
                                        << LOG_KV("receiptSize", _receipts->size());
                                // TODO: add success msg
                                auto success = std::make_shared<Error>(CommonError::SUCCESS, "");
                                LEDGER_LOG(TRACE) << LOG_DESC("[#getReceipts]Write to cache");
                                m_receiptCache.add(_blockNumber, _receipts);
                                _onGetReceipts(success, _receipts);
                            }
                            else
                            {
                                LEDGER_LOG(ERROR)
                                    << LOG_DESC("[#getReceipts]Get receipts from storage error")
                                    << LOG_KV("errorCode", _error->errorCode())
                                    << LOG_KV("errorMsg", _error->errorMessage())
                                    << LOG_KV("blockNumber", _blockNumber);
                                // TODO: add error code and msg
                                auto error = std::make_shared<Error>(-1, "");
                                _onGetReceipts(error, nullptr);
                            }
                        });
                }
                else
                {
                    LEDGER_LOG(ERROR) << LOG_DESC("[#getReceipts]Get receipts from storage error")
                                      << LOG_KV("errorCode", _error->errorCode())
                                      << LOG_KV("errorMsg", _error->errorMessage())
                                      << LOG_KV("blockNumber", _blockNumber);
                    // TODO: add error code and msg
                    auto error =
                        std::make_shared<Error>(_error->errorCode(), "" + _error->errorMessage());
                    _onGetReceipts(error, nullptr);
                }
            });
        auto decode_receipts_time_cost = utcTime() - record_time;
        LEDGER_LOG(DEBUG) << LOG_DESC("Get Receipts from db")
                          << LOG_KV("getCacheTimeCost", getCache_time_cost)
                          << LOG_KV("decodeTxsTimeCost", decode_receipts_time_cost)
                          << LOG_KV("totalTimeCost", utcTime() - start_time);
    }
}

void Ledger::getTxProof(
    const HashType& _txHash, std::function<void(Error::Ptr, MerkleProofPtr)> _onGetProof)
{
    getStorageGetter()->getReceiptByTxHash(
        _txHash.hex(), getMemoryTableFactory(0), [&](Error::Ptr _error, std::shared_ptr<std::string> _receiptStr) {
            if (!_error || _error->errorCode() == CommonError::SUCCESS)
            {
                MerkleProofPtr merkleProofPtr = nullptr;
                if (! _receiptStr->empty())
                {
                    auto receipt = decodeReceipt(getReceiptFactory(), *_receiptStr);
                    auto blockNumber = receipt->blockNumber();
                    getTxs(blockNumber, [&](Error::Ptr _error, TransactionsPtr _txs) {
                        if((!_error || _error->errorCode() == CommonError::SUCCESS) && _txs!= nullptr && !_txs->empty())
                        {
                            if (!_txs || _txs->empty())
                            {
                                LEDGER_LOG(TRACE) << LOG_DESC("[#getTxProof] get txs error")
                                                  << LOG_KV("blockNumber", blockNumber)
                                                  << LOG_KV("txHash", _txHash.hex());
                                return;
                            }
                            auto merkleProof = std::make_shared<MerkleProof>();
                            auto parent2ChildList =
                                getParent2ChildListByTxsProofCache(blockNumber, _txs);
                            auto child2Parent =
                                getChild2ParentCacheByTransaction(parent2ChildList, blockNumber);
                            getMerkleProof(_txHash, *parent2ChildList, *child2Parent, *merkleProof);

                            // TODO: add success msg
                            auto success = std::make_shared<Error>(CommonError::SUCCESS, "");
                            _onGetProof(success, merkleProofPtr);
                        }
                        else
                        {
                            // TODO: add error msg
                            auto error = std::make_shared<Error>(
                                _error->errorCode(), "" + _error->errorMessage());
                            _onGetProof(error, nullptr);
                        }
                    });
                }
                else{
                    // TODO: add error code and msg
                    auto error = std::make_shared<Error>(-1, "");
                    _onGetProof(error, nullptr);
                }
            }
            else
            {
                LEDGER_LOG(TRACE) << LOG_DESC("[#getTxProof] get txs error")
                                  << LOG_KV("errorCode", _error->errorCode())
                                  << LOG_KV("errorMsg", _error->errorMessage())
                                  << LOG_KV("txHash", _txHash.hex());
                // TODO: add error code and msg
                auto error =
                    std::make_shared<Error>(_error->errorCode(), "" + _error->errorMessage());
                _onGetProof(error, nullptr);
            }
        });
}

// FIXME: turn function to sync
LedgerConfig::Ptr Ledger::getLedgerConfig(protocol::BlockNumber _number, const crypto::HashType& _hash){
    auto ledgerConfig = std::make_shared<LedgerConfig>();
    ledgerConfig->setBlockNumber(_number);
    ledgerConfig->setHash(_hash);
    asyncGetSystemConfigByKey(
        SYSTEM_KEY_CONSENSUS_TIMEOUT, [&](Error::Ptr _error, std::string _value, BlockNumber) {
            if (_error->errorCode() == 0)
            {
                ledgerConfig->setConsensusTimeout(boost::lexical_cast<uint64_t>(_value));
            }
            else
            {
                LEDGER_LOG(ERROR) << LOG_DESC("") << LOG_KV("getKey", SYSTEM_KEY_CONSENSUS_TIMEOUT)
                                  << LOG_KV("errorCode", _error->errorCode())
                                  << LOG_KV("errorMsg", _error->errorMessage());
            }
        });
    asyncGetSystemConfigByKey(
        SYSTEM_KEY_TX_COUNT_LIMIT, [&](Error::Ptr _error, std::string _value, BlockNumber) {
            if (_error->errorCode() == 0)
            {
                ledgerConfig->setBlockTxCountLimit(boost::lexical_cast<uint64_t>(_value));
            }
            else
            {
                LEDGER_LOG(ERROR) << LOG_DESC("") << LOG_KV("getKey", SYSTEM_KEY_TX_COUNT_LIMIT)
                                  << LOG_KV("errorCode", _error->errorCode())
                                  << LOG_KV("errorMsg", _error->errorMessage());
            }
        });
    asyncGetNodeListByType(
        CONSENSUS_SEALER, [&](Error::Ptr _error, consensus::ConsensusNodeListPtr _nodeList) {
            if (_error->errorCode() == 0)
            {
                ledgerConfig->setConsensusNodeList(*_nodeList);
            }
            else
            {
                LEDGER_LOG(ERROR) << LOG_DESC("") << LOG_KV("getKey", CONSENSUS_SEALER)
                                  << LOG_KV("errorCode", _error->errorCode())
                                  << LOG_KV("errorMsg", _error->errorMessage());
            }
        });

    asyncGetNodeListByType(
        CONSENSUS_OBSERVER, [&](Error::Ptr _error, consensus::ConsensusNodeListPtr _nodeList) {
            if (_error->errorCode() == 0)
            {
                ledgerConfig->setObserverNodeList(*_nodeList);
            }
            else
            {
                LEDGER_LOG(ERROR) << LOG_DESC("") << LOG_KV("getKey", CONSENSUS_OBSERVER)
                                  << LOG_KV("errorCode", _error->errorCode())
                                  << LOG_KV("errorMsg", _error->errorMessage());
            }
        });
    return ledgerConfig;
}

std::shared_ptr<Child2ParentMap> Ledger::getChild2ParentCacheByReceipt(
    std::shared_ptr<Parent2ChildListMap> _parent2ChildList, BlockNumber _blockNumber)
{
    return getChild2ParentCache(
        x_receiptChild2ParentCache, m_receiptChild2ParentCache, _parent2ChildList, _blockNumber);
}

std::shared_ptr<Child2ParentMap> Ledger::getChild2ParentCacheByTransaction(
    std::shared_ptr<Parent2ChildListMap> _parent2Child, BlockNumber _blockNumber)
{
    return getChild2ParentCache(
        x_txsChild2ParentCache, m_txsChild2ParentCache, _parent2Child, _blockNumber);
}

std::shared_ptr<Child2ParentMap> Ledger::getChild2ParentCache(SharedMutex& _mutex,
    std::pair<BlockNumber, std::shared_ptr<Child2ParentMap>>& _cache,
    std::shared_ptr<Parent2ChildListMap> _parent2Child, BlockNumber _blockNumber)
{
    UpgradableGuard l(_mutex);
    if (_cache.second && _cache.first == _blockNumber)
    {
        return _cache.second;
    }
    UpgradeGuard ul(l);
    // After preempting the write lock, judge again whether m_receiptWithProof has been updated
    // to prevent lock competition
    if (_cache.second && _cache.first == _blockNumber)
    {
        return _cache.second;
    }
    std::shared_ptr<Child2ParentMap> child2Parent = std::make_shared<Child2ParentMap>();
    parseMerkleMap(_parent2Child, *child2Parent);
    _cache = std::make_pair(_blockNumber, child2Parent);
    return child2Parent;
}

std::shared_ptr<Parent2ChildListMap> Ledger::getParent2ChildListByReceiptProofCache(
    protocol::BlockNumber _blockNumber, protocol::ReceiptsPtr _receipts)
{
    UpgradableGuard l(m_receiptWithProofMutex);
    // cache for the block parent2ChildList
    if (m_receiptWithProof.second && m_receiptWithProof.first == _blockNumber)
    {
        return m_receiptWithProof.second;
    }

    UpgradeGuard ul(l);
    // After preempting the write lock, judge again whether m_receiptWithProof has been updated
    // to prevent lock competition
    if (m_receiptWithProof.second && m_receiptWithProof.first == _blockNumber)
    {
        return m_receiptWithProof.second;
    }

    auto parent2ChildList = getReceiptProof(m_blockFactory->cryptoSuite(), _receipts);
    m_receiptWithProof = std::make_pair(_blockNumber, parent2ChildList);
    return parent2ChildList;
}

std::shared_ptr<Parent2ChildListMap> Ledger::getParent2ChildListByTxsProofCache(
    protocol::BlockNumber _blockNumber, protocol::TransactionsPtr _txs)
{
    UpgradableGuard l(m_transactionWithProofMutex);
    // cache for the block parent2ChildList
    if (m_transactionWithProof.second &&
        m_transactionWithProof.first == _blockNumber)
    {
        return m_transactionWithProof.second;
    }
    UpgradeGuard ul(l);
    // After preempting the write lock, judge again whether m_transactionWithProof has been
    // updated to prevent lock competition
    if (m_transactionWithProof.second &&
        m_transactionWithProof.first == _blockNumber)
    {
        return m_transactionWithProof.second;
    }
    auto parent2ChildList = getTransactionProof(m_blockFactory->cryptoSuite(), _txs);
    m_transactionWithProof = std::make_pair(_blockNumber, parent2ChildList);
    return parent2ChildList;
}

bool Ledger::isBlockShouldCommit(const BlockNumber& _blockNumber, const std::string& _parentHash)
{
    BlockNumber number = -2;
    std::string hash;
    bool ret = true;
    getLatestBlockNumber([&](protocol::BlockNumber _number) {
        number = _number;
        getStorageGetter()->getBlockHashByNumber(
            _number, getMemoryTableFactory(0), [&](Error::Ptr _error, std::shared_ptr<std::string> _hash) {
                if ((!_error || _error->errorCode() == CommonError::SUCCESS) && _hash)
                {
                    hash = std::move(*_hash);
                }
                else
                {
                    LEDGER_LOG(ERROR)
                        << LOG_DESC("[#isBlockShouldCommit]Get block hash error")
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
        if(!ret) {
            break;
        }
        boost::unique_lock<boost::mutex> l(x_signalled);
        m_signalled.wait_for(l, boost::chrono::milliseconds(10));
    }
    if (_blockNumber == number + 1 && _parentHash == hash && ret)
    {
        return true;
    }
    LEDGER_LOG(WARNING) << LOG_DESC("[#commitBlock]Commit fail due to incorrect block number or incorrect parent hash")
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
    size_t totalTxCount = 0;
    getStorageGetter()->getCurrentState(SYS_KEY_TOTAL_TRANSACTION_COUNT, _tableFactory,
        [&](Error::Ptr _error, std::shared_ptr<std::string> _totalTxStr) {
            if (!_error || _error->errorCode() == CommonError::SUCCESS)
            {
                if(!_totalTxStr->empty()){
                    totalTxCount += block->transactionsSize();
                }
                totalTxCount += block->transactionsSize();
                auto ret = getStorageSetter()->setCurrentState(_tableFactory,
                    SYS_KEY_TOTAL_TRANSACTION_COUNT,
                    boost::lexical_cast<std::string>(totalTxCount));
                if(!ret){
                    LEDGER_LOG(DEBUG) << LOG_BADGE("WriteCurrentState")
                                      << LOG_DESC("Write SYS_KEY_TOTAL_TRANSACTION_COUNT error")
                                      << LOG_KV("blockNumber", block->blockHeader()->number());
                    return;
                }
            }
            else
            {
                LEDGER_LOG(ERROR) << LOG_BADGE("GetCurrentState")
                                  << LOG_DESC("Get SYS_KEY_TOTAL_TRANSACTION_COUNT error")
                                  << LOG_KV("blockNumber", block->blockHeader()->number());
                return;
            }
        });

    auto receipts = blockReceiptListGetter(block);
    size_t failedTransactions = 0;
    for (auto& receipt : *receipts)
    {
        // TODO: check receipt status
        if (receipt->status() != 0)
        {
            ++failedTransactions;
        }
    }

    getStorageGetter()->getCurrentState(SYS_KEY_TOTAL_FAILED_TRANSACTION, _tableFactory,
        [&](Error::Ptr _error, std::shared_ptr<std::string> _totalFailedTxsStr) {
            if (!_error || _error->errorCode() == CommonError::SUCCESS)
            {
                if(!_totalFailedTxsStr->empty()){
                    failedTransactions += boost::lexical_cast<size_t>(_totalFailedTxsStr);
                }
                auto ret = getStorageSetter()->setCurrentState(_tableFactory,
                    SYS_KEY_TOTAL_FAILED_TRANSACTION,
                    boost::lexical_cast<std::string>(failedTransactions));
                if(!ret){
                    LEDGER_LOG(DEBUG) << LOG_BADGE("WriteCurrentState")
                                      << LOG_DESC("Write SYS_KEY_TOTAL_FAILED_TRANSACTION error")
                                      << LOG_KV("blockNumber", block->blockHeader()->number());
                    return;
                }
            }
            else
            {
                LEDGER_LOG(ERROR) << LOG_BADGE("GetCurrentState")
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
    bool ret;
    tbb::parallel_for(tbb::blocked_range<size_t>(0, _block->transactionsHashSize()),
        [&](const tbb::blocked_range<size_t>& range) {
            for (size_t i = range.begin(); i < range.end() ; ++i)
            {
                auto encodeReceipt = _block->receipt(i)->encode();
                ret = getStorageSetter()->setHashToReceipt(
                    _tableFactory, _block->transactionHash(i).hex(), asString(encodeReceipt));
            }
        });

    if(!ret){
        LEDGER_LOG(DEBUG) << LOG_BADGE("writeHash2Receipt")
                          << LOG_DESC("Write row in SYS_HASH_2_RECEIPT error")
                          << LOG_KV("blockNumber", _block->blockHeader()->number());
    }
}
bool Ledger::buildGenesisBlock(LedgerConfig::Ptr _ledgerConfig)
{
    LEDGER_LOG(INFO) << LOG_DESC("[#buildGenesisBlock]");
    // TODO: to check NUMBER_2_HEADER table is created
    // TODO: creat tables
    getBlock(0, HEADER, [&](Error::Ptr, protocol::Block::Ptr) {

    });
    // FIXME: use getBlock
    Block::Ptr block = nullptr;
    // to build genesis block
    if(block == nullptr)
    {
        auto txLimit = _ledgerConfig->blockTxCountLimit();
        LEDGER_LOG(TRACE) << LOG_DESC("test") << LOG_KV("txLimit", txLimit);
        auto tableFactory = getState()->getStateCache(0);
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
            if (retPair.second->errorCode() == CommonError::SUCCESS && retPair.first > 0)
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
