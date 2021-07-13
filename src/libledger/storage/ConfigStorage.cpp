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
 * @file ConfigStorage.cpp
 * @author: kyonRay
 * @date 2021-07-09
 */

#include "ConfigStorage.h"
#include "bcos-ledger/libledger/storage/StorageUtilities.h"
#include "bcos-ledger/libledger/utilities/BlockUtilities.h"
#include "bcos-ledger/libledger/utilities/Common.h"
#include <bcos-framework/interfaces/protocol/CommonError.h>
#include <tbb/parallel_invoke.h>
#include <future>

using namespace bcos;
using namespace bcos::protocol;
using namespace bcos::storage;
using namespace bcos::consensus;

namespace bcos::ledger
{
void ConfigStorage::getCurrentState(std::string _row,
    const TableFactoryInterface::Ptr& _tableFactory,
    std::function<void(Error::Ptr, bcos::storage::Entry::Ptr)> _onGetEntry)
{
    StorageUtilities::asyncTableGetter(_tableFactory, SYS_CURRENT_STATE, _row, _onGetEntry);
}

void ConfigStorage::getSysConfig(std::string _key, const TableFactoryInterface::Ptr& _tableFactory,
    std::function<void(Error::Ptr, bcos::storage::Entry::Ptr)> _onGetEntry)
{
    StorageUtilities::asyncTableGetter(_tableFactory, SYS_CONFIG, _key, _onGetEntry);
}

void ConfigStorage::getSystemConfigList(const std::shared_ptr<std::vector<std::string>>& _keys,
    const storage::TableFactoryInterface::Ptr& _tableFactory, bool _allowEmpty,
    std::function<void(const Error::Ptr&, std::map<std::string, Entry::Ptr> const&)> _onGetConfig)
{
    // open table
    auto table = _tableFactory->openTable(SYS_CONFIG);
    if (!table)
    {
        auto error = std::make_shared<Error>(
            LedgerError::OpenTableFailed, "open table " + SYS_CONFIG + " failed.");
        std::map<std::string, bcos::storage::Entry::Ptr> emptyEntries;
        _onGetConfig(error, emptyEntries);
        return;
    }
    table->asyncGetRows(_keys, [_keys, _allowEmpty, _onGetConfig](const Error::Ptr& _error,
                                   std::map<std::string, Entry::Ptr> const& _entries) {
        std::map<std::string, Entry::Ptr> emptyEntryMap;
        if (_error)
        {
            LEDGER_LOG(ERROR) << LOG_DESC("asyncGetSystemConfigList failed")
                              << LOG_KV("code", _error->errorCode())
                              << LOG_KV("msg", _error->errorMessage());
            _onGetConfig(_error, emptyEntryMap);
            return;
        }
        if (_allowEmpty)
        {
            _onGetConfig(_error, _entries);
            return;
        }
        // check the result
        for (auto const& key : *_keys)
        {
            // Note: must make sure all the configs are not empty
            if (!_entries.count(key))
            {
                auto entry = _entries.at(key);
                if (entry)
                {
                    continue;
                }
                auto errorMsg =
                    "asyncGetSystemConfigList failed for get empty config for key: " + key;
                LEDGER_LOG(ERROR) << LOG_DESC(errorMsg) << LOG_KV("key", key);
                _onGetConfig(
                    std::make_shared<Error>(LedgerError::CallbackError, errorMsg), emptyEntryMap);
                return;
            }
        }
        _onGetConfig(_error, _entries);
    });
}

void ConfigStorage::getConsensusConfig(const std::string& _nodeType,
    const TableFactoryInterface::Ptr& _tableFactory,
    std::function<void(Error::Ptr, consensus::ConsensusNodeListPtr)> _onGetConfig)
{
    std::vector<std::string> nodeTypeList;
    nodeTypeList.emplace_back(_nodeType);
    getConsensusConfigList(nodeTypeList, _tableFactory,
        [_nodeType, _onGetConfig](
            Error::Ptr _error, std::map<std::string, consensus::ConsensusNodeListPtr> _nodeMap) {
            if (_error)
            {
                _onGetConfig(_error, nullptr);
                return;
            }
            if (_nodeMap.count(_nodeType))
            {
                _onGetConfig(_error, _nodeMap[_nodeType]);
                return;
            }
            _onGetConfig(_error, nullptr);
        });
}

void ConfigStorage::getConsensusConfigList(std::vector<std::string> const& _nodeTypeList,
    const TableFactoryInterface::Ptr& _tableFactory,
    std::function<void(Error::Ptr, std::map<std::string, consensus::ConsensusNodeListPtr>)>
        _onGetConfig)
{
    auto table = _tableFactory->openTable(SYS_CONSENSUS);
    if (!table)
    {
        LEDGER_LOG(DEBUG) << LOG_BADGE("getConsensusConfigList")
                          << LOG_DESC("Open table error from db")
                          << LOG_KV("tableName", SYS_CONSENSUS);
        auto error =
            std::make_shared<Error>(LedgerError::OpenTableFailed, "open SYS_CONSENSUS table error");
        _onGetConfig(error, {});
        return;
    }
    table->asyncGetPrimaryKeys(
        nullptr, [_onGetConfig, table, _nodeTypeList, this](
                     const Error::Ptr& _error, std::vector<std::string> _keys) {
            if (_error && _error->errorCode() != CommonError::SUCCESS)
            {
                _onGetConfig(_error, {});
                return;
            }
            auto keys = std::make_shared<std::vector<std::string>>(_keys);
            table->asyncGetRows(
                keys, [_nodeTypeList, _onGetConfig, this](const Error::Ptr& _error,
                          const std::map<std::string, Entry::Ptr>& _entryMap) {
                    if (_error && _error->errorCode() != CommonError::SUCCESS)
                    {
                        _onGetConfig(_error, {});
                        return;
                    }
                    std::map<std::string, consensus::ConsensusNodeListPtr> nodeMap;
                    for (auto const& type : _nodeTypeList)
                    {
                        auto node = std::make_shared<ConsensusNodeList>();
                        for (const auto& nodePair : _entryMap)
                        {
                            if (!nodePair.second)
                            {
                                continue;
                            }
                            auto nodeType = nodePair.second->getField(NODE_TYPE);
                            if (nodeType == type)
                            {
                                crypto::NodeIDPtr nodeID =
                                    m_keyFactory->createKey(*fromHexString(nodePair.first));
                                // Note: use try-catch to handle the exception case
                                auto weight = boost::lexical_cast<uint64_t>(
                                    nodePair.second->getField(NODE_WEIGHT));
                                node->emplace_back(std::make_shared<ConsensusNode>(nodeID, weight));
                            }
                        }
                        nodeMap[type] = node;
                    }
                    _onGetConfig(nullptr, nodeMap);
                });
        });
}

bool ConfigStorage::setCurrentState(const std::string& _row, const std::string& _stateValue,
    const TableFactoryInterface::Ptr& _tableFactory)
{
    return StorageUtilities::syncTableSetter(
        _tableFactory, SYS_CURRENT_STATE, _row, SYS_VALUE, _stateValue);
}
bool ConfigStorage::setSysConfig(const std::string& _key, const std::string& _value,
    const std::string& _enableBlock, const TableFactoryInterface::Ptr& _tableFactory)
{
    auto table = _tableFactory->openTable(SYS_CONFIG);

    if (table)
    {
        auto entry = table->newEntry();
        entry->setField(SYS_VALUE, _value);
        entry->setField(SYS_CONFIG_ENABLE_BLOCK_NUMBER, _enableBlock);
        auto ret = table->setRow(_key, entry);

        LEDGER_LOG(TRACE) << LOG_BADGE("Write data to DB") << LOG_KV("openTable", SYS_CONFIG);
        return ret;
    }
    else
    {
        BOOST_THROW_EXCEPTION(OpenSysTableFailed() << errinfo_comment(SYS_CONFIG));
    }
}
bool ConfigStorage::setConsensusConfig(const std::string& _type, const ConsensusNodeList& _nodeList,
    const std::string& _enableBlock, const TableFactoryInterface::Ptr& _tableFactory)
{
    auto table = _tableFactory->openTable(SYS_CONSENSUS);

    if (table)
    {
        bool ret = (!_nodeList.empty());
        for (const auto& node : _nodeList)
        {
            auto entry = table->newEntry();
            entry->setField(NODE_TYPE, _type);
            entry->setField(NODE_WEIGHT, boost::lexical_cast<std::string>(node->weight()));
            entry->setField(NODE_ENABLE_NUMBER, _enableBlock);
            ret = ret && table->setRow(node->nodeID()->hex(), entry);
        }

        LEDGER_LOG(TRACE) << LOG_BADGE("Write data to DB") << LOG_KV("openTable", SYS_CONSENSUS);
        return ret;
    }
    else
    {
        BOOST_THROW_EXCEPTION(OpenSysTableFailed() << errinfo_comment(SYS_CONSENSUS));
    }
}
void ConfigStorage::calTotalTransactionCount(const bcos::protocol::Block::Ptr& block,
    const bcos::storage::TableFactoryInterface::Ptr& _tableFactory)
{
    // empty block
    if (block->transactionsSize() == 0 && block->receiptsSize() == 0)
    {
        LEDGER_LOG(ERROR) << LOG_BADGE("writeTotalTransactionCount")
                          << LOG_DESC("Empty block, stop update total tx count")
                          << LOG_KV("blockNumber", block->blockHeader()->number());
        return;
    }
    getCurrentState(SYS_KEY_TOTAL_TRANSACTION_COUNT, _tableFactory,
        [block, _tableFactory, this](Error::Ptr _error, bcos::storage::Entry::Ptr _totalTxEntry) {
            if (_error && _error->errorCode() != CommonError::SUCCESS)
            {
                LEDGER_LOG(ERROR) << LOG_BADGE("writeTotalTransactionCount")
                                  << LOG_DESC("Get SYS_KEY_TOTAL_TRANSACTION_COUNT error")
                                  << LOG_KV("blockNumber", block->blockHeader()->number());
                return;
            }
            int64_t totalTxCount = 0;
            auto totalTxStr = _totalTxEntry->getField(SYS_VALUE);
            if (!totalTxStr.empty())
            {
                totalTxCount += boost::lexical_cast<int64_t>(totalTxStr);
            }
            totalTxCount += block->transactionsSize();
            setCurrentState(SYS_KEY_TOTAL_TRANSACTION_COUNT,
                boost::lexical_cast<std::string>(totalTxCount), _tableFactory);
        });
    getCurrentState(SYS_KEY_TOTAL_FAILED_TRANSACTION, _tableFactory,
        [_tableFactory, block, this](
            Error::Ptr _error, bcos::storage::Entry::Ptr _totalFailedTxsEntry) {
            if (_error && _error->errorCode() != CommonError::SUCCESS)
            {
                LEDGER_LOG(ERROR) << LOG_BADGE("writeTotalTransactionCount")
                                  << LOG_DESC("Get SYS_KEY_TOTAL_FAILED_TRANSACTION error")
                                  << LOG_KV("blockNumber", block->blockHeader()->number());
                return;
            }
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
            auto totalFailedTxsStr = _totalFailedTxsEntry->getField(SYS_VALUE);
            if (!totalFailedTxsStr.empty())
            {
                failedTransactions += boost::lexical_cast<int64_t>(totalFailedTxsStr);
            }
            setCurrentState(SYS_KEY_TOTAL_FAILED_TRANSACTION,
                boost::lexical_cast<std::string>(failedTransactions), _tableFactory);
        });
}

void ConfigStorage::setLedgerConfig(const LedgerConfig::Ptr& _ledgerConfig,
    const storage::TableFactoryInterface::Ptr& _tableFactory)
{
    tbb::parallel_invoke(
        [this, _ledgerConfig, _tableFactory]() {
            setSysConfig(SYSTEM_KEY_TX_COUNT_LIMIT,
                boost::lexical_cast<std::string>(_ledgerConfig->blockTxCountLimit()), "0",
                _tableFactory);
        },
        [this, _ledgerConfig, _tableFactory]() {
            setSysConfig(SYSTEM_KEY_CONSENSUS_LEADER_PERIOD,
                boost::lexical_cast<std::string>(_ledgerConfig->leaderSwitchPeriod()), "0",
                _tableFactory);
        },
        [this, _ledgerConfig, _tableFactory]() {
            setSysConfig(SYSTEM_KEY_CONSENSUS_TIMEOUT,
                boost::lexical_cast<std::string>(_ledgerConfig->consensusTimeout()), "0",
                _tableFactory);
        },
        [this, _ledgerConfig, _tableFactory]() {
            setConsensusConfig(
                CONSENSUS_SEALER, _ledgerConfig->consensusNodeList(), "0", _tableFactory);
        },
        [this, _ledgerConfig, _tableFactory]() {
            setConsensusConfig(
                CONSENSUS_OBSERVER, _ledgerConfig->observerNodeList(), "0", _tableFactory);
        });
    LEDGER_LOG(INFO) << LOG_BADGE("setLedgerConfig") << LOG_DESC("set all ledger config data");
}

LedgerConfig::Ptr ConfigStorage::getLedgerConfig(protocol::BlockNumber _number,
    const crypto::HashType& _hash, const storage::TableFactoryInterface::Ptr& _tableFactory)
{
    auto ledgerConfig = std::make_shared<LedgerConfig>();
    ledgerConfig->setBlockNumber(_number);
    ledgerConfig->setHash(_hash);

    auto timeoutPromise = std::make_shared<std::promise<std::string>>();
    auto countLimitPromise = std::make_shared<std::promise<std::string>>();
    auto sealerPromise = std::make_shared<std::promise<consensus::ConsensusNodeListPtr>>();
    auto observerPromise = std::make_shared<std::promise<consensus::ConsensusNodeListPtr>>();
    auto switchPeriodPromise = std::make_shared<std::promise<std::string>>();

    auto timeoutFuture = timeoutPromise->get_future();
    auto countLimitFuture = countLimitPromise->get_future();
    auto sealerFuture = sealerPromise->get_future();
    auto observerFuture = observerPromise->get_future();
    auto switchPeriodFuture = switchPeriodPromise->get_future();

    getSysConfig(SYSTEM_KEY_CONSENSUS_TIMEOUT, _tableFactory,
        [timeoutPromise](Error::Ptr _error, bcos::storage::Entry::Ptr _configEntry) {
            if ((_error && _error->errorCode() != CommonError::SUCCESS) || !_configEntry)
            {
                LEDGER_LOG(ERROR) << LOG_BADGE("getLedgerConfig")
                                  << LOG_DESC("getSysConfig callback error")
                                  << LOG_KV("key", SYSTEM_KEY_CONSENSUS_TIMEOUT);
                timeoutPromise->set_value("");
                return;
            }
            auto value = _configEntry->getField(SYS_VALUE);
            timeoutPromise->set_value(value);
            LEDGER_LOG(TRACE) << LOG_BADGE("getLedgerConfig") << LOG_DESC("get config in db")
                              << LOG_KV("key", SYSTEM_KEY_CONSENSUS_TIMEOUT)
                              << LOG_KV("value", value);
        });
    getSysConfig(SYSTEM_KEY_TX_COUNT_LIMIT, _tableFactory,
        [countLimitPromise](Error::Ptr _error, bcos::storage::Entry::Ptr _configEntry) {
            if ((_error && _error->errorCode() != CommonError::SUCCESS) || !_configEntry)
            {
                LEDGER_LOG(ERROR) << LOG_BADGE("getLedgerConfig")
                                  << LOG_DESC("getSysConfig callback error")
                                  << LOG_KV("key", SYSTEM_KEY_TX_COUNT_LIMIT);
                countLimitPromise->set_value("");
                return;
            }
            auto value = _configEntry->getField(SYS_VALUE);
            countLimitPromise->set_value(value);
            LEDGER_LOG(TRACE) << LOG_BADGE("getLedgerConfig") << LOG_DESC("get config in db")
                              << LOG_KV("key", SYSTEM_KEY_TX_COUNT_LIMIT) << LOG_KV("value", value);
        });
    getSysConfig(SYSTEM_KEY_CONSENSUS_LEADER_PERIOD, _tableFactory,
        [switchPeriodPromise](Error::Ptr _error, bcos::storage::Entry::Ptr _configEntry) {
            if ((_error && _error->errorCode() != CommonError::SUCCESS) || !_configEntry)
            {
                LEDGER_LOG(ERROR) << LOG_BADGE("getLedgerConfig")
                                  << LOG_DESC("getSysConfig callback error")
                                  << LOG_KV("key", SYSTEM_KEY_CONSENSUS_LEADER_PERIOD);
                switchPeriodPromise->set_value("");
                return;
            }
            auto value = _configEntry->getField(SYS_VALUE);
            switchPeriodPromise->set_value(value);
            LEDGER_LOG(TRACE) << LOG_BADGE("getLedgerConfig") << LOG_DESC("get config in db")
                              << LOG_KV("key", SYSTEM_KEY_CONSENSUS_LEADER_PERIOD)
                              << LOG_KV("value", value);
        });
    getConsensusConfig(CONSENSUS_SEALER, _tableFactory,
        [sealerPromise](Error::Ptr _error, ConsensusNodeListPtr _nodeList) {
            if (_error && _error->errorCode() != CommonError::SUCCESS)
            {
                LEDGER_LOG(ERROR) << LOG_BADGE("getLedgerConfig")
                                  << LOG_DESC("getConsensusConfig callback error")
                                  << LOG_KV("getKey", CONSENSUS_SEALER)
                                  << LOG_KV("errorCode", _error->errorCode())
                                  << LOG_KV("errorMsg", _error->errorMessage());
                sealerPromise->set_value(nullptr);
                return;
            }
            sealerPromise->set_value(_nodeList);
        });

    getConsensusConfig(CONSENSUS_OBSERVER, _tableFactory,
        [observerPromise](Error::Ptr _error, ConsensusNodeListPtr _nodeList) {
            if (_error && _error->errorCode() != CommonError::SUCCESS)
            {
                LEDGER_LOG(ERROR) << LOG_BADGE("getLedgerConfig")
                                  << LOG_DESC("asyncGetNodeListByType callback error")
                                  << LOG_KV("getKey", CONSENSUS_OBSERVER)
                                  << LOG_KV("errorCode", _error->errorCode())
                                  << LOG_KV("errorMsg", _error->errorMessage());
                observerPromise->set_value(nullptr);
                return;
            }
            observerPromise->set_value(_nodeList);
        });
    auto consensusTimeout = timeoutFuture.get();
    auto txLimit = countLimitFuture.get();
    auto sealerList = sealerFuture.get();
    auto observerList = observerFuture.get();
    auto switchPeriod = switchPeriodFuture.get();

    if (consensusTimeout.empty() || txLimit.empty() || switchPeriod.empty() || !sealerList ||
        !observerList)
    {
        LEDGER_LOG(ERROR) << LOG_BADGE("getLedgerConfig")
                          << LOG_DESC("Get ledgerConfig from db error");
        return nullptr;
    }
    ledgerConfig->setConsensusTimeout(boost::lexical_cast<uint64_t>(consensusTimeout));
    ledgerConfig->setBlockTxCountLimit(boost::lexical_cast<uint64_t>(txLimit));
    ledgerConfig->setLeaderSwitchPeriod(boost::lexical_cast<uint64_t>(switchPeriod));
    ledgerConfig->setConsensusNodeList(*sealerList);
    ledgerConfig->setObserverNodeList(*observerList);
    return ledgerConfig;
}
}  // namespace bcos::ledger
