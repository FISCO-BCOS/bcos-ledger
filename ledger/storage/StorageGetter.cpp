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
 * @file StorageGetter.cpp
 * @author: kyonRay
 * @date 2021-04-23
 */

#include "StorageGetter.h"
#include "../utilities/Common.h"
#include "../utilities/BlockUtilities.h"
#include <bcos-framework/interfaces/protocol/CommonError.h>

using namespace bcos;
using namespace bcos::protocol;
using namespace bcos::storage;
using namespace bcos::consensus;

namespace bcos::ledger
{
bool StorageGetter::checkTableExist(
    std::string _tableName, const bcos::storage::TableFactoryInterface::Ptr& _tableFactory)
{
    auto table = _tableFactory->openTable(_tableName);
    return table != nullptr;
}

void StorageGetter::getTxsFromStorage(
    const BlockNumber& _blockNumber, const TableFactoryInterface::Ptr& _tableFactory,
    std::function<void(Error::Ptr, std::shared_ptr<std::string>)> _onGetString)
{
    asyncTableGetter(_tableFactory, SYS_NUMBER_2_TXS,
        boost::lexical_cast<std::string>(_blockNumber), SYS_VALUE, _onGetString);
}

void StorageGetter::getBlockHeaderFromStorage(
    const bcos::protocol::BlockNumber& _blockNumber,
    const bcos::storage::TableFactoryInterface::Ptr& _tableFactory,
    std::function<void(Error::Ptr, std::shared_ptr<std::string>)> _onGetString)
{
    asyncTableGetter(_tableFactory, SYS_NUMBER_2_BLOCK_HEADER,
        boost::lexical_cast<std::string>(_blockNumber), SYS_VALUE, _onGetString);
}

void StorageGetter::getNoncesFromStorage(const BlockNumber& _blockNumber,
    const TableFactoryInterface::Ptr& _tableFactory,
    std::function<void(Error::Ptr, std::shared_ptr<std::string>)> _onGetString)
{
    asyncTableGetter(_tableFactory, SYS_BLOCK_NUMBER_2_NONCES,
        boost::lexical_cast<std::string>(_blockNumber), SYS_VALUE, _onGetString);
}

void StorageGetter::getNoncesBatchFromStorage(const bcos::protocol::BlockNumber& _startNumber,
    const protocol::BlockNumber& _endNumber,
    const bcos::storage::TableFactoryInterface::Ptr& _tableFactory,
    const bcos::protocol::BlockFactory::Ptr& _blockFactory,
    std::function<void(
        Error::Ptr, std::shared_ptr<std::map<protocol::BlockNumber, protocol::NonceListPtr>>)>
        _onGetData)
{
    auto retMap = std::make_shared<std::map<protocol::BlockNumber, protocol::NonceListPtr>>();

    auto start_time = utcTime();
    auto record_time = utcTime();

    auto table = _tableFactory->openTable(SYS_BLOCK_NUMBER_2_NONCES);
    auto openTable_time_cost = utcTime() - record_time;
    record_time = utcTime();

    if (table)
    {
        auto numberList = std::make_shared<std::vector<std::string>>();
        for (BlockNumber i = _startNumber; i <= _endNumber; ++i)
        {
            numberList->emplace_back(boost::lexical_cast<std::string>(i));
        }

        table->asyncGetRows(numberList,
            [&](const Error::Ptr& _error, const std::map<std::string, Entry::Ptr>& numberEntryMap) {
                if (!_error || _error->errorCode() == CommonError::SUCCESS)
                {
                    auto select_time_cost = utcTime() - record_time;
                    record_time = utcTime();
                    if (numberEntryMap.size() != size_t(_endNumber - _startNumber + 1))
                    {
                        LEDGER_LOG(DEBUG) << LOG_DESC(
                            "getRows SYS_BLOCK_NUMBER_2_NONCES table error from db, wrong size of "
                            "map returned");
                        // TODO: add error code and msg
                        auto error = std::make_shared<Error>(-1, "");
                        _onGetData(error, retMap);
                        return;
                    }
                    for (const auto& number : *numberList)
                    {
                        auto nonceStr = numberEntryMap.at(number)->getField(SYS_VALUE);
                        auto block = _blockFactory->createBlock();
                        block->decode(nonceStr, false, false);
                        auto nonceList = std::make_shared<protocol::NonceList>(block->nonceList());
                        retMap->emplace(std::make_pair(boost::lexical_cast<BlockNumber>(number), nonceList));
                    }
                    auto get_field_time_cost = utcTime() - record_time;
                    LEDGER_LOG(DEBUG) << LOG_DESC("Get Nonce list from db")
                                      << LOG_KV("openTableTimeCost", openTable_time_cost)
                                      << LOG_KV("selectTimeCost", select_time_cost)
                                      << LOG_KV("getFieldTimeCost", get_field_time_cost)
                                      << LOG_KV("totalTimeCost", utcTime() - start_time);
                    _onGetData(nullptr, retMap);
                }
                else{
                    // TODO: add error code and msg
                    auto error =
                        std::make_shared<Error>(_error->errorCode(), "" + _error->errorMessage());
                    _onGetData(error, retMap);
                }
            });
    }
    else
    {
        LEDGER_LOG(DEBUG) << LOG_DESC("Open SYS_BLOCK_NUMBER_2_NONCES table error from db");
        // TODO: add error code and msg
        auto error =
            std::make_shared<Error>(-1, "");
        _onGetData(error, nullptr);
    }
}

void StorageGetter::getBlockNumberByHash(const std::string& _hash,
    const bcos::storage::TableFactoryInterface::Ptr& _tableFactory,
    std::function<void(Error::Ptr, std::shared_ptr<std::string>)> _onGetString)
{
    asyncTableGetter(_tableFactory, SYS_HASH_2_NUMBER, _hash, SYS_VALUE, _onGetString);
}

void StorageGetter::getBlockHashByNumber(const BlockNumber& _num,
    const bcos::storage::TableFactoryInterface::Ptr& _tableFactory,
    std::function<void(Error::Ptr, std::shared_ptr<std::string>)> _onGetString)
{
    asyncTableGetter(_tableFactory, SYS_NUMBER_2_HASH, boost::lexical_cast<std::string>(_num),
        SYS_VALUE, _onGetString);
}

void StorageGetter::getCurrentState(const std::string& _row,
    const storage::TableFactoryInterface::Ptr& _tableFactory,
    std::function<void(Error::Ptr, std::shared_ptr<std::string>)> _onGetString)
{
    return asyncTableGetter(_tableFactory, SYS_CURRENT_STATE, _row, SYS_VALUE, _onGetString);
}

void StorageGetter::getSysConfig(const std::string& _key, const TableFactoryInterface::Ptr& _tableFactory,
    std::function<void(Error::Ptr, std::shared_ptr<stringsPair>)> _onGetConfig)
{
    auto ret = std::make_shared<stringsPair>(std::make_pair("", ""));
    auto start_time = utcTime();
    auto record_time = utcTime();

    auto table = _tableFactory->openTable(SYS_CONFIG);
    auto openTable_time_cost = utcTime() - record_time;
    record_time = utcTime();
    if (table)
    {
        table->asyncGetRow(_key, [&](const Error::Ptr& _error, std::shared_ptr<Entry> _entry) {
            if(!_error || _error->errorCode() == CommonError::SUCCESS)
            {
                auto select_time_cost = utcTime() - record_time;
                if (_entry)
                {
                    auto value = _entry->getField(SYS_VALUE);
                    auto number = _entry->getField(SYS_CONFIG_ENABLE_BLOCK_NUMBER);
                    auto get_field_time_cost = utcTime() - record_time;

                    ret->first.swap(value);
                    ret->second.swap(number);

                    LEDGER_LOG(DEBUG)
                        << LOG_DESC("Get config from db") << LOG_KV("openTable", SYS_CONFIG)
                        << LOG_KV("openTableTimeCost", openTable_time_cost)
                        << LOG_KV("selectTimeCost", select_time_cost)
                        << LOG_KV("getFieldTimeCost", get_field_time_cost)
                        << LOG_KV("totalTimeCost", utcTime() - start_time);
                    _onGetConfig(nullptr, ret);
                }
                else
                {
                    // TODO: add error msg
                    auto error =
                        std::make_shared<Error>(-1, "");
                    _onGetConfig(error, ret);
                }
            }
            else
            {
                // TODO: add error msg
                auto error =
                    std::make_shared<Error>(_error->errorCode(), "" + _error->errorMessage());
                _onGetConfig(error, ret);
            }
        });
    }
    else
    {
        LEDGER_LOG(DEBUG) << LOG_DESC("Open SYS_CONFIG table error from db");
        // TODO: add error msg
        auto error = std::make_shared<Error>(-1, "");
        _onGetConfig(error, nullptr);
    }
}

void StorageGetter::getConsensusConfig(const std::string& _nodeType,
    const BlockNumber& _blockNumber, const TableFactoryInterface::Ptr& _tableFactory,
    const crypto::KeyFactory::Ptr& _keyFactory,
    std::function<void(Error::Ptr, consensus::ConsensusNodeListPtr)> _onGetConfig)
{
    ConsensusNodeListPtr nodeList = std::make_shared<ConsensusNodeList>();
    auto start_time = utcTime();
    auto record_time = utcTime();

    auto table = _tableFactory->openTable(SYS_CONSENSUS);
    auto openTable_time_cost = utcTime() - record_time;

    if (table)
    {
        table->asyncGetPrimaryKeys(nullptr, [_onGetConfig, &table, _nodeType, _blockNumber, &_keyFactory, nodeList](const Error::Ptr& _error, std::vector<std::string> _keys) {
            if (!_error || _error->errorCode() == CommonError::SUCCESS)
            {
                if(!_keys.empty())
                {
                    auto keys = std::make_shared<std::vector<std::string>>(_keys);
                    table->asyncGetRows(
                        keys, [_nodeType, _blockNumber, &_keyFactory, _onGetConfig, nodeList](
                                  const Error::Ptr& _error,
                                  const std::map<std::string, Entry::Ptr>& _entryMap) {
                            if (!_error || _error->errorCode() == CommonError::SUCCESS)
                            {
                                for (const auto& nodePair : _entryMap)
                                {
                                    auto nodeType = nodePair.second->getField(NODE_TYPE);
                                    auto blockNum = boost::lexical_cast<BlockNumber>(
                                        nodePair.second->getField(NODE_ENABLE_NUMBER));
                                    if (nodeType == _nodeType && blockNum <= _blockNumber)
                                    {
                                        crypto::NodeIDPtr nodeID =
                                            _keyFactory->createKey(*fromHexString(nodePair.first));
                                        auto weight = boost::lexical_cast<uint64_t>(
                                            nodePair.second->getField(NODE_WEIGHT));
                                        auto node = std::make_shared<ConsensusNode>(nodeID, weight);
                                        nodeList->emplace_back(node);
                                    }
                                }
                                _onGetConfig(nullptr, nodeList);
                            }
                            else
                            {
                                auto error = std::make_shared<Error>(_error->errorCode(),
                                    "asyncGetRows callback error" + _error->errorMessage());
                                _onGetConfig(error, nodeList);
                            }
                        });
                }
                else
                {
                    auto error = std::make_shared<Error>(-1, "get primary keys empty");
                    _onGetConfig(error, nodeList);
                }
            }
            else
            {
                auto error = std::make_shared<Error>(_error->errorCode(),
                    "asyncGetPrimaryKeys callback error" + _error->errorMessage());
                _onGetConfig(error, nodeList);
            }
        });
        LEDGER_LOG(DEBUG)
                << LOG_DESC("Get ConsensusConfig from db")
                << LOG_KV("openTableTimeCost", openTable_time_cost)
                << LOG_KV("totalTimeCost", utcTime() - start_time);
    }
    else
    {
        LEDGER_LOG(DEBUG) << LOG_DESC("Open SYS_CONSENSUS table error from db");
        auto error = std::make_shared<Error>(-1, "open SYS_CONSENSUS table error");
        _onGetConfig(error, nullptr);
    }
}

void StorageGetter::asyncTableGetter(
    const bcos::storage::TableFactoryInterface::Ptr& _tableFactory, const std::string& _tableName,
    const std::string& _row, const std::string& _field,
    std::function<void(Error::Ptr, std::shared_ptr<std::string>)> _onGetString)
{
    auto start_time = utcTime();
    auto record_time = utcTime();

    auto table = _tableFactory->openTable(_tableName);

    auto openTable_time_cost = utcTime() - record_time;

    if (table)
    {
        table->asyncGetRow(_row, [_onGetString, _field](const Error::Ptr& _error, Entry::Ptr _entry) {
            if (!_error || _error->errorCode() == CommonError::SUCCESS)
            {
                if (_entry)
                {
                    auto ret = std::make_shared<std::string>(_entry->getField(_field));
                    _onGetString(nullptr, ret);
                }
                else
                {
                    // TODO: add error code
                    auto error = std::make_shared<Error>(-1, "asyncGetRow callback null entry");
                    _onGetString(error, nullptr);
                }
            }
            else
            {
                auto error = std::make_shared<Error>(
                    _error->errorCode(), "asyncGetRow callback error" + _error->errorMessage());
                _onGetString(error, nullptr);
            }
        });
        LEDGER_LOG(TRACE) << LOG_DESC("Get string from db") << LOG_KV("openTable", _tableName)
                          << LOG_KV("openTableTimeCost", openTable_time_cost)
                          << LOG_KV("totalTimeCost", utcTime() - start_time);
    }
    else
    {
        LEDGER_LOG(DEBUG) << LOG_DESC("Open table error from db")
                          << LOG_KV("openTable", _tableName);
        // TODO: add error code and msg
        auto error = std::make_shared<Error>(-1, "");
        _onGetString(error, nullptr);
    }
}

void StorageGetter::getBatchTxByHashList(
    const std::shared_ptr<std::vector<std::string>>& _hashList,
    const bcos::storage::TableFactoryInterface::Ptr& _tableFactory,
    const bcos::protocol::TransactionFactory::Ptr& _txFactory,
    std::function<void(Error::Ptr, protocol::TransactionsPtr)> _onGetTx)
{
    auto txList = std::make_shared<Transactions>();
    auto start_time = utcTime();
    auto record_time = utcTime();

    auto table = _tableFactory->openTable(SYS_HASH_2_TX);
    auto openTable_time_cost = utcTime() - record_time;

    if (table)
    {
        table->asyncGetRows(_hashList,
            [&_txFactory, _onGetTx, _hashList, txList](
                const Error::Ptr& _error, const std::map<std::string, Entry::Ptr>& _hashEntryMap) {
                if (!_error || _error->errorCode() == CommonError::SUCCESS)
                {
                    if (_hashEntryMap.size() != _hashList->size())
                    {
                        LEDGER_LOG(DEBUG) << LOG_DESC("getRows SYS_HASH_2_TX table error from db");
                        auto error = std::make_shared<Error>(-1, "asyncGetRows callback null map");
                        _onGetTx(error, nullptr);
                        return;
                    }
                    for (const auto& hash : *_hashList)
                    {
                        auto txStr = _hashEntryMap.at(hash)->getField(SYS_VALUE);
                        auto tx = decodeTransaction(_txFactory, txStr);
                        txList->emplace_back(tx);
                    }
                    _onGetTx(nullptr, txList);
                }
                else
                {
                    auto error = std::make_shared<Error>(_error->errorCode(),
                        "asyncGetRows callback error" + _error->errorMessage());
                    _onGetTx(error, nullptr);
                }
            });
        LEDGER_LOG(DEBUG) << LOG_DESC("Get txs list from db")
                          << LOG_KV("openTableTimeCost", openTable_time_cost)
                          << LOG_KV("totalTimeCost", utcTime() - start_time);
    }
    else
    {
        LEDGER_LOG(DEBUG) << LOG_DESC("Open SYS_HASH_2_TX table error from db");
        auto error = std::make_shared<Error>(-1, "open table SYS_HASH_2_TX error");
        _onGetTx(error, nullptr);
    }
}

void StorageGetter::getReceiptByTxHash(const std::string& _txHash,
    const bcos::storage::TableFactoryInterface::Ptr& _tableFactory,
    std::function<void(Error::Ptr, std::shared_ptr<std::string>)> _onGetString)
{
    asyncTableGetter(_tableFactory, SYS_HASH_2_RECEIPT, _txHash, SYS_VALUE, _onGetString);
}

void StorageGetter::getBatchReceiptsByHashList(
    std::shared_ptr<std::vector<std::string>> _hashList,
    const bcos::storage::TableFactoryInterface::Ptr& _tableFactory,
    const bcos::protocol::TransactionReceiptFactory::Ptr& _receiptFactory,
    std::function<void(Error::Ptr, protocol::ReceiptsPtr)> _onGetReceipt)
{
    auto receiptList = std::make_shared<Receipts>();
    auto start_time = utcTime();
    auto record_time = utcTime();

    auto table = _tableFactory->openTable(SYS_HASH_2_RECEIPT);
    auto openTable_time_cost = utcTime() - record_time;

    if (table)
    {
        table->asyncGetRows(_hashList,
            [_onGetReceipt, _receiptFactory, _hashList, receiptList](
                const Error::Ptr& _error, const std::map<std::string, Entry::Ptr>& _hashEntryMap) {
                if (!_error || _error->errorCode() == CommonError::SUCCESS)
                {
                    if (_hashEntryMap.size() != _hashList->size())
                    {
                        LEDGER_LOG(DEBUG)
                            << LOG_DESC("getRows SYS_HASH_2_RECEIPT table error from db");
                        // TODO: add error code
                        auto error = std::make_shared<Error>(-1, "asyncGetRows callback null map");
                        _onGetReceipt(error, nullptr);
                        return;
                    }
                    for (const auto& hash : *_hashList)
                    {
                        auto receiptStr = _hashEntryMap.at(hash)->getField(SYS_VALUE);
                        auto receipt = decodeReceipt(_receiptFactory, receiptStr);
                        receiptList->emplace_back(receipt);
                    }
                    _onGetReceipt(nullptr, receiptList);
                }
                else
                {
                    auto error =
                        std::make_shared<Error>(_error->errorCode(), "" + _error->errorMessage());
                    _onGetReceipt(error, nullptr);
                }
            });

        LEDGER_LOG(DEBUG) << LOG_DESC("Get receipt list from db")
                          << LOG_KV("openTableTimeCost", openTable_time_cost)
                          << LOG_KV("totalTimeCost", utcTime() - start_time);

    }
    else
    {
        LEDGER_LOG(DEBUG) << LOG_DESC("Open SYS_HASH_2_RECEIPT table error from db");
        // TODO: add error code
        auto error = std::make_shared<Error>(-1, "open table SYS_HASH_2_RECEIPT error");
        _onGetReceipt(error, nullptr);
    }
}

} // namespace bcos::ledger