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

using namespace bcos;
using namespace bcos::protocol;
using namespace bcos::storage;
using namespace bcos::consensus;

namespace bcos::ledger
{
std::string StorageGetter::getTxsFromStorage(
    const BlockNumber& _blockNumber, const TableFactoryInterface::Ptr& _tableFactory)
{
    auto txsStr = getterByBlockNumber(_blockNumber, _tableFactory, SYS_NUMBER_2_TXS);
    if (txsStr.empty())
    {
        LEDGER_LOG(DEBUG) << LOG_DESC("[#getTxsFromStorage] Cannot get txs, return empty.");
    }
    return txsStr;
}
std::string StorageGetter::getReceiptsFromStorage(const bcos::protocol::BlockNumber& _blockNumber,
    const bcos::storage::TableFactoryInterface::Ptr& _tableFactory)
{
    auto rcptStr = getterByBlockNumber(_blockNumber, _tableFactory, SYS_NUMBER_2_RECEIPTS);
    if (rcptStr.empty())
    {
        LEDGER_LOG(DEBUG) << LOG_DESC(
            "[#getReceiptFromStorage] Cannot get receipts, return empty.");
    }
    return rcptStr;
}
std::string StorageGetter::getBlockHeaderFromStorage(
    const bcos::protocol::BlockNumber& _blockNumber,
    const bcos::storage::TableFactoryInterface::Ptr& _tableFactory)
{
    auto headerStr = getterByBlockNumber(_blockNumber, _tableFactory, SYS_NUMBER_2_BLOCK_HEADER);
    if (headerStr.empty())
    {
        LEDGER_LOG(DEBUG) << LOG_DESC(
            "[#getBlockHeaderFromStorage] Cannot get header, return empty.");
    }
    return headerStr;
}
std::string StorageGetter::getFullBlockFromStorage(
    const BlockNumber& _blockNumber, const TableFactoryInterface::Ptr& _tableFactory)
{
    auto blockStr = getterByBlockNumber(_blockNumber,_tableFactory,SYS_NUMBER_2_BLOCK);
    if (blockStr.empty())
    {
        LEDGER_LOG(DEBUG) << LOG_DESC(
                    "[#getFullBlockFromStorage] Cannot get block, return empty.");
    }
    return blockStr;
}
std::string StorageGetter::getNoncesFromStorage(
    const BlockNumber& _blockNumber, const TableFactoryInterface::Ptr& _tableFactory)
{
    auto noncesStr = getterByBlockNumber(_blockNumber, _tableFactory, SYS_BLOCK_NUMBER_2_NONCES);
    if (noncesStr.empty())
    {
        LEDGER_LOG(DEBUG) << LOG_DESC("[#getNoncesFromStorage] Cannot get nonces, return empty.");
    }
    return noncesStr;
}

std::shared_ptr<std::map<protocol::BlockNumber, protocol::NonceListPtr>>
StorageGetter::getNoncesBatchFromStorage(const bcos::protocol::BlockNumber& _startNumber,
    const protocol::BlockNumber& _endNumber,
    const bcos::storage::TableFactoryInterface::Ptr& _tableFactory,
    const bcos::protocol::BlockFactory::Ptr& _blockFactory)
{
    auto retMap = std::make_shared<std::map<protocol::BlockNumber, protocol::NonceListPtr>>();

    auto start_time = utcTime();
    auto record_time = utcTime();

    auto table = _tableFactory->openTable(SYS_BLOCK_NUMBER_2_NONCES);
    auto openTable_time_cost = utcTime() - record_time;
    record_time = utcTime();

    if (table)
    {
        std::vector<std::string> numberList;
        for (BlockNumber i = _startNumber; i <= _endNumber; ++i)
        {
            numberList.emplace_back(boost::lexical_cast<std::string>(i));
        }

        auto numberEntryMap = table->getRows(numberList);
        auto select_time_cost = utcTime() - record_time;
        record_time = utcTime();

        if (numberEntryMap.size() != (size_t)_endNumber - _startNumber + 1)
        {
            LEDGER_LOG(DEBUG) << LOG_DESC("getRows SYS_BLOCK_NUMBER_2_NONCES table error from db");
            return retMap;
        }

        for (const auto& number : numberList)
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
    }
    else
    {
        LEDGER_LOG(DEBUG) << LOG_DESC("Open SYS_BLOCK_NUMBER_2_NONCES table error from db");
        return retMap;
    }
    return retMap;
}

std::string StorageGetter::getterByBlockNumber(const BlockNumber& _blockNumber, const bcos::storage::TableFactoryInterface::Ptr& _tableFactory, const std::string& _tableName)
{
    auto numberStr = boost::lexical_cast<std::string>(_blockNumber);
    return tableGetterByRowAndField(_tableFactory, _tableName, numberStr, SYS_VALUE);
}

std::string StorageGetter::getterByBlockHash(const std::string& _blockHash,
    const bcos::storage::TableFactoryInterface::Ptr& _tableFactory, const std::string& _tableName)
{
    return tableGetterByRowAndField(_tableFactory, _tableName, _blockHash, SYS_VALUE);
}

std::string StorageGetter::getBlockNumberByHash(
    const std::string& _hash, const bcos::storage::TableFactoryInterface::Ptr& _tableFactory)
{
    auto numberStr = getterByBlockHash(_hash, _tableFactory, SYS_HASH_2_NUMBER);
    if (numberStr.empty())
    {
        LEDGER_LOG(DEBUG) << LOG_DESC("[#getBlockNumberByHash] Cannot get numberStr, return empty.");
    }
    return numberStr;
}

std::string StorageGetter::getBlockHashByNumber(
    const BlockNumber& _num, const bcos::storage::TableFactoryInterface::Ptr& _tableFactory)
{
    auto hashStr = getterByBlockNumber(_num, _tableFactory, SYS_NUMBER_2_HASH);
    if (hashStr.empty())
    {
        LEDGER_LOG(DEBUG) << LOG_DESC("[#getBlockHashByNumber] Cannot get hashStr, return empty.");
    }
    return hashStr;
}

std::string StorageGetter::getCurrentState(
    const std::string& _row, const bcos::storage::TableFactoryInterface::Ptr& _tableFactory)
{
    return tableGetterByRowAndField(_tableFactory, SYS_CURRENT_STATE, _row, SYS_VALUE);
}

std::shared_ptr<stringsPair> StorageGetter::getBlockNumberAndIndexByHash(
    const std::string& _hash, const bcos::storage::TableFactoryInterface::Ptr& _tableFactory)
{
    return stringsPairGetterByRowAndFields(
        _tableFactory, SYS_TX_HASH_2_BLOCK_NUMBER, _hash, SYS_VALUE, "index");
}
std::shared_ptr<stringsPair> StorageGetter::getSysConfig(
    const std::string& _key, const bcos::storage::TableFactoryInterface::Ptr& _tableFactory)
{
    return stringsPairGetterByRowAndFields(_tableFactory, SYS_CONFIG, _key, SYS_VALUE, SYS_CONFIG_ENABLE_BLOCK_NUMBER);
}

ConsensusNodeListPtr StorageGetter::getConsensusConfig(const std::string& _nodeType,
    const BlockNumber& _blockNumber, const TableFactoryInterface::Ptr& _tableFactory,
    const crypto::KeyFactory::Ptr& _keyFactory)
{
    ConsensusNodeListPtr nodeList = std::make_shared<ConsensusNodeList>();
    auto start_time = utcTime();
    auto record_time = utcTime();

    auto table = _tableFactory->openTable(SYS_CONSENSUS);
    auto openTable_time_cost = utcTime() - record_time;
    record_time = utcTime();

    if (table)
    {
        auto nodeIdList = table->getPrimaryKeys(nullptr);
        auto select_time_cost = utcTime() - record_time;
        record_time = utcTime();

        auto nodeMap = table->getRows(nodeIdList);
        for (const auto& nodePair : nodeMap)
        {
            auto nodeType = nodePair.second->getField(NODE_TYPE);
            auto blockNum = boost::lexical_cast<BlockNumber>(nodePair.second->getField(NODE_ENABLE_NUMBER));
            if (nodeType == _nodeType && blockNum <= _blockNumber)
            {
                crypto::NodeIDPtr nodeID = _keyFactory->createKey(*fromHexString(nodePair.first));
                auto weight = boost::lexical_cast<uint64_t>(nodePair.second->getField(NODE_WEIGHT));
                auto node = std::make_shared<ConsensusNode>(nodeID, weight);
                nodeList->emplace_back(node);
            }
        }
        auto get_field_time_cost = utcTime() - record_time;
        LEDGER_LOG(DEBUG) << LOG_DESC("Get ConsensusConfig from db")
                          << LOG_KV("openTableTimeCost", openTable_time_cost)
                          << LOG_KV("selectTimeCost", select_time_cost)
                          << LOG_KV("getFieldTimeCost", get_field_time_cost)
                          << LOG_KV("totalTimeCost", utcTime() - start_time);
    }
    else
    {
        LEDGER_LOG(DEBUG) << LOG_DESC("Open SYS_CONSENSUS table error from db");
        return nullptr;
    }
    return nodeList;
}

std::string StorageGetter::tableGetterByRowAndField(const bcos::storage::TableFactoryInterface::Ptr& _tableFactory, const std::string& _tableName, const std::string& _row, const std::string& _field)
{
    std::string ret;
    auto start_time = utcTime();
    auto record_time = utcTime();

    auto table = _tableFactory->openTable(_tableName);

    auto openTable_time_cost = utcTime() - record_time;
    record_time = utcTime();

    if(table){
        auto entry = table->getRow(_row);
        auto select_time_cost = utcTime() - record_time;
        record_time = utcTime();

        if(entry){
            ret = entry->getField(_field);
            auto get_field_time_cost = utcTime() - record_time;

            LEDGER_LOG(DEBUG) << LOG_DESC("Get string from db") << LOG_KV("openTable", _tableName)
                              << LOG_KV("openTableTimeCost", openTable_time_cost)
                              << LOG_KV("selectTimeCost", select_time_cost)
                              << LOG_KV("getFieldTimeCost", get_field_time_cost)
                              << LOG_KV("totalTimeCost", utcTime() - start_time);
        }
    }
    else{
        LEDGER_LOG(DEBUG) << LOG_DESC("Open table error from db")
                          << LOG_KV("openTable", _tableName);
    }
    return ret;
}

std::shared_ptr<stringsPair> StorageGetter::stringsPairGetterByRowAndFields(
    const bcos::storage::TableFactoryInterface::Ptr& _tableFactory, const std::string& _tableName,
    const std::string& _row, const std::string& _field1, const std::string& _filed2)
{
    auto ret = std::make_shared<stringsPair>(std::make_pair("",""));
    auto start_time = utcTime();
    auto record_time = utcTime();

    auto table = _tableFactory->openTable(_tableName);
    auto openTable_time_cost = utcTime() - record_time;
    record_time = utcTime();
    if(table)
    {
        auto entry = table->getRow(_row);
        auto select_time_cost = utcTime() - record_time;
        if(entry)
        {
            auto field1 = entry->getField(_field1);
            auto field2 = entry->getField(_filed2);
            auto get_field_time_cost = utcTime() - record_time;

            ret->first.swap(field1);
            ret->second.swap(field2);

            LEDGER_LOG(DEBUG) << LOG_DESC("Get string from db") << LOG_KV("openTable", _tableName)
                              << LOG_KV("openTableTimeCost", openTable_time_cost)
                              << LOG_KV("selectTimeCost", select_time_cost)
                              << LOG_KV("getFieldTimeCost", get_field_time_cost)
                              << LOG_KV("totalTimeCost", utcTime() - start_time);
        }
    }
    return ret;
}
std::string StorageGetter::getTxByTxHash(
    const std::string& _txHash, const TableFactoryInterface::Ptr& _tableFactory)
{
    return tableGetterByRowAndField(_tableFactory, SYS_HASH_2_TX, _txHash, SYS_VALUE);
}
std::shared_ptr<std::vector<bytesPointer>> StorageGetter::getBatchTxByHashList(
    const std::vector<std::string>& _hashList,
    const bcos::storage::TableFactoryInterface::Ptr& _tableFactory)
{
    auto retList = std::make_shared<std::vector<bytesPointer>>();
    auto start_time = utcTime();
    auto record_time = utcTime();

    auto table = _tableFactory->openTable(SYS_HASH_2_TX);
    auto openTable_time_cost = utcTime() - record_time;
    record_time = utcTime();

    if (table)
    {
        auto hashEntryMap = table->getRows(_hashList);
        auto select_time_cost = utcTime() - record_time;
        record_time = utcTime();

        if (hashEntryMap.size() != _hashList.size())
        {
            LEDGER_LOG(DEBUG) << LOG_DESC("getRows SYS_HASH_2_TX table error from db");
            return retList;
        }

        for (const auto& hash : _hashList)
        {
            auto txStr = hashEntryMap.at(hash)->getField(SYS_VALUE);
            auto txPointer = std::make_shared<bytes>(asBytes(txStr));
            retList->emplace_back(txPointer);
        }
        auto get_field_time_cost = utcTime() - record_time;
        LEDGER_LOG(DEBUG) << LOG_DESC("Get txs list from db")
                          << LOG_KV("openTableTimeCost", openTable_time_cost)
                          << LOG_KV("selectTimeCost", select_time_cost)
                          << LOG_KV("getFieldTimeCost", get_field_time_cost)
                          << LOG_KV("totalTimeCost", utcTime() - start_time);
    }
    else
    {
        LEDGER_LOG(DEBUG) << LOG_DESC("Open SYS_HASH_2_TX table error from db");
        return retList;
    }
    return retList;
}

} // namespace bcos::ledger