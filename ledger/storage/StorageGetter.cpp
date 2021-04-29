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
    auto hashStr = getterByBlockHash(_hash, _tableFactory, SYS_HASH_2_NUMBER);
    if (hashStr.empty())
    {
        LEDGER_LOG(DEBUG) << LOG_DESC("[#getBlockNumberByHash] Cannot get nonces, return empty.");
    }
    return hashStr;
}

std::string StorageGetter::getBlockHashByNumber(
    const std::string& _num, const bcos::storage::TableFactoryInterface::Ptr& _tableFactory)
{
    return getKeyByValue(_tableFactory,SYS_HASH_2_NUMBER,_num);
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

std::string StorageGetter::tableGetterByRowAndField(const bcos::storage::TableFactoryInterface::Ptr& _tableFactory, const std::string& _tableName, const std::string& _row, const std::string& _field)
{
    std::string ret;
    auto start_time = utcTime();
    auto record_time = utcTime();

    auto table = _tableFactory->openTable(_tableName);

    auto openTable_time_cost = utcTime() - record_time;
    record_time = utcTime();

    // TODO: return error
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

std::string StorageGetter::getKeyByValue(const TableFactoryInterface::Ptr& _tableFactory,
    const std::string& _tableName, const std::string& _keyValue)
{
    std::string ret;
    auto start_time = utcTime();
    auto record_time = utcTime();

    auto table = _tableFactory->openTable(_tableName);

    auto openTable_time_cost = utcTime() - record_time;
    record_time = utcTime();

    if(table){
        auto condition = std::make_shared<Condition>();
        condition->GE(_keyValue);
        condition->LE(_keyValue);
        auto keyVector = table->getPrimaryKeys(condition);
        auto select_time_cost = utcTime() - record_time;

        if(!keyVector.empty()){
            ret = keyVector.at(0);
            LEDGER_LOG(DEBUG) << LOG_DESC("Get string from db") << LOG_KV("openTable", _tableName)
                              << LOG_KV("openTableTimeCost", openTable_time_cost)
                              << LOG_KV("selectTimeCost", select_time_cost)
                              << LOG_KV("totalTimeCost", utcTime() - start_time);
        }
    }
    return ret;
}


} // namespace bcos::ledger