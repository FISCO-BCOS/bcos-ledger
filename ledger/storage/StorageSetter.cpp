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
 * @file StorageSetter.cpp
 * @author: kyonRay
 * @date 2021-04-23
 */

#include "StorageSetter.h"
#include "../utilities/Common.h"
#include "../utilities/BlockUtilities.h"
#include <tbb/parallel_invoke.h>
#include <tbb/parallel_for.h>
#include <boost/lexical_cast.hpp>
#include <boost/algorithm/string.hpp>
#include <bcos-framework/interfaces/ledger/LedgerTypeDef.h>
#include <bcos-framework/interfaces/protocol/CommonError.h>

using namespace bcos;
using namespace bcos::protocol;
using namespace bcos::storage;

namespace bcos::ledger
{

void StorageSetter::createTables(const storage::TableFactoryInterface::Ptr& _tableFactory)
{
    auto configFields = boost::join(std::vector{SYS_VALUE, SYS_CONFIG_ENABLE_BLOCK_NUMBER}, ",");
    auto consensusFields = boost::join(std::vector{NODE_TYPE, NODE_WEIGHT, NODE_ENABLE_NUMBER}, ",");

    _tableFactory->createTable(SYS_CONFIG, SYS_KEY, configFields);
    _tableFactory->createTable(SYS_CONSENSUS, "node_id", consensusFields);
    _tableFactory->createTable(SYS_CURRENT_STATE, SYS_KEY, SYS_VALUE);
    _tableFactory->createTable(SYS_HASH_2_TX, "tx_hash", SYS_VALUE);
    _tableFactory->createTable(SYS_HASH_2_NUMBER, "block_hash", SYS_VALUE);
    _tableFactory->createTable(SYS_NUMBER_2_HASH, "block_num", SYS_VALUE);
    _tableFactory->createTable(SYS_NUMBER_2_BLOCK_HEADER, "block_num", SYS_VALUE);
    _tableFactory->createTable(SYS_NUMBER_2_TXS, "block_num", SYS_VALUE);
    _tableFactory->createTable(SYS_HASH_2_RECEIPT, "block_num", SYS_VALUE);
    _tableFactory->createTable(SYS_BLOCK_NUMBER_2_NONCES, "block_num", SYS_VALUE);

    // db sync commit
    auto retPair = _tableFactory->commit();
    if ((retPair.second == nullptr || retPair.second->errorCode() == CommonError::SUCCESS) && retPair.first > 0)
    {
        LEDGER_LOG(TRACE) << LOG_DESC("[#buildGenesisBlock]Storage commit success")
                          << LOG_KV("commitSize", retPair.first);
    }
    else
    {
        LEDGER_LOG(ERROR) << LOG_DESC("[#buildGenesisBlock]Storage commit error");
        BOOST_THROW_EXCEPTION(CreateSysTableFailed() << errinfo_comment(""));
    }
}

bool StorageSetter::syncTableSetter(
    const bcos::storage::TableFactoryInterface::Ptr& _tableFactory, const std::string& _tableName,
    const std::string& _row, const std::string& _fieldName, const std::string& _fieldValue)
{
    auto start_time = utcTime();
    auto record_time = utcTime();

    auto table = _tableFactory->openTable(_tableName);
    auto openTable_time_cost = utcTime() - record_time;
    record_time = utcTime();

    if(table){
        auto entry = table->newEntry();
        entry->setField(_fieldName, _fieldValue);
        auto ret = table->setRow(_row, entry);
        auto insertTable_time_cost = utcTime() - record_time;

        LEDGER_LOG(DEBUG) << LOG_BADGE("Write data to DB")
                          << LOG_KV("openTable", _tableName)
                          << LOG_KV("openTableTimeCost", openTable_time_cost)
                          << LOG_KV("insertTableTimeCost", insertTable_time_cost)
                          << LOG_KV("totalTimeCost", utcTime() - start_time);
        return ret;
    } else{
        BOOST_THROW_EXCEPTION(OpenSysTableFailed() << errinfo_comment(_tableName));
    }
}

bool StorageSetter::setCurrentState(const TableFactoryInterface::Ptr& _tableFactory,
    const std::string& _row, const std::string& _stateValue)
{
    return syncTableSetter(_tableFactory, SYS_CURRENT_STATE, _row, SYS_VALUE, _stateValue);
}
bool StorageSetter::setNumber2Header(const TableFactoryInterface::Ptr& _tableFactory,
    const std::string& _row, const std::string& _headerValue)
{
    return syncTableSetter(_tableFactory, SYS_NUMBER_2_BLOCK_HEADER, _row, SYS_VALUE, _headerValue);
}
bool StorageSetter::setNumber2Txs(const TableFactoryInterface::Ptr& _tableFactory,
    const std::string& _row, const std::string& _txsValue)
{
    return syncTableSetter(_tableFactory, SYS_NUMBER_2_TXS, _row, SYS_VALUE, _txsValue);
}

bool StorageSetter::setHash2Number(const TableFactoryInterface::Ptr& _tableFactory,
    const std::string& _row, const std::string& _numberValue)
{
    return syncTableSetter(_tableFactory, SYS_HASH_2_NUMBER, _row, SYS_VALUE, _numberValue);
}

bool StorageSetter::setNumber2Hash(const TableFactoryInterface::Ptr& _tableFactory,
                                   const std::string& _row, const std::string& _hashValue)
{
    return syncTableSetter(_tableFactory, SYS_NUMBER_2_HASH, _row, SYS_VALUE, _hashValue);
}

bool StorageSetter::setNumber2Nonces(const TableFactoryInterface::Ptr& _tableFactory,
    const std::string& _row, const std::string& _noncesValue)
{
    return syncTableSetter(_tableFactory, SYS_BLOCK_NUMBER_2_NONCES, _row, SYS_VALUE, _noncesValue);
}

bool StorageSetter::setSysConfig(const TableFactoryInterface::Ptr& _tableFactory,
    const std::string& _key, const std::string& _value, const std::string& _enableBlock)
{
    auto start_time = utcTime();
    auto record_time = utcTime();

    auto table = _tableFactory->openTable(SYS_CONFIG);
    auto openTable_time_cost = utcTime() - record_time;
    record_time = utcTime();

    if (table)
    {
        auto entry = table->newEntry();
        entry->setField(SYS_VALUE, _value);
        entry->setField(SYS_CONFIG_ENABLE_BLOCK_NUMBER, _enableBlock);
        auto ret = table->setRow(_key, entry);
        auto insertTable_time_cost = utcTime() - record_time;

        LEDGER_LOG(DEBUG) << LOG_BADGE("Write data to DB") << LOG_KV("openTable", SYS_CONFIG)
                          << LOG_KV("openTableTimeCost", openTable_time_cost)
                          << LOG_KV("insertTableTimeCost", insertTable_time_cost)
                          << LOG_KV("totalTimeCost", utcTime() - start_time);
        return ret;
    }
    else
    {
        BOOST_THROW_EXCEPTION(OpenSysTableFailed() << errinfo_comment(SYS_CONFIG));
    }
}

bool StorageSetter::setConsensusConfig(
    const bcos::storage::TableFactoryInterface::Ptr& _tableFactory, const std::string& _type,
    const consensus::ConsensusNodeList& _nodeList, const std::string& _enableBlock)
{
    auto start_time = utcTime();
    auto record_time = utcTime();

    auto table = _tableFactory->openTable(SYS_CONSENSUS);
    auto openTable_time_cost = utcTime() - record_time;
    record_time = utcTime();

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
        auto insertTable_time_cost = utcTime() - record_time;

        LEDGER_LOG(DEBUG) << LOG_BADGE("Write data to DB") << LOG_KV("openTable", SYS_CONSENSUS)
                          << LOG_KV("openTableTimeCost", openTable_time_cost)
                          << LOG_KV("insertTableTimeCost", insertTable_time_cost)
                          << LOG_KV("totalTimeCost", utcTime() - start_time);
        return ret;
    }
    else
    {
        BOOST_THROW_EXCEPTION(OpenSysTableFailed() << errinfo_comment(SYS_CONSENSUS));
    }
}

bool StorageSetter::setHashToTx(const bcos::storage::TableFactoryInterface::Ptr& _tableFactory,
    const std::string& _txHash, const std::string& _encodeTx)
{
    return syncTableSetter(_tableFactory, SYS_HASH_2_TX, _txHash, SYS_VALUE, _encodeTx);
}

bool StorageSetter::setHashToReceipt(const bcos::storage::TableFactoryInterface::Ptr& _tableFactory, const std::string& _txHash, const std::string& _encodeReceipt)
{
    return syncTableSetter(_tableFactory, SYS_HASH_2_RECEIPT, _txHash, SYS_VALUE, _encodeReceipt);
}

} // namespace bcos::ledger
