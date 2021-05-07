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
#include "bcos-ledger/ledger/utilities/Common.h"
#include "bcos-ledger/ledger/utilities/BlockUtilities.h"
#include <tbb/parallel_invoke.h>
#include <tbb/parallel_for.h>
#include <boost/lexical_cast.hpp>

using namespace bcos;
using namespace bcos::protocol;
using namespace bcos::storage;

namespace bcos::ledger
{

bool StorageSetter::tableSetterByRowAndField(
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

bool StorageSetter::setNumber2Block(const TableFactoryInterface::Ptr& _tableFactory,
    const std::string& _row, const std::string& _blockValue)
{
    // return update(SYS_NUMBER_2_BLOCK)
    //     .set(SYS_VALUE, _blockValue)
    //     .whereRowIs(_row)
    //    .exec(_tableFactory);
    return tableSetterByRowAndField(
        _tableFactory, SYS_NUMBER_2_BLOCK, _row, SYS_VALUE, _blockValue);
}

bool StorageSetter::setCurrentState(const TableFactoryInterface::Ptr& _tableFactory,
    const std::string& _row, const std::string& _stateValue)
{
    // return update(SYS_CURRENT_STATE).set(SYS_VALUE,_stateValue).whereRowIs(_row).exec(_tableFactory);
    return tableSetterByRowAndField(_tableFactory, SYS_CURRENT_STATE, _row, SYS_VALUE, _stateValue);
}
bool StorageSetter::setNumber2Header(const TableFactoryInterface::Ptr& _tableFactory,
    const std::string& _row, const std::string& _headerValue)
{
    // return update(SYS_NUMBER_2_BLOCK_HEADER).set(SYS_VALUE,_headerValue).whereRowIs(_row).exec(_tableFactory);
    return tableSetterByRowAndField(
        _tableFactory, SYS_NUMBER_2_BLOCK_HEADER, _row, SYS_VALUE, _headerValue);
}
bool StorageSetter::setNumber2Txs(const TableFactoryInterface::Ptr& _tableFactory,
    const std::string& _row, const std::string& _txsValue)
{
    // return update(SYS_NUMBER_2_TXS).set(SYS_VALUE,_txsValue).whereRowIs(_row).exec(_tableFactory);
    return tableSetterByRowAndField(_tableFactory, SYS_NUMBER_2_TXS, _row, SYS_VALUE, _txsValue);
}
bool StorageSetter::setNumber2Receipts(const TableFactoryInterface::Ptr& _tableFactory,
    const std::string& _row, const std::string& _receiptsValue)
{
    // return update(SYS_NUMBER_2_RECEIPTS).set(SYS_VALUE,_receiptsValue).whereRowIs(_row).exec(_tableFactory);
    return tableSetterByRowAndField(_tableFactory, SYS_NUMBER_2_RECEIPTS,_row,SYS_VALUE,_receiptsValue);
}
bool StorageSetter::setHash2Number(const TableFactoryInterface::Ptr& _tableFactory,
    const std::string& _row, const std::string& _numberValue)
{
    // return update(SYS_HASH_2_NUMBER).set(SYS_VALUE,_numberValue).whereRowIs(_row).exec(_tableFactory);
    return tableSetterByRowAndField(_tableFactory, SYS_HASH_2_NUMBER, _row, SYS_VALUE, _numberValue);
}

bool StorageSetter::setNumber2Nonces(const TableFactoryInterface::Ptr& _tableFactory,
    const std::string& _row, const std::string& _noncesValue)
{
    // return update(SYS_BLOCK_NUMBER_2_NONCES)
    //    .set(SYS_VALUE, _noncesValue)
    //    .whereRowIs(_row)
    //    .exec(_tableFactory);
    return tableSetterByRowAndField(
        _tableFactory, SYS_BLOCK_NUMBER_2_NONCES, _row, SYS_VALUE, _noncesValue);
}

void StorageSetter::writeTxToBlock(
    const Block::Ptr& _block, const TableFactoryInterface::Ptr& _tableFactory)
{
    auto start_time = utcTime();
    auto record_time = utcTime();
    TableInterface::Ptr tb = _tableFactory->openTable(SYS_TX_HASH_2_BLOCK_NUMBER);
    auto openTable_time_cost = utcTime() - record_time;
    record_time = utcTime();
    if (tb)
    {
        auto txs = blockTransactionListGetter(_block);
        auto constructVector_time_cost = utcTime() - record_time;
        record_time = utcTime();
        auto blockNumberStr = boost::lexical_cast<std::string>(_block->blockHeader()->number());
        tbb::parallel_for(
            tbb::blocked_range<size_t>(0, txs->size()), [&](const tbb::blocked_range<size_t>& _r) {
              for (size_t i = _r.begin(); i != _r.end(); ++i)
              {
                  auto entry = tb->newEntry();
                  // entry: <blockNumber, txIndex>
                  entry->setField(SYS_VALUE, blockNumberStr);
                  entry->setField("index", boost::lexical_cast<std::string>(i));
                  tb->setRow((*txs)[i]->hash().hex(), entry);
              }
            });
        auto insertTable_time_cost = utcTime() - record_time;
        LEDGER_LOG(DEBUG) << LOG_BADGE("WriteTxOnCommit")
                          << LOG_DESC("Write tx to block time record")
                          << LOG_KV("openTableTimeCost", openTable_time_cost)
                          << LOG_KV("constructVectorTimeCost", constructVector_time_cost)
                          << LOG_KV("insertTableTimeCost", insertTable_time_cost)
                          << LOG_KV("totalTimeCost", utcTime() - start_time);
    }
    else
    {
        BOOST_THROW_EXCEPTION(OpenSysTableFailed() << errinfo_comment(SYS_TX_HASH_2_BLOCK_NUMBER));
    }
}

bool StorageSetter::exec(const TableFactoryInterface::Ptr& _tableFactory)
{
    auto start_time = utcTime();
    auto record_time = utcTime();

    auto table = _tableFactory->openTable(this->tableName);
    auto openTable_time_cost = utcTime() - record_time;
    record_time = utcTime();

    if(table){
        auto entry = table->newEntry();
        for(stringsPair& pair: this->setterPair){
            entry->setField(pair.first,pair.second);
        }
        auto ret = table->setRow(this->row, entry);
        auto insertTable_time_cost = utcTime() - record_time;
        LEDGER_LOG(DEBUG) << LOG_BADGE("Write data to DB")
                          << LOG_KV("openTable", this->tableName)
                          << LOG_KV("openTableTimeCost", openTable_time_cost)
                          << LOG_KV("insertTableTimeCost", insertTable_time_cost)
                          << LOG_KV("totalTimeCost", utcTime() - start_time);
        return ret;
    } else{
        BOOST_THROW_EXCEPTION(OpenSysTableFailed() << errinfo_comment(SYS_CURRENT_STATE));
    }
}
} // namespace bcos::ledger
