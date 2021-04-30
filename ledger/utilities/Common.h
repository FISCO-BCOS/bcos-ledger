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
 * @file Common.h
 * @author: kyonRay
 * @date 2021-04-13
 */
#pragma once
#include "interfaces/protocol/Block.h"
#include "libutilities/ThreadPool.h"
#include <tbb/concurrent_unordered_map.h>
#include <tbb/parallel_for_each.h>
#include <map>

#define LEDGER_LOG(LEVEL) LOG(LEVEL) << LOG_BADGE("LEDGER")

namespace bcos::ledger
{
// parent=>children
using Parent2ChildListMap = std::map<std::string, std::vector<std::string>>;
// child=>parent
using Child2ParentMap = tbb::concurrent_unordered_map<std::string, std::string>;

static const std::string ID_FIELD = "_id_";
static const std::string NUM_FIELD = "_num_";
static const std::string STATUS = "_status_";
static const std::string SYS_KEY_CURRENT_NUMBER = "current_number";
static const std::string SYS_KEY_CURRENT_HASH = "current_hash";
static const std::string SYS_KEY_CURRENT_ID = "current_id";
static const std::string SYS_KEY_TOTAL_TRANSACTION_COUNT = "total_transaction_count";
static const std::string SYS_KEY_TOTAL_FAILED_TRANSACTION = "total_failed_transaction_count";
static const std::string SYS_VALUE = "value";
static const std::string SYS_KEY = "key";
static const std::string SYS_CONFIG_ENABLE_BLOCK_NUMBER = "enable_block_num";

static const std::string SYS_TABLES = "_sys_tables_";
static const std::string SYS_CONSENSUS = "_sys_consensus_";
static const std::string SYS_ACCESS_TABLE = "_sys_table_access_";
static const std::string SYS_CNS = "_sys_cns_";
static const std::string SYS_CONFIG = "_sys_config_";
static const std::string SYS_CURRENT_STATE = "_sys_current_state_";
static const std::string SYS_TX_HASH_2_BLOCK_NUMBER = "_sys_tx_hash_2_block_number_";
static const std::string SYS_HASH_2_NUMBER = "_sys_hash_2_number_";
static const std::string SYS_NUMBER_2_BLOCK = "_sys_number_2_block_";
static const std::string SYS_BLOCK_NUMBER_2_NONCES = "_sys_block_number_2_nonces_";
static const std::string SYS_NUMBER_2_BLOCK_HEADER = "_sys_number_2_header_";
static const std::string SYS_NUMBER_2_TXS = "_sys_number_2_txs_";
static const std::string SYS_NUMBER_2_RECEIPTS = "_sys_number_2_receipts";

struct SystemConfigRecordCache
{
    std::string value;
    bcos::protocol::BlockNumber enableNumber;
    bcos::protocol::BlockNumber curBlockNum = -1;  // at which block gets the configuration value
    SystemConfigRecordCache(std::string& _value, bcos::protocol::BlockNumber const& _enableNumber,
        bcos::protocol::BlockNumber const& _num)
      : value(_value), enableNumber(_enableNumber), curBlockNum(_num){};
};

protocol::TransactionsPtr blockTransactionListGetter(const protocol::Block::Ptr& _block)
{
    auto txs = std::make_shared<std::vector<protocol::Transaction::Ptr>>();
    if(_block == nullptr){
        return txs;
    }
    auto txSize = _block->transactionsSize();
    if(txSize == 0){
        LEDGER_LOG(DEBUG)<<LOG_DESC("Block transactions size is 0, return nullptr");
        return nullptr;
    }
    for (size_t i = 0; i < txSize; ++i)
    {
        auto tx = std::const_pointer_cast<protocol::Transaction>(_block->transaction(i));
        txs->emplace_back(tx);
    }
    return txs;
}

size_t blockTransactionListSetter(const protocol::Block::Ptr& _block, const protocol::TransactionsPtr& _txs){

    if(_block == nullptr || _txs == nullptr || _txs->empty()){
        LEDGER_LOG(DEBUG)<<LOG_DESC("blockTransactionListSetter set error");
        return -1;
    }
    for (const auto& tx : *_txs)
    {
        _block->appendTransaction(tx);
    }
    return _block->transactionsSize();
}

protocol::ReceiptsPtr blockReceiptListGetter(const protocol::Block::Ptr& _block)
{
    auto receipts = std::make_shared<std::vector<protocol::TransactionReceipt::Ptr>>();
    if(_block == nullptr){
        return receipts;
    }
    auto receiptSize = _block->receiptsSize();
    if(receiptSize == 0){
        LEDGER_LOG(DEBUG)<<LOG_DESC("Block receipts size is 0, return nullptr");
        return nullptr;
    }
    for (size_t i = 0; i < receiptSize; ++i)
    {
        auto receipt = std::const_pointer_cast<protocol::TransactionReceipt>(_block->receipt(i));
        receipts->emplace_back(receipt);
    }
    return receipts;
}

size_t blockReceiptListSetter(const protocol::Block::Ptr& _block, const protocol::ReceiptsPtr& _receipts)
{
    if(_block == nullptr || _receipts == nullptr || _receipts->empty()){
        LEDGER_LOG(DEBUG)<<LOG_DESC("Block receipts size is 0");
        return -1;
    }
    for (const auto& rcpt : *_receipts)
    {
        _block->appendReceipt(rcpt);
    }
    return _block->receiptsSize();
}


} // namespace bcos
