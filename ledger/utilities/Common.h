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
#include <bcos-framework/interfaces/protocol/Block.h>
#include <tbb/concurrent_unordered_map.h>
#include <map>

#define LEDGER_LOG(LEVEL) LOG(LEVEL) << LOG_BADGE("LEDGER")

namespace bcos::ledger
{
// parent=>children
using Parent2ChildListMap = std::map<std::string, std::vector<std::string>>;
// child=>parent
using Child2ParentMap = tbb::concurrent_unordered_map<std::string, std::string>;

static const std::string SYS_KEY_CURRENT_NUMBER = "current_number";
static const std::string SYS_KEY_CURRENT_HASH = "current_hash";
static const std::string SYS_KEY_TOTAL_TRANSACTION_COUNT = "total_transaction_count";
static const std::string SYS_KEY_TOTAL_FAILED_TRANSACTION = "total_failed_transaction_count";
static const std::string SYS_VALUE = "value";
static const std::string SYS_KEY = "key";
static const std::string TX_INDEX = "index";
static const std::string SYS_CONFIG_ENABLE_BLOCK_NUMBER = "enable_block_num";

static const std::string SYS_CONSENSUS = "_sys_consensus_";
static const std::string SYS_ACCESS_TABLE = "_sys_table_access_";
static const std::string SYS_CNS = "_sys_cns_";
static const std::string SYS_CONFIG = "_sys_config_";
static const std::string SYS_CURRENT_STATE = "_sys_current_state_";
static const std::string SYS_TX_HASH_2_BLOCK_NUMBER = "_sys_tx_hash_2_block_number_";
static const std::string SYS_HASH_2_NUMBER = "_sys_hash_2_number_";
static const std::string SYS_NUMBER_2_HASH = "_sys_number_2_hash_";
static const std::string SYS_NUMBER_2_BLOCK = "_sys_number_2_block_";
static const std::string SYS_BLOCK_NUMBER_2_NONCES = "_sys_block_number_2_nonces_";
static const std::string SYS_NUMBER_2_BLOCK_HEADER = "_sys_number_2_header_";
static const std::string SYS_NUMBER_2_TXS = "_sys_number_2_txs_";
static const std::string SYS_NUMBER_2_RECEIPTS = "_sys_number_2_receipts";

static const std::string NODE_TYPE = "_type_";
static const std::string NODE_WEIGHT = "_weight_";
static const std::string NODE_ENABLE_NUMBER = "_enable_block_number";

struct SystemConfigRecordCache
{
    std::string value;
    bcos::protocol::BlockNumber enableNumber;
    bcos::protocol::BlockNumber curBlockNum = -1;  // at which block gets the configuration value
    SystemConfigRecordCache(std::string& _value, bcos::protocol::BlockNumber const& _enableNumber,
        bcos::protocol::BlockNumber const& _num)
      : value(_value), enableNumber(_enableNumber), curBlockNum(_num){};
};

} // namespace bcos
