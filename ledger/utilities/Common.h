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
#include <bcos-framework/interfaces/consensus/ConsensusNodeInterface.h>
#include <tbb/concurrent_unordered_map.h>
#include <map>

#define LEDGER_LOG(LEVEL) LOG(LEVEL) << LOG_BADGE("LEDGER")

namespace bcos::ledger
{
// parent=>children
using Parent2ChildListMap = std::map<std::string, std::vector<std::string>>;
// child=>parent
using Child2ParentMap = tbb::concurrent_unordered_map<std::string, std::string>;

static const std::string SYS_VALUE = "value";
static const std::string SYS_KEY = "key";
static const std::string SYS_CONFIG_ENABLE_BLOCK_NUMBER = "enable_block_number";

static const std::string NODE_TYPE = "type";
static const std::string NODE_WEIGHT = "weight";
static const std::string NODE_ENABLE_NUMBER = "enable_block_number";

struct LedgerConfigCache
{
    std::string value;
    bcos::protocol::BlockNumber enableNumber;
    bcos::protocol::BlockNumber curBlockNum = -1;  // at which block gets the configuration value
    LedgerConfigCache(const std::string& _value, bcos::protocol::BlockNumber const& _enableNumber,
        bcos::protocol::BlockNumber const& _num)
      : value(_value), enableNumber(_enableNumber), curBlockNum(_num){};
};
} // namespace bcos
