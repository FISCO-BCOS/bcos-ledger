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
#include <bcos-framework/interfaces/consensus/ConsensusNodeInterface.h>
#include <bcos-framework/interfaces/protocol/Block.h>
#include <tbb/concurrent_unordered_map.h>
#include <map>

#define LEDGER_LOG(LEVEL) BCOS_LOG(LEVEL) << LOG_BADGE("LEDGER")

namespace bcos::ledger
{
// parent=>children
using Parent2ChildListMap = std::map<std::string, std::vector<std::string>>;
// child=>parent
using Child2ParentMap = tbb::concurrent_unordered_map<std::string, std::string>;

static const std::string SYS_VALUE = "value";
static const std::string SYS_KEY = "key";
static const std::string SYS_CONFIG_ENABLE_BLOCK_NUMBER = "enable_number";

static const std::string NODE_TYPE = "type";
static const std::string NODE_WEIGHT = "weight";
static const std::string NODE_ENABLE_NUMBER = "enable_number";

// FileSystem paths
static const std::string FS_ROOT = "/";
static const std::string FS_USER_BIN = "/usr/bin";
static const std::string FS_USER_LOCAL = "/usr/local";
static const std::string FS_SYS_BIN = "/bin/extensions";
static const std::string FS_USER_DATA = "/data";
// FileSystem keys
static const std::string FS_KEY_TYPE = "type";
static const std::string FS_KEY_SUB = "subdirectories";
static const std::string FS_KEY_NUM = "enable_number";
// FileSystem file type
static const std::string FS_TYPE_DIR = "directory";
static const std::string FS_TYPE_CONTRACT = "contract";
static const std::string FS_TYPE_LINK = "link";

enum LedgerError : int32_t
{
    SUCCESS = 0,
    OpenTableFailed = 3001,
    CallbackError = 3002,
    ErrorArgument = 3003,
    DecodeError = 3004,
    ErrorCommitBlock = 3005,
    CollectAsyncCallbackError = 3006,
    LedgerLockError = 3007,
    GetStorageError = 3008
};
}  // namespace bcos::ledger
