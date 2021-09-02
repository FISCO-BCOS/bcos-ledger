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
#include "bcos-ledger/libledger/utilities/Common.h"
#include <bcos-framework/interfaces/protocol/CommonError.h>
#include <boost/algorithm/string.hpp>
#include <boost/lexical_cast.hpp>

using namespace bcos;
using namespace bcos::protocol;
using namespace bcos::storage;

namespace bcos::ledger
{
void StorageSetter::createTables(
    const storage::TableStorage::Ptr& _tableFactory, const std::string& _groupId)
{
    auto configFields = SYS_VALUE + "," + SYS_CONFIG_ENABLE_BLOCK_NUMBER;
    auto consensusFields = NODE_TYPE + "," + NODE_WEIGHT + "," + NODE_ENABLE_NUMBER;

    _tableFactory->createTable(SYS_CONFIG, SYS_KEY, configFields);
    _tableFactory->createTable(SYS_CONSENSUS, "node_id", consensusFields);
    _tableFactory->createTable(SYS_CURRENT_STATE, SYS_KEY, SYS_VALUE);
    _tableFactory->createTable(SYS_HASH_2_TX, "tx_hash", SYS_VALUE);
    _tableFactory->createTable(SYS_HASH_2_NUMBER, "block_hash", SYS_VALUE);
    _tableFactory->createTable(SYS_NUMBER_2_HASH, "block_num", SYS_VALUE);
    _tableFactory->createTable(SYS_NUMBER_2_BLOCK_HEADER, "block_num", SYS_VALUE);
    _tableFactory->createTable(SYS_NUMBER_2_TXS, "block_num", SYS_VALUE);
    _tableFactory->createTable(SYS_HASH_2_RECEIPT, "tx_hash", SYS_VALUE);
    _tableFactory->createTable(SYS_BLOCK_NUMBER_2_NONCES, "block_num", SYS_VALUE);
    createFileSystemTables(_tableFactory, _groupId);

    /*
    // db sync commit
    auto retPair = _tableFactory->commit();
    if ((retPair.second == nullptr || retPair.second->errorCode() == CommonError::SUCCESS) &&
        retPair.first > 0)
    {
        LEDGER_LOG(TRACE) << LOG_BADGE("createTables") << LOG_DESC("Storage commit success")
                          << LOG_KV("commitSize", retPair.first);
    }
    else
    {
        LEDGER_LOG(ERROR) << LOG_BADGE("createTables") << LOG_DESC("Storage commit error")
                          << LOG_KV("code", retPair.second->errorCode())
                          << LOG_KV("msg", retPair.second->errorMessage());
        BOOST_THROW_EXCEPTION(CreateSysTableFailed() << errinfo_comment(""));
    }
    */
}

void StorageSetter::createFileSystemTables(
    const storage::TableStorage::Ptr& _tableFactory, const std::string& _groupId)
{
    // create / dir
    _tableFactory->createTable(FS_ROOT, FS_KEY_NAME, FS_FIELD_COMBINED);
    auto table = _tableFactory->openTable(FS_ROOT);
    assert(table);
    auto rootEntry = table->newEntry();
    rootEntry->setField(FS_FIELD_TYPE, FS_TYPE_DIR);
    // TODO: set root default permission?
    rootEntry->setField(FS_FIELD_ACCESS, "");
    rootEntry->setField(FS_FIELD_OWNER, "root");
    rootEntry->setField(FS_FIELD_GID, "/usr");
    rootEntry->setField(FS_FIELD_EXTRA, "");
    table->setRow(FS_ROOT, rootEntry);

    std::string appsDir = "/" + _groupId + FS_APPS;
    std::string tableDir = "/" + _groupId + FS_USER_TABLE;

    recursiveBuildDir(_tableFactory, FS_USER);
    recursiveBuildDir(_tableFactory, FS_SYS_BIN);
    recursiveBuildDir(_tableFactory, appsDir);
    recursiveBuildDir(_tableFactory, tableDir);
}
void StorageSetter::recursiveBuildDir(
    const TableStorage::Ptr& _tableFactory, const std::string& _absoluteDir)
{
    if (_absoluteDir.empty())
    {
        return;
    }
    // transfer /usr/local/bin => ["usr", "local", "bin"]
    auto dirList = std::make_shared<std::vector<std::string>>();
    std::string absoluteDir = _absoluteDir;
    if (absoluteDir[0] == '/')
    {
        absoluteDir = absoluteDir.substr(1);
    }
    if (absoluteDir.at(absoluteDir.size() - 1) == '/')
    {
        absoluteDir = absoluteDir.substr(0, absoluteDir.size() - 1);
    }
    boost::split(*dirList, absoluteDir, boost::is_any_of("/"), boost::token_compress_on);
    std::string root = "/";
    for (auto& dir : *dirList)
    {
        auto table = _tableFactory->openTable(root);
        if (!table)
        {
            LEDGER_LOG(ERROR) << LOG_BADGE("recursiveBuildDir")
                              << LOG_DESC("can not open path table") << LOG_KV("tableName", root);
            return;
        }
        if (root != "/")
        {
            root += "/";
        }
        auto entry = table->getRow(dir);
        if (entry)
        {
            LEDGER_LOG(DEBUG) << LOG_BADGE("recursiveBuildDir")
                              << LOG_DESC("dir already existed in parent dir, continue")
                              << LOG_KV("parentDir", root) << LOG_KV("dir", dir);
            root += dir;
            continue;
        }
        // not exist, then create table and write in parent dir
        auto newFileEntry = table->newEntry();
        newFileEntry->setField(FS_FIELD_TYPE, FS_TYPE_DIR);
        // FIXME: consider permission inheritance
        newFileEntry->setField(FS_FIELD_ACCESS, "");
        newFileEntry->setField(FS_FIELD_OWNER, "root");
        newFileEntry->setField(FS_FIELD_GID, "/usr");
        newFileEntry->setField(FS_FIELD_EXTRA, "");
        table->setRow(dir, newFileEntry);

        _tableFactory->createTable(root + dir, FS_KEY_NAME, FS_FIELD_COMBINED);
        root += dir;
    }
}

bool StorageSetter::syncTableSetter(const bcos::storage::TableStorage::Ptr& _tableFactory,
    const std::string& _tableName, const std::string& _row, const std::string& _fieldName,
    const std::string& _fieldValue)
{
    auto table = _tableFactory->openTable(_tableName);
    if (table)
    {
        auto entry = table->newEntry();
        entry->setField(_fieldName, _fieldValue);
        auto ret = table->setRow(_row, entry);

        LEDGER_LOG(TRACE) << LOG_BADGE("Write data to DB") << LOG_KV("openTable", _tableName)
                          << LOG_KV("row", _row);
        return ret;
    }
    else
    {
        BOOST_THROW_EXCEPTION(OpenSysTableFailed() << errinfo_comment(_tableName));
    }
}

bool StorageSetter::setCurrentState(const TableStorage::Ptr& _tableFactory,
    const std::string& _row, const std::string& _stateValue)
{
    return syncTableSetter(_tableFactory, SYS_CURRENT_STATE, _row, SYS_VALUE, _stateValue);
}
bool StorageSetter::setNumber2Header(const TableStorage::Ptr& _tableFactory,
    const std::string& _row, const std::string& _headerValue)
{
    return syncTableSetter(_tableFactory, SYS_NUMBER_2_BLOCK_HEADER, _row, SYS_VALUE, _headerValue);
}
bool StorageSetter::setNumber2Txs(const TableStorage::Ptr& _tableFactory,
    const std::string& _row, const std::string& _txsValue)
{
    return syncTableSetter(_tableFactory, SYS_NUMBER_2_TXS, _row, SYS_VALUE, _txsValue);
}

bool StorageSetter::setHash2Number(const TableStorage::Ptr& _tableFactory,
    const std::string& _row, const std::string& _numberValue)
{
    return syncTableSetter(_tableFactory, SYS_HASH_2_NUMBER, _row, SYS_VALUE, _numberValue);
}

bool StorageSetter::setNumber2Hash(const TableStorage::Ptr& _tableFactory,
    const std::string& _row, const std::string& _hashValue)
{
    return syncTableSetter(_tableFactory, SYS_NUMBER_2_HASH, _row, SYS_VALUE, _hashValue);
}

bool StorageSetter::setNumber2Nonces(const TableStorage::Ptr& _tableFactory,
    const std::string& _row, const std::string& _noncesValue)
{
    return syncTableSetter(_tableFactory, SYS_BLOCK_NUMBER_2_NONCES, _row, SYS_VALUE, _noncesValue);
}

bool StorageSetter::setSysConfig(const TableStorage::Ptr& _tableFactory,
    const std::string& _key, const std::string& _value, const std::string& _enableBlock)
{
    auto table = _tableFactory->openTable(SYS_CONFIG);
    if (table)
    {
        auto entry = table->newEntry();
        entry->setField(SYS_VALUE, _value);
        entry->setField(SYS_CONFIG_ENABLE_BLOCK_NUMBER, _enableBlock);
        auto ret = table->setRow(_key, entry);

        LEDGER_LOG(TRACE) << LOG_BADGE("Write data to DB") << LOG_KV("openTable", SYS_CONFIG)
                          << LOG_KV("key", _key) << LOG_KV("SYS_VALUE", _value);
        return ret;
    }
    else
    {
        BOOST_THROW_EXCEPTION(OpenSysTableFailed() << errinfo_comment(SYS_CONFIG));
    }
}

bool StorageSetter::setConsensusConfig(
    const bcos::storage::TableStorage::Ptr& _tableFactory, const std::string& _type,
    const consensus::ConsensusNodeList& _nodeList, const std::string& _enableBlock)
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
            LEDGER_LOG(TRACE) << LOG_BADGE("Write data to DB") << LOG_KV("openTable", SYS_CONSENSUS)
                              << LOG_KV("NODE_TYPE", _type) << LOG_KV("NODE_WEIGHT", node->weight())
                              << LOG_KV("nodeId", node->nodeID()->hex());
        }
        return ret;
    }
    else
    {
        BOOST_THROW_EXCEPTION(OpenSysTableFailed() << errinfo_comment(SYS_CONSENSUS));
    }
}

bool StorageSetter::setHashToTx(const bcos::storage::TableStorage::Ptr& _tableFactory,
    const std::string& _txHash, const std::string& _encodeTx)
{
    return syncTableSetter(_tableFactory, SYS_HASH_2_TX, _txHash, SYS_VALUE, _encodeTx);
}

bool StorageSetter::setHashToReceipt(const bcos::storage::TableStorage::Ptr& _tableFactory,
    const std::string& _txHash, const std::string& _encodeReceipt)
{
    return syncTableSetter(_tableFactory, SYS_HASH_2_RECEIPT, _txHash, SYS_VALUE, _encodeReceipt);
}
}  // namespace bcos::ledger
