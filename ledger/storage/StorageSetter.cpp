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
#include <bcos-framework/interfaces/protocol/CommonError.h>
#include <boost/algorithm/string.hpp>
#include <boost/lexical_cast.hpp>

using namespace bcos;
using namespace bcos::protocol;
using namespace bcos::storage;

namespace bcos::ledger
{
std::string FileInfo::toString()
{
    std::stringstream ss;
    boost::archive::text_oarchive oa(ss);
    oa << *this;
    return ss.str();
}

bool FileInfo::fromString(FileInfo& _f, std::string _str)
{
    std::stringstream ss(_str);
    try
    {
        boost::archive::text_iarchive ia(ss);
        ia >> _f;
    }
    catch (boost::archive::archive_exception const& e)
    {
        LEDGER_LOG(ERROR) << LOG_BADGE("FileInfo::fromString") << LOG_DESC("deserialization error")
                          << LOG_KV("e.what", e.what()) << LOG_KV("str", _str);
        return false;
    }
    return true;
}

std::string DirInfo::toString()
{
    std::stringstream ss;
    boost::archive::text_oarchive oa(ss);
    oa << *this;
    return ss.str();
}

bool DirInfo::fromString(DirInfo& _dir, std::string _str)
{
    std::stringstream ss(_str);
    try
    {
        boost::archive::text_iarchive ia(ss);
        ia >> _dir;
    }
    catch (boost::archive::archive_exception const& e)
    {
        LEDGER_LOG(ERROR) << LOG_BADGE("DirInfo::fromString") << LOG_DESC("deserialization error")
                          << LOG_KV("e.what", e.what()) << LOG_KV("str", _str);
        return false;
    }
    return true;
}

void StorageSetter::createTables(const storage::TableFactoryInterface::Ptr& _tableFactory)
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
    _tableFactory->createTable(SYS_HASH_2_RECEIPT, "block_num", SYS_VALUE);
    _tableFactory->createTable(SYS_BLOCK_NUMBER_2_NONCES, "block_num", SYS_VALUE);
    createFileSystemTables(_tableFactory);
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
        LEDGER_LOG(ERROR) << LOG_BADGE("createTables") << LOG_DESC("Storage commit error");
        BOOST_THROW_EXCEPTION(CreateSysTableFailed() << errinfo_comment(""));
    }
}

void StorageSetter::createFileSystemTables(const storage::TableFactoryInterface::Ptr& _tableFactory)
{
    _tableFactory->createTable(FS_ROOT, SYS_KEY, SYS_VALUE);
    auto table = _tableFactory->openTable(FS_ROOT);
    auto typeEntry = table->newEntry();
    typeEntry->setField(SYS_VALUE, FS_TYPE_DIR);
    table->setRow(FS_KEY_TYPE, typeEntry);

    auto subEntry = table->newEntry();
    subEntry->setField(SYS_VALUE, DirInfo::emptyDirString());
    table->setRow(FS_KEY_SUB, subEntry);

    recursiveBuildDir(_tableFactory, FS_USER_BIN);
    recursiveBuildDir(_tableFactory, FS_USER_LOCAL);
    recursiveBuildDir(_tableFactory, FS_SYS_BIN);
    recursiveBuildDir(_tableFactory, FS_USER_DATA);
}
void StorageSetter::recursiveBuildDir(
    const TableFactoryInterface::Ptr& _tableFactory, const std::string& _absoluteDir)
{
    if (_absoluteDir.empty())
    {
        return;
    }
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
    DirInfo parentDir;
    for (auto& dir : *dirList)
    {
        auto table = _tableFactory->openTable(root);
        if (root != "/")
        {
            root += "/";
        }
        if (!table)
        {
            LEDGER_LOG(ERROR) << LOG_BADGE("recursiveBuildDir")
                              << LOG_DESC("can not open table root") << LOG_KV("root", root);
            return;
        }
        auto entry = table->getRow(FS_KEY_SUB);
        if (!entry)
        {
            LEDGER_LOG(ERROR) << LOG_BADGE("recursiveBuildDir")
                              << LOG_DESC("can get entry of FS_KEY_SUB") << LOG_KV("root", root);
            return;
        }
        auto subdirectories = entry->getField(SYS_VALUE);
        if (!DirInfo::fromString(parentDir, subdirectories))
        {
            LEDGER_LOG(ERROR) << LOG_BADGE("recursiveBuildDir") << LOG_DESC("parse error")
                              << LOG_KV("str", subdirectories);
            return;
        }
        FileInfo newDirectory(dir, FS_TYPE_DIR, 0);
        bool exist = false;
        for (const FileInfo& _f : parentDir.getSubDir())
        {
            if (_f.getName() == dir)
            {
                exist = true;
                break;
            }
        }
        if (exist)
        {
            root += dir;
            continue;
        }
        parentDir.getMutableSubDir().emplace_back(newDirectory);
        entry->setField(SYS_VALUE, parentDir.toString());
        table->setRow(FS_KEY_SUB, entry);

        std::string newDirPath = root + dir;
        _tableFactory->createTable(newDirPath, SYS_KEY, SYS_VALUE);
        auto newTable = _tableFactory->openTable(newDirPath);
        auto typeEntry = newTable->newEntry();
        typeEntry->setField(SYS_VALUE, FS_TYPE_DIR);
        newTable->setRow(FS_KEY_TYPE, typeEntry);

        auto subEntry = newTable->newEntry();
        subEntry->setField(SYS_VALUE, DirInfo::emptyDirString());
        newTable->setRow(FS_KEY_SUB, subEntry);

        auto numberEntry = newTable->newEntry();
        numberEntry->setField(SYS_VALUE, "0");
        newTable->setRow(FS_KEY_NUM, numberEntry);
        root += dir;
    }
}

bool StorageSetter::syncTableSetter(const bcos::storage::TableFactoryInterface::Ptr& _tableFactory,
    const std::string& _tableName, const std::string& _row, const std::string& _fieldName,
    const std::string& _fieldValue)
{
    auto start_time = utcTime();
    auto record_time = utcTime();

    auto table = _tableFactory->openTable(_tableName);
    auto openTable_time_cost = utcTime() - record_time;
    record_time = utcTime();

    if (table)
    {
        auto entry = table->newEntry();
        entry->setField(_fieldName, _fieldValue);
        auto ret = table->setRow(_row, entry);
        auto insertTable_time_cost = utcTime() - record_time;

        LEDGER_LOG(TRACE) << LOG_BADGE("Write data to DB") << LOG_KV("openTable", _tableName)
                          << LOG_KV("openTableTimeCost", openTable_time_cost)
                          << LOG_KV("insertTableTimeCost", insertTable_time_cost)
                          << LOG_KV("totalTimeCost", utcTime() - start_time);
        return ret;
    }
    else
    {
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

        LEDGER_LOG(TRACE) << LOG_BADGE("Write data to DB") << LOG_KV("openTable", SYS_CONFIG)
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

        LEDGER_LOG(TRACE) << LOG_BADGE("Write data to DB") << LOG_KV("openTable", SYS_CONSENSUS)
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

bool StorageSetter::setHashToReceipt(const bcos::storage::TableFactoryInterface::Ptr& _tableFactory,
    const std::string& _txHash, const std::string& _encodeReceipt)
{
    return syncTableSetter(_tableFactory, SYS_HASH_2_RECEIPT, _txHash, SYS_VALUE, _encodeReceipt);
}
}  // namespace bcos::ledger
