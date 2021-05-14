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
 * @file MockStorage.h
 * @author: kyonRay
 * @date 2021-05-06
 */

#pragma once

#include "../ledger/utilities/Common.h"
#include "bcos-framework/interfaces/storage/StorageInterface.h"
#include "bcos-framework/interfaces/storage/Common.h"
#include "bcos-framework/libtable/Table.h"

using namespace bcos::storage;
using namespace bcos::ledger;
namespace bcos::test
{
class MockStorage : public StorageInterface
{
public:
    MockStorage(){
        data[storage::SYS_TABLE] = std::map<std::string, Entry::Ptr>();
    }
    virtual ~MockStorage() = default;

    std::vector<std::string> getPrimaryKeys(
        std::shared_ptr<TableInfo> _tableInfo, std::shared_ptr<Condition> _condition) const override
    {
        std::vector<std::string> ret;
        std::lock_guard<std::mutex> lock(m_mutex);
        if (data.count(_tableInfo->name))
        {
            for (auto& kv : data.at(_tableInfo->name))
            {
                if (!_condition || _condition->isValid(kv.first))
                {
                    ret.emplace_back(kv.first);
                }
            }
        }
        return ret;
    }
    std::shared_ptr<Entry> getRow(
        std::shared_ptr<TableInfo> _tableInfo, const std::string_view& _key) override
    {
        std::shared_ptr<Entry> ret = nullptr;
        std::lock_guard<std::mutex> lock(m_mutex);
        if (data.count(_tableInfo->name))
        {
            if (data[_tableInfo->name].count(std::string(_key)))
            {
                return data[_tableInfo->name][std::string(_key)];
            }
        }
        return ret;
    }
    std::map<std::string, std::shared_ptr<Entry>> getRows(
        std::shared_ptr<TableInfo> _tableInfo, const std::vector<std::string>& _keys) override
    {
        std::map<std::string, std::shared_ptr<Entry>> ret;
        std::lock_guard<std::mutex> lock(m_mutex);
        if (data.count(_tableInfo->name))
        {
            for (auto& key : _keys)
            {
                if (data[_tableInfo->name].count(std::string(key)))
                {
                    ret[key] = data[_tableInfo->name][key];
                }
            }
        }
        return ret;
    }
    size_t commitTables(const std::vector<std::shared_ptr<TableInfo>> _tableInfos,
                        std::vector<std::shared_ptr<std::map<std::string, std::shared_ptr<Entry>>>>& _tableDatas)
    override
    {
        if (_tableInfos.size() != _tableDatas.size())
        {
            return 0;
        }
        std::lock_guard<std::mutex> lock(m_mutex);
        for (size_t i = 0; i < _tableInfos.size(); ++i)
        {
            for (auto& tableData : *(_tableDatas.at(i)))
            {
                if (data[_tableInfos[i]->name].count(tableData.first))
                {
                    data[_tableInfos[i]->name].at(tableData.first) = tableData.second;
                }
                else
                {
                    data[_tableInfos[i]->name].insert(tableData);
                }
            }
        }
        return _tableInfos.size();
    }
    void asyncGetPrimaryKeys(std::shared_ptr<TableInfo>, std::shared_ptr<Condition>,
                             std::function<void(Error, std::vector<std::string>)>) override
    {}
    void asyncGetRow(std::shared_ptr<TableInfo>, std::shared_ptr<std::string>,
                     std::function<void(Error, std::shared_ptr<Entry>)>) override
    {}
    void asyncGetRows(std::shared_ptr<TableInfo>, std::shared_ptr<std::vector<std::string>>,
                      std::function<void(Error, std::map<std::string, std::shared_ptr<Entry>>)>) override
    {}
    void asyncCommitTables(std::shared_ptr<std::vector<std::shared_ptr<TableInfo>>>,
                           std::shared_ptr<std::vector<std::shared_ptr<std::map<std::string, Entry::Ptr>>>>&,
                           std::function<void(Error, size_t)>) override
    {}

    // cache TableFactory
    void asyncAddStateCache(protocol::BlockNumber, protocol::Block::Ptr,
                            std::shared_ptr<TableFactory>, std::function<void(Error)>) override
    {}
    void asyncDropStateCache(protocol::BlockNumber, std::function<void(Error)>) override {}
    void asyncGetBlock(
        protocol::BlockNumber, std::function<void(Error, protocol::Block::Ptr)>) override
    {}
    void asyncGetStateCache(
        protocol::BlockNumber, std::function<void(Error, std::shared_ptr<TableFactory>)>) override
    {}
    protocol::Block::Ptr getBlock(protocol::BlockNumber _blockNumber) override
    {
        if (m_number2TableFactory.count(_blockNumber))
        {
            return m_number2TableFactory[_blockNumber].block;
        }
        return nullptr;
    }

    std::shared_ptr<TableFactory> getStateCache(protocol::BlockNumber _blockNumber) override
    {
        if (m_number2TableFactory.count(_blockNumber))
        {
            return m_number2TableFactory[_blockNumber].tableFactory;
        }
        return nullptr;
    }

    void dropStateCache(protocol::BlockNumber) override {}
    void addStateCache(
        protocol::BlockNumber _blockNumber, protocol::Block::Ptr _block, std::shared_ptr<TableFactory> _tableFactory) override
    {
        m_number2TableFactory[_blockNumber] = BlockCache{_block, _tableFactory};
    }
    // KV store in split database, used to store data off-chain
    bool put(const std::string&, const std::string_view&, const std::string_view&) override
    {
        return true;
    }
    std::string get(const std::string&, const std::string_view&) override { return ""; }
    void asyncGetBatch(const std::string&, std::shared_ptr<std::vector<std::string_view>>,
                       std::function<void(Error, std::shared_ptr<std::vector<std::string>>)>) override
    {}
    struct BlockCache
    {
        protocol::Block::Ptr block;
        std::shared_ptr<TableFactory> tableFactory;
    };
private:
    std::map<std::string, std::map<std::string, Entry::Ptr>> data;
    mutable std::mutex m_mutex;
    std::map<protocol::BlockNumber, BlockCache> m_number2TableFactory;
};
}