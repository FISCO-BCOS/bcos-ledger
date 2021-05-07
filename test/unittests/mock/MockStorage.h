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

#include "bcos-framework/interfaces/storage/StorageInterface.h"
#include "bcos-framework/interfaces/storage/Common.h"
#include "MockBlock.h"
#include "MockBlockHeader.h"

using namespace bcos::storage;

namespace bcos::test
{
class MockStorage : public storage::StorageInterface{
public:
    std::vector<std::string> getPrimaryKeys(
        std::shared_ptr<TableInfo> , std::shared_ptr<Condition> ) const override
    {
        std::vector<std::string> keys{"1","2","3"};
        return keys;
    }
    std::shared_ptr<Entry> getRow(
        std::shared_ptr<TableInfo> _table, const std::string_view&) override
    {
        auto entry = std::make_shared<Entry>();
        entry->setField(_table->key, "value");
        entry->setDirty(false);
        return entry;
    }
    std::map<std::string, std::shared_ptr<Entry>> getRows(
        std::shared_ptr<TableInfo> _tableInfo, const std::vector<std::string>& _keys) override
    {
        std::map<std::string, std::shared_ptr<Entry>> ret;
        auto entry = std::make_shared<Entry>();
        entry->setField(_tableInfo->key, "value");
        entry->setDirty(false);
        ret.insert(std::make_pair(_keys.at(0),entry));
        return ret;
    }
    size_t commitTables(const std::vector<std::shared_ptr<TableInfo>> ,
        std::vector<std::shared_ptr<std::map<std::string, std::shared_ptr<Entry>>>>& )
        override
    {
        return 0;
    }
    void asyncGetPrimaryKeys(std::shared_ptr<TableInfo> , std::shared_ptr<Condition> ,
        std::function<void(Error, std::vector<std::string>)> ) override
    {}
    void asyncGetRow(std::shared_ptr<TableInfo> , std::shared_ptr<std::string> ,
        std::function<void(Error, std::shared_ptr<Entry>)> ) override
    {}
    void asyncGetRows(std::shared_ptr<TableInfo> ,
        std::shared_ptr<std::vector<std::string>> ,
        std::function<void(Error, std::map<std::string, std::shared_ptr<Entry>>)> )
        override
    {}
    void asyncCommitTables(std::shared_ptr<std::vector<std::shared_ptr<TableInfo>>> ,
                           std::shared_ptr<std::vector<std::shared_ptr<std::map<std::string, Entry::Ptr>>>>& ,
        std::function<void(Error, size_t)> ) override
    {}
    void asyncAddStateCache(protocol::BlockNumber , protocol::Block::Ptr ,
                            std::shared_ptr<TableFactory> , std::function<void(Error)> ) override
    {}
    void asyncDropStateCache(
        protocol::BlockNumber , std::function<void(Error)> ) override
    {}
    void asyncGetBlock(protocol::BlockNumber ,
        std::function<void(Error, protocol::Block::Ptr)> ) override
    {}
    void asyncGetStateCache(protocol::BlockNumber ,
        std::function<void(Error, std::shared_ptr<TableFactory>)> ) override
    {}
    protocol::Block::Ptr getBlock(protocol::BlockNumber _number) override
    {
        if (m_number2TableFactory.count(_number))
        {
            return m_number2TableFactory[_number].block;
        }
        auto header = std::make_shared<MockBlockHeader>();
        header->setNumber(_number);
        auto block = std::make_shared<MockBlock>();
        block->setBlockHeader(header);
        return block;
    }
    std::shared_ptr<TableFactory> getStateCache(protocol::BlockNumber _blockNumber) override
    {
        if (m_number2TableFactory.count(_blockNumber))
        {
            return m_number2TableFactory[_blockNumber].tableFactory;
        }
        return nullptr;
    }
    void dropStateCache(protocol::BlockNumber _blockNumber) override
    {
        m_number2TableFactory.erase(_blockNumber);
    }
    void addStateCache(protocol::BlockNumber _blockNumber, protocol::Block::Ptr _block,
                       std::shared_ptr<TableFactory> _tableFactory) override
    {
        m_number2TableFactory[_blockNumber] = BlockCache{_block, _tableFactory};
    }
    bool put(const std::string& , const std::string_view& ,
        const std::string_view& ) override
    {
        return false;
    }
    std::string get(const std::string& , const std::string_view& ) override
    {
        return std::string();
    }
    void asyncGetBatch(const std::string& ,
        std::shared_ptr<std::vector<std::string_view>> ,
        std::function<void(Error, std::shared_ptr<std::vector<std::string>>)> ) override
    {}

    struct BlockCache
    {
        protocol::Block::Ptr block;
        std::shared_ptr<TableFactory> tableFactory;
    };
private:
    std::map<protocol::BlockNumber, BlockCache> m_number2TableFactory;
};
}