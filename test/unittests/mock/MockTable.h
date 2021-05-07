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
 * @file MockTable.h
 * @author: kyonRay
 * @date 2021-05-07
 */
#pragma once

#include "bcos-ledger/ledger/utilities/Common.h"
#include "bcos-framework/libtable/Table.h"
#include "bcos-framework/libtable/TableFactory.h"
#include "bcos-framework/interfaces/storage/Common.h"
#include "MockBlock.h"
#include "MockBlockHeader.h"
#include <tbb/concurrent_unordered_map.h>

using namespace bcos::storage;
using namespace bcos::ledger;

namespace bcos::test{
class MockTable : public Table
{
public:
    using Ptr = std::shared_ptr<MockTable>;

    MockTable(): Table(nullptr, nullptr, nullptr, 0)
    {
        m_fakeStorage[SYS_CONFIG] = std::unordered_map<std::string, Entry::Ptr>();
        m_fakeStorage[SYS_CURRENT_STATE] = std::unordered_map<std::string, Entry::Ptr>();
        m_fakeStorage[SYS_HASH_2_NUMBER] = std::unordered_map<std::string, Entry::Ptr>();
        m_fakeStorage[SYS_NUMBER_2_BLOCK_HEADER] = std::unordered_map<std::string, Entry::Ptr>();
        m_fakeStorage[SYS_NUMBER_2_TXS] = std::unordered_map<std::string, Entry::Ptr>();
        m_fakeStorage[SYS_NUMBER_2_RECEIPTS] = std::unordered_map<std::string, Entry::Ptr>();
        m_fakeStorage[SYS_TX_HASH_2_BLOCK_NUMBER] = std::unordered_map<std::string, Entry::Ptr>();
    }

private:
    std::unordered_map<std::string, std::unordered_map<std::string, Entry::Ptr>> m_fakeStorage;
};
class MockTableFactory : public TableFactory
{
public:
    MockTableFactory(): TableFactory(nullptr, nullptr, 0){}
    using Ptr = std::shared_ptr<MockTableFactory>;
    std::shared_ptr<TableInterface> openTable(const std::string &_tableName) override
    {
        auto it = m_name2Table.find(_tableName);
        if (it != m_name2Table.end())
        {
            return it->second;
        }
        auto table = std::make_shared<MockTable>();
        m_name2Table.insert({_tableName, table});
        return table;
    }
    void commit() override
    {
        m_name2Table.clear();
    }

private:

    tbb::concurrent_unordered_map<std::string, Table::Ptr> m_name2Table;
};

class MockErrorTableFactory : public TableFactory
{
public:
    MockErrorTableFactory(): TableFactory(nullptr, nullptr, 0){}
    std::shared_ptr<TableInterface> openTable(const std::string &) override
    {
        return nullptr;
    }
};
}
