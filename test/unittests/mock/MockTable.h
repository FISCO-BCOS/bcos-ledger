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

    MockTable(std::string const& _tableName):Table(nullptr, nullptr, nullptr, 0),
        m_tableName(_tableName) {}

    std::shared_ptr<Entry> getRow(const std::string &_key) override
    {
        auto entry = m_fakeStorage[_key];
        if(entry){
            return entry;
        }
        return nullptr;
    }

    bool setRow(const std::string &_key, std::shared_ptr<Entry> _entry) override{
        m_fakeStorage[_key] = _entry;
        return true;
    }

    std::vector<std::string> getPrimaryKeys(std::shared_ptr<Condition>) const override{
        std::vector<std::string> keys;
        keys.reserve(m_fakeStorage.size());
        std::transform(m_fakeStorage.begin(), m_fakeStorage.end(), keys.begin(), [](auto pair){return pair.first;});
        return keys;
    }

private:
    std::string m_tableName;
    std::unordered_map<std::string, Entry::Ptr> m_fakeStorage;
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
        auto table = std::make_shared<MockTable>(_tableName);
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
