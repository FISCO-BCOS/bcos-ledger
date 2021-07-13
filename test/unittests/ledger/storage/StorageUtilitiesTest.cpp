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
 * @file StorageUtilitiesTest.cpp
 * @author: kyonRay
 * @date 2021-05-06
 */

#include "bcos-ledger/libledger/storage/StorageUtilities.h"
#include "mock/MockKeyFactor.h"
#include "unittests/ledger/common/FakeBlock.h"
#include "unittests/ledger/common/FakeTable.h"
#include <bcos-framework/interfaces/ledger/LedgerTypeDef.h>
#include <bcos-framework/testutils/TestPromptFixture.h>
#include <boost/lexical_cast.hpp>
#include <boost/test/unit_test.hpp>

using namespace bcos;
using namespace bcos::ledger;
using namespace bcos::protocol;

namespace bcos::test
{
class TableFactoryFixture : public TestPromptFixture
{
public:
    TableFactoryFixture() : TestPromptFixture()
    {
        tableFactory = fakeTableFactory(0);
        BOOST_TEST(tableFactory != nullptr);
        StorageUtilities::createTables(tableFactory);
    }
    ~TableFactoryFixture() {}

    TableFactoryInterface::Ptr tableFactory = nullptr;
};

BOOST_FIXTURE_TEST_SUITE(StorageUtilitiesTest, TableFactoryFixture)

BOOST_AUTO_TEST_CASE(testCreateTable)
{
    auto table = tableFactory->openTable(FS_ROOT);
    BOOST_CHECK_EQUAL(table->getRow(FS_KEY_TYPE)->getField(SYS_VALUE), "directory");
    DirInfo d1;
    DirInfo::fromString(d1, table->getRow(FS_KEY_SUB)->getField(SYS_VALUE));
    BOOST_CHECK_EQUAL(d1.getSubDir().at(0).getName(), "usr");
    BOOST_CHECK_EQUAL(d1.getSubDir().at(1).getName(), "bin");
    BOOST_CHECK_EQUAL(d1.getSubDir().at(2).getName(), "data");

    table = tableFactory->openTable("/usr");
    BOOST_CHECK_EQUAL(table->getRow(FS_KEY_TYPE)->getField(SYS_VALUE), "directory");
    DirInfo d2;
    DirInfo::fromString(d2, table->getRow(FS_KEY_SUB)->getField(SYS_VALUE));
    BOOST_CHECK_EQUAL(d2.getSubDir().at(0).getName(), "bin");
    BOOST_CHECK_EQUAL(d2.getSubDir().at(1).getName(), "local");

    table = tableFactory->openTable("/bin");
    BOOST_CHECK_EQUAL(table->getRow(FS_KEY_TYPE)->getField(SYS_VALUE), "directory");
    DirInfo d3;
    DirInfo::fromString(d3, table->getRow(FS_KEY_SUB)->getField(SYS_VALUE));
    BOOST_CHECK_EQUAL(d3.getSubDir().at(0).getName(), "extensions");
}

BOOST_AUTO_TEST_CASE(testTableSetterGetterByRowAndField)
{
    bool setterRet = StorageUtilities::syncTableSetter(
        tableFactory, SYS_HASH_2_NUMBER, "test", SYS_VALUE, "world");
    BOOST_CHECK(setterRet);

    std::promise<bool> p1;
    auto f1 = p1.get_future();
    StorageUtilities::asyncTableGetter(tableFactory, SYS_HASH_2_NUMBER, "test",
        [&](Error::Ptr _error, bcos::storage::Entry::Ptr _ret) {
            BOOST_CHECK_EQUAL(_error, nullptr);
            BOOST_CHECK_EQUAL(_ret->getField(SYS_VALUE), "world");
            p1.set_value(true);
        });
    BOOST_CHECK(f1.get());
}
BOOST_AUTO_TEST_CASE(testErrorOpenTable)
{
    auto tableFactory = fakeErrorTableFactory();
    BOOST_CHECK_THROW(StorageUtilities::createTables(tableFactory), CreateSysTableFailed);
    BOOST_CHECK_THROW(
        StorageUtilities::syncTableSetter(tableFactory, "errorTable", "error", "error", ""),
        OpenSysTableFailed);

    std::promise<bool> p1;
    auto f1 = p1.get_future();
    StorageUtilities::asyncTableGetter(tableFactory, "errorTable", "row",
        [&](Error::Ptr _error, bcos::storage::Entry::Ptr _value) {
            BOOST_CHECK(_error != nullptr);
            BOOST_CHECK_EQUAL(_value, nullptr);
            p1.set_value(true);
        });
    BOOST_CHECK(f1.get());
}

BOOST_AUTO_TEST_CASE(testDirInfo)
{
    auto f1 = std::make_shared<FileInfo>("test1", "dir", 0);
    auto f2 = std::make_shared<FileInfo>("test2", "dir", 0);
    auto f3 = std::make_shared<FileInfo>("test3", "dir", 0);
    std::vector<FileInfo> v;
    v.emplace_back(*f1);
    v.emplace_back(*f2);
    v.emplace_back(*f3);
    auto d = std::make_shared<DirInfo>(v);

    auto ret = d->toString();
    std::cout << ret << std::endl;
    DirInfo d2;
    BOOST_CHECK_EQUAL(DirInfo::fromString(d2, ret), true);
    BOOST_CHECK_EQUAL(DirInfo::fromString(d2, ret), true);
    BOOST_CHECK_EQUAL(d->getSubDir().at(1).getName(), d2.getSubDir().at(1).getName());
    BOOST_CHECK_EQUAL(d->getSubDir().at(1).getType(), d2.getSubDir().at(1).getType());
    BOOST_CHECK_EQUAL(d->getSubDir().at(1).getNumber(), d2.getSubDir().at(1).getNumber());
    BOOST_CHECK_EQUAL(DirInfo::fromString(d2, "123"), false);

    auto d_e = std::make_shared<DirInfo>();
    auto ret_e = d_e->toString();
    std::cout << ret_e << std::endl;
    DirInfo d2_e;
    BOOST_CHECK_EQUAL(DirInfo::fromString(d2_e, ret_e), true);
}

BOOST_AUTO_TEST_SUITE_END()
}  // namespace bcos::test