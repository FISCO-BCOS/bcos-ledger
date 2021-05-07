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
 * @file StorageGetterTest.cpp
 * @author: kyonRay
 * @date 2021-05-06
 */

#include "unittests/ledger/common/FakeTable.h"
#include "unittests/ledger/common/FakeBlock.h"
#include "bcos-ledger/ledger/storage/StorageGetter.h"
#include <bcos-test/libutils/TestPromptFixture.h>
#include <boost/test/unit_test.hpp>

using namespace bcos;
using namespace bcos::ledger;
using namespace bcos::protocol;

namespace bcos::test
{
BOOST_FIXTURE_TEST_SUITE(StorageGetterTest, TestPromptFixture)
BOOST_AUTO_TEST_CASE(testTableGetterByRowAndField)
{
    auto tableFactory = fakeTableFactory(0);
    auto storageGetter =  StorageGetter::storageGetterFactory();
    auto ret = storageGetter->tableGetterByRowAndField(tableFactory, "_sys_config_", "", "");
    std::cout << ret << std::endl;
}

BOOST_AUTO_TEST_SUITE_END()
}