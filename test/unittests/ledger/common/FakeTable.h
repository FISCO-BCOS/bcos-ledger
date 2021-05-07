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
 * @file FakeTable.h
 * @author: kyonRay
 * @date 2021-05-06
 */
#pragma once

#include "unittests/mock/MockStorage.h"
#include "bcos-framework/libtable/TableFactory.h"
#include "bcos-framework/libtable/Table.h"
#include "bcos-test/libutils/HashImpl.h"

namespace bcos::test{
inline storage::TableFactory::Ptr fakeTableFactory(protocol::BlockNumber _blockNumber){
    auto hashImpl = std::make_shared<Keccak256Hash>();
    auto storage = std::make_shared<MockStorage>();
    auto tableFactory = std::make_shared<TableFactory>(storage,hashImpl, _blockNumber);
    return tableFactory;
}
}