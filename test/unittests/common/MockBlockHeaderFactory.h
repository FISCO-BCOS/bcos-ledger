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
 * @file MockBlockHeaderFactory.h
 * @author: kyonRay
 * @date 2021-04-14
 */

#pragma once
#include <bcos-framework/interfaces/protocol/BlockHeaderFactory.h>
using namespace bcos;
namespace bcos::protocol
{
class MockBlockHeaderFactory : public BlockHeaderFactory{
public:
    using Ptr = std::shared_ptr<MockBlockHeaderFactory>;
    MockBlockHeaderFactory()= default;
    ~MockBlockHeaderFactory() override {}
    BlockHeader::Ptr createBlockHeader() override {
        return std::make_shared<MockBlockHeader>();
    }
    BlockHeader::Ptr createBlockHeader(const bytes& ) override
    {
        return bcos::protocol::BlockHeader::Ptr();
    }
    BlockHeader::Ptr createBlockHeader(bytesConstRef ) override
    {
        return bcos::protocol::BlockHeader::Ptr();
    }
};
}