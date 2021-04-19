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
 * @file MockBlockFactory.h
 * @author: kyonRay
 * @date 2021-04-14
 */

#pragma once

#include <bcos-framework/interfaces/protocol/BlockFactory.h>
#include <bcos-framework/interfaces/protocol/BlockHeaderFactory.h>

#include <utility>
#include "unittests/common/MockBlock.h"

using namespace bcos;
namespace bcos::protocol{
class MockBlockFactory : public BlockFactory{
public:
    using Ptr = std::shared_ptr<MockBlockFactory>;
    MockBlockFactory() = default;
    explicit MockBlockFactory(BlockHeaderFactory::Ptr _blockHeaderFactory) : BlockFactory(), m_blockHeaderFactory(std::move(_blockHeaderFactory))
    {
        assert(m_blockHeaderFactory);
    }
    virtual ~MockBlockFactory() override {}
    Block::Ptr createBlock() override {
        return std::make_shared<MockBlock>();
    }
    Block::Ptr createBlock(const bytes& , bool , bool ) override { return Block::Ptr(); }

private:
    BlockHeaderFactory::Ptr m_blockHeaderFactory;
};

} // namespace bcos::protocol
