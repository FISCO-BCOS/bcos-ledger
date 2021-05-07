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
 * @file FakeBlockHeader.h
 * @author: kyonRay
 * @date 2021-04-14
 */

#pragma once
#include "../../mock/MockBlockHeaderFactory.h"
#include <bcos-framework/libprotocol/Exceptions.h>
#include <bcos-framework/libprotocol/protobuf/PBBlockHeaderFactory.h>
#include <bcos-framework/libutilities/Common.h>
#include <boost/test/unit_test.hpp>

inline bcos::protocol::BlockHeader::Ptr fakeBlockHeader(
    bcos::crypto::HashType const& _txsRoot, bcos::crypto::HashType const& _receiptRoot,bcos::crypto::HashType const& _stateRoot,
    int64_t _number, u256 const& _gasUsed, bytes const& _extraData)
{
    bcos::protocol::BlockHeaderFactory::Ptr blockHeaderFactory =
        std::make_shared<bcos::test::MockBlockHeaderFactory>();
    bcos::protocol::BlockHeader::Ptr blockHeader = blockHeaderFactory->createBlockHeader();
    blockHeader->setTxsRoot(_txsRoot);
    blockHeader->setReceiptRoot(_receiptRoot);
    blockHeader->setStateRoot(_stateRoot);
    blockHeader->setNumber(_number);
    blockHeader->setGasUsed(_gasUsed);
    blockHeader->setExtraData(_extraData);
    return blockHeader;
}

inline bcos::protocol::BlockHeader::Ptr getBlockHeader()
{
    auto txsRoot = bcos::crypto::HashType(1);
    auto receiptRoot = bcos::crypto::HashType(2);
    auto stateRoot = bcos::crypto::HashType(3);
    bcos::protocol::BlockNumber number = rand();
    u256 gasUsed = 3453456346534;
    bytes extraData = stateRoot.asBytes();

    auto blockHeader =
        fakeBlockHeader(txsRoot, receiptRoot, stateRoot, number, gasUsed, extraData);
    return blockHeader;
}
