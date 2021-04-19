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
#include <bcos-framework/libprotocol/Exceptions.h>
#include <bcos-framework/libprotocol/protobuf/PBBlockHeaderFactory.h>
#include <bcos-framework/libutilities/Common.h>
#include <boost/test/unit_test.hpp>
using namespace bcos;
using namespace bcos::protocol;
using namespace bcos::crypto;

inline BlockHeader::Ptr fakeBlockHeader(
    h256 const& _txsRoot, h256 const& _receiptRoot,h256 const& _stateRoot,
    int64_t _number, u256 const& _gasUsed, int64_t _timestamp, bytes const& _extraData)
{
    BlockHeaderFactory::Ptr blockHeaderFactory =
        std::make_shared<MockBlockHeaderFactory>();
    BlockHeader::Ptr blockHeader = blockHeaderFactory->createBlockHeader();
    blockHeader->setTxsRoot(_txsRoot);
    blockHeader->setReceiptRoot(_receiptRoot);
    blockHeader->setStateRoot(_stateRoot);
    blockHeader->setNumber(_number);
    blockHeader->setGasUsed(_gasUsed);
    blockHeader->setTimestamp(_timestamp);
    blockHeader->setExtraData(_extraData);
    WeightListPtr weights = std::make_shared<WeightList>();
    weights->push_back(0);
    return blockHeader;
}

inline BlockHeader::Ptr getBlockHeader()
{
    auto txsRoot = h256(1);
    auto receiptRoot = h256(2);
    auto stateRoot = h256(3);
    int64_t number = 12312312412;
    u256 gasUsed = 3453456346534;
    int64_t timestamp = 9234234234;
    bytes extraData = stateRoot.asBytes();

    auto blockHeader =
        fakeBlockHeader(txsRoot, receiptRoot, stateRoot, number, gasUsed, timestamp, extraData);
    return blockHeader;
}
