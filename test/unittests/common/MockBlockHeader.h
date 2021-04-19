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
 * @file MockBlockHeader.h
 * @author: kyonRay
 * @date 2021-04-14
 */

#pragma once

#include <bcos-framework/interfaces/protocol/BlockHeader.h>
using namespace bcos;

namespace bcos::protocol{
class MockBlockHeader : public BlockHeader{
public:
    using Ptr = std::shared_ptr<MockBlockHeader>;
    MockBlockHeader() {}
    virtual ~MockBlockHeader() override {}
    void decode(bytesConstRef ) override { }
    void encode(bytes& ) const override { }
    const crypto::HashType& hash() const override { return m_hash; }
    void populateFromParents(BlockHeadersPtr, BlockNumber) override {}
    void clear() override {}
    void verifySignatureList() const override {}
    void populateEmptyBlock(int64_t ) override {}
    int32_t version() const override { return 0; }
    ParentInfoListPtr parentInfo() const override { return bcos::protocol::ParentInfoListPtr(); }
    const crypto::HashType& txsRoot() const override { return m_txsRoot; }
    const crypto::HashType& receiptRoot() const override { return m_receiptRoot; }
    const crypto::HashType& stateRoot() const override { return m_stateRoot; }
    BlockNumber number() const override { return m_number; }
    const u256& gasUsed() override { return m_gasUsed; }
    int64_t timestamp() override { return 0; }
    int64_t sealer() override { return 0; }
    BytesListPtr sealerList() const override { return bcos::protocol::BytesListPtr(); }
    const bytes& extraData() const override { return m_extraData; }
    SignatureListPtr signatureList() const override { return bcos::protocol::SignatureListPtr(); }
    const WeightList& consensusWeights() const override { return *m_consensusWeights; }
    void setVersion(int32_t ) override {}
    void setParentInfo(ParentInfoListPtr ) override {}
    void setTxsRoot(const crypto::HashType& ) override {}
    void setReceiptRoot(const crypto::HashType& ) override {}
    void setStateRoot(const crypto::HashType& ) override {}
    void setNumber(BlockNumber ) override {}
    void setGasUsed(const u256& ) override {}
    void setTimestamp(const int64_t& ) override {}
    void setSealer(int64_t ) override {}
    void setSealerList(const BytesList& ) override {}
    void setConsensusWeights(WeightListPtr ) override {}
    void setExtraData(const bytes& ) override {}
    void setExtraData(bytes&& ) override {}
    void setSignatureList(SignatureListPtr ) override {}

private:
    bcos::crypto::HashType m_hash = bcos::crypto::HashType(random());
    bcos::crypto::HashType m_txsRoot;
    bcos::crypto::HashType m_receiptRoot;
    bcos::crypto::HashType m_stateRoot;
    u256 m_gasUsed;
    bytes m_extraData;
    WeightListPtr m_consensusWeights;
    BlockNumber m_number = 0;
};
} // namespace bcos::protocol
