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

namespace bcos::test{
class MockBlockHeader : public bcos::protocol::BlockHeader{
public:
    using Ptr = std::shared_ptr<MockBlockHeader>;
    MockBlockHeader():m_hash(bcos::crypto::HashType(random())),m_number((bcos::protocol::BlockNumber)random()) {}
    virtual ~MockBlockHeader() override {}
    void decode(bytesConstRef ) override { }
    void encode(bytes& ) const override { }
    const crypto::HashType& hash() const override { return m_hash; }
    void populateFromParents(BlockHeadersPtr, bcos::protocol::BlockNumber) override {}
    void clear() override {}
    void verifySignatureList() const override {}
    void populateEmptyBlock(int64_t ) override {}
    int32_t version() const override { return 0; }
    bcos::protocol::ParentInfoListPtr parentInfo() const override { return bcos::protocol::ParentInfoListPtr(); }
    const crypto::HashType& txsRoot() const override { return m_txsRoot; }
    const crypto::HashType& receiptRoot() const override { return m_receiptRoot; }
    const crypto::HashType& stateRoot() const override { return m_stateRoot; }
    bcos::protocol::BlockNumber number() const override { return m_number; }
    const u256& gasUsed() override { return m_gasUsed; }
    int64_t timestamp() override { return 0; }
    int64_t sealer() override { return 0; }
    bcos::protocol::BytesListPtr sealerList() const override { return bcos::protocol::BytesListPtr(); }
    const bytes& extraData() const override { return m_extraData; }
    bcos::protocol:: SignatureListPtr signatureList() const override { return bcos::protocol::SignatureListPtr(); }
    const bcos::protocol::WeightList& consensusWeights() const override { return *m_consensusWeights; }
    void setVersion(int32_t ) override {}
    void setParentInfo(bcos::protocol::ParentInfoListPtr ) override {}
    void setTxsRoot(const crypto::HashType& _txsRoot) override { m_txsRoot=_txsRoot; }
    void setReceiptRoot(const crypto::HashType& _receiptRoot) override { m_receiptRoot = _receiptRoot; }
    void setStateRoot(const crypto::HashType& _stateRoot) override { m_stateRoot = _stateRoot; }
    void setNumber(bcos::protocol::BlockNumber _number) override { m_number = _number; }
    void setGasUsed(const u256& _gas) override { m_gasUsed = _gas; }
    void setTimestamp(const int64_t& ) override {}
    void setSealer(int64_t ) override {}
    void setSealerList(const bcos::protocol::BytesList& ) override {}
    void setConsensusWeights(bcos::protocol::WeightListPtr ) override {}
    void setExtraData(const bytes& _extraData) override { m_extraData = _extraData; }
    void setExtraData(bytes&& _extraData) override { m_extraData = _extraData; }
    void setSignatureList(bcos::protocol::SignatureListPtr _sigList) override { m_sigList = _sigList; }

private:
    bcos::crypto::HashType m_hash;
    bcos::crypto::HashType m_txsRoot;
    bcos::crypto::HashType m_receiptRoot;
    bcos::crypto::HashType m_stateRoot;
    u256 m_gasUsed;
    bytes m_extraData;
    bcos::protocol::WeightListPtr m_consensusWeights;
    bcos::protocol::BlockNumber m_number = 0;
    bcos::protocol::SignatureListPtr m_sigList;
};
} // namespace bcos::protocol
