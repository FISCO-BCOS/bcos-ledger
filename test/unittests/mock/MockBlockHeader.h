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
#include <bcos-framework/interfaces/protocol/ProtocolTypeDef.h>

namespace bcos::test{
class MockBlockHeader : public bcos::protocol::BlockHeader{
public:
    using Ptr = std::shared_ptr<MockBlockHeader>;
    MockBlockHeader():m_hash(bcos::crypto::HashType(random())),m_number((bcos::protocol::BlockNumber)random()) {}
    virtual ~MockBlockHeader() override {}
    void decode(bytesConstRef ) override { }
    void encode(bytes& ) const override { }
    crypto::HashType const& hash() const override { return m_hash; }
    void populateFromParents(BlockHeadersPtr, bcos::protocol::BlockNumber) override {}
    void clear() override {}
    void verifySignatureList() const override {}
    void populateEmptyBlock(int64_t ) override {}
    int32_t version() const override { return 0; }
    gsl::span<const protocol::ParentInfo> parentInfo() const override
    {
        return gsl::span<const protocol::ParentInfo>();
    }
    const crypto::HashType& txsRoot() const override { return m_txsRoot; }
    const crypto::HashType& receiptRoot() const override { return m_receiptRoot; }
    const crypto::HashType& stateRoot() const override { return m_stateRoot; }
    bcos::protocol::BlockNumber number() const override { return m_number; }
    const u256& gasUsed() override { return m_gasUsed; }
    int64_t timestamp() override { return 0; }
    int64_t sealer() override { return 0; }
    gsl::span<const bytes> sealerList() const override { return gsl::span<const bytes>(); }
    bytesConstRef extraData() const override { return bcos::bytesConstRef(); }
    gsl::span<const protocol::Signature> signatureList() const override
    {
        return gsl::span<const protocol::Signature>();
    }
    gsl::span<const uint64_t> consensusWeights() const override
    {
        return m_consensusWeights;
    }
    void setVersion(int32_t ) override {}
    void setParentInfo(const gsl::span<const protocol::ParentInfo>&) override {}
    void setTxsRoot(const crypto::HashType& _txsRoot) override { m_txsRoot=_txsRoot; }
    void setReceiptRoot(const crypto::HashType& _receiptRoot) override { m_receiptRoot = _receiptRoot; }
    void setStateRoot(const crypto::HashType& _stateRoot) override { m_stateRoot = _stateRoot; }
    void setNumber(bcos::protocol::BlockNumber _number) override { m_number = _number; }
    void setGasUsed(const u256& _gas) override { m_gasUsed = _gas; }
    void setTimestamp(int64_t) override {}
    void setSealer(int64_t ) override {}
    void setSealerList(const gsl::span<const bytes>&) override {}
    void setExtraData(const bytes& _extraData) override { m_extraData = _extraData; }
    void setExtraData(bytes&& _extraData) override { m_extraData = _extraData; }
    void setParentInfo(protocol::ParentInfoList&&) override {}
    void setSealerList(std::vector<bytes>&&) override {}
    void setConsensusWeights(const gsl::span<const uint64_t>&) override {}
    void setConsensusWeights(std::vector<uint64_t>&&) override {}
    void setSignatureList(const gsl::span<const protocol::Signature>& _signatureList) override { m_sigList = _signatureList; }
    void setSignatureList(protocol::SignatureList&& _signatureList) override { m_sigList = _signatureList; }

private:
    bcos::crypto::HashType m_hash;
    bcos::crypto::HashType m_txsRoot;
    bcos::crypto::HashType m_receiptRoot;
    bcos::crypto::HashType m_stateRoot;
    u256 m_gasUsed;
    bytes m_extraData;
    gsl::span<const uint64_t> m_consensusWeights;
    bcos::protocol::BlockNumber m_number = 0;
    gsl::span<const protocol::Signature> m_sigList;
};
} // namespace bcos::protocol
