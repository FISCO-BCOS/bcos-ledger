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
 * @file MockBlock.h
 * @author: kyonRay
 * @date 2021-04-14
 */

#pragma once

#include <bcos-framework/interfaces/protocol/Block.h>
#include "MockBlockHeader.h"

using namespace bcos;

namespace bcos::test {
class MockBlock : public bcos::protocol::Block{
public:
    MockBlock()
      :m_blockHeader(std::make_shared<bcos::test::MockBlockHeader>()),
       m_transactionsHash(std::make_shared<bcos::protocol::HashList>()),
       m_receiptsHash(std::make_shared<bcos::protocol::HashList>())
    {
        assert(m_transactionsHash);
        assert(m_receiptsHash);
    }
    ~MockBlock() override = default;
    void decode(bytesConstRef , bool , bool ) override {}
    void encode(bytes& ) const override {}
    crypto::HashType calculateTransactionRoot(bool ) const override
    {
        return bcos::crypto::HashType();
    }
    crypto::HashType calculateReceiptRoot(bool ) const override
    {
        return bcos::crypto::HashType();
    }
    int32_t version() const override { return 0; }
    void setVersion(int32_t ) override {}
    bcos::protocol::BlockType blockType() const override { return bcos::protocol::CompleteBlock; }
    bcos::protocol::BlockHeader::Ptr blockHeader() override { return m_blockHeader; }
    bcos::protocol::TransactionsConstPtr transactions() const override { return bcos::protocol::TransactionsConstPtr(); }
    bcos::protocol::Transaction::ConstPtr transaction(size_t ) const override
    {
        return bcos::protocol::Transaction::ConstPtr();
    }
    bcos::protocol::ReceiptsConstPtr receipts() const override { return bcos::protocol::ReceiptsConstPtr(); }
    bcos::protocol::TransactionReceipt::ConstPtr receipt(size_t ) const override
    {
        return bcos::protocol::TransactionReceipt::ConstPtr();
    }
    bcos::protocol::HashListConstPtr transactionsHash() const override { return m_transactionsHash; }
    const crypto::HashType& transactionHash(size_t _index) const override { return (*m_transactionsHash)[_index]; }
    bcos::protocol::HashListConstPtr receiptsHash() const override { return m_receiptsHash; }
    const crypto::HashType& receiptHash(size_t _index) const override { return (*m_receiptsHash)[_index]; }
    void setBlockType(bcos::protocol::BlockType ) override {}
    void setBlockHeader(bcos::protocol::BlockHeader::Ptr _header) override {m_blockHeader = _header;}
    void setTransactions(bcos::protocol::TransactionsPtr ) override {}
    void setTransaction(size_t , bcos::protocol::Transaction::Ptr ) override {}
    void appendTransaction(bcos::protocol::Transaction::Ptr ) override {}
    void setReceipts(bcos::protocol::ReceiptsPtr ) override {}
    void setReceipt(size_t , bcos::protocol::TransactionReceipt::Ptr ) override {}
    void appendReceipt(bcos::protocol::TransactionReceipt::Ptr ) override {}
    void setTransactionsHash(bcos::protocol::HashListPtr ) override {}
    void setTransactionHash(size_t , const crypto::HashType& ) override {}
    void appendTransactionHash(const crypto::HashType& _txHash ) override {
        m_transactionsHash->push_back(_txHash);
    }
    void setReceiptsHash(bcos::protocol::HashListPtr ) override {}
    void setReceiptHash(size_t , const crypto::HashType& ) override {}
    void appendReceiptHash(const crypto::HashType& _receiptHash) override {
        m_receiptsHash->push_back(_receiptHash);
    }
    bcos::protocol::NonceListPtr nonces() override { return bcos::protocol::NonceListPtr(); }
    size_t transactionsSize() override { return 0; }
    size_t transactionsHashSize() override { return m_transactionsHash->size(); }
    size_t receiptsSize() override { return 0; }
    size_t receiptsHashSize() override { return m_receiptsHash->size(); }

private:
    bcos::protocol::BlockHeader::Ptr m_blockHeader;
    bcos::protocol::HashListPtr m_transactionsHash;
    bcos::protocol::HashListPtr m_receiptsHash;
};
}