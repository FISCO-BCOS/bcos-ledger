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
       m_receiptsHash(std::make_shared<bcos::protocol::HashList>()),
        m_transactions(std::make_shared<bcos::protocol::Transactions>()),
        m_receipts(std::make_shared<bcos::protocol::Receipts>())
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
    bcos::protocol::Transaction::ConstPtr transaction(size_t ) const override
    {
        return bcos::protocol::Transaction::ConstPtr();
    }
    bcos::protocol::TransactionReceipt::ConstPtr receipt(size_t ) const override
    {
        return bcos::protocol::TransactionReceipt::ConstPtr();
    }
    const crypto::HashType& transactionHash(size_t _index) const override { return (*m_transactionsHash)[_index]; }
    const crypto::HashType& receiptHash(size_t _index) const override { return (*m_receiptsHash)[_index]; }
    void setBlockType(bcos::protocol::BlockType ) override {}
    void setBlockHeader(bcos::protocol::BlockHeader::Ptr _header) override {m_blockHeader = _header;}
    void setTransaction(size_t , bcos::protocol::Transaction::Ptr ) override {}
    void appendTransaction(bcos::protocol::Transaction::Ptr _transaction) override
    {
        m_transactions->push_back(_transaction);
    }
    void setReceipt(size_t , bcos::protocol::TransactionReceipt::Ptr ) override {}
    void appendReceipt(bcos::protocol::TransactionReceipt::Ptr _receipt) override
    {
        m_receipts->push_back(_receipt);
    }
    void setTransactionHash(size_t , const crypto::HashType& ) override {}
    void appendTransactionHash(const crypto::HashType& _txHash ) override {
        m_transactionsHash->push_back(_txHash);
    }
    void setReceiptHash(size_t , const crypto::HashType& ) override {}
    void appendReceiptHash(const crypto::HashType& _receiptHash) override {
        m_receiptsHash->push_back(_receiptHash);
    }
    bcos::protocol::NonceListPtr nonces() override { return bcos::protocol::NonceListPtr(); }
    size_t transactionsSize() override { return m_transactions->size(); }
    size_t transactionsHashSize() override { return m_transactionsHash->size(); }
    size_t receiptsSize() override { return m_receipts->size(); }
    size_t receiptsHashSize() override { return m_receiptsHash->size(); }

private:
    bcos::protocol::BlockHeader::Ptr m_blockHeader;
    bcos::protocol::HashListPtr m_transactionsHash;
    bcos::protocol::HashListPtr m_receiptsHash;
    bcos::protocol::TransactionsPtr m_transactions;
    bcos::protocol::ReceiptsPtr m_receipts;
};
}