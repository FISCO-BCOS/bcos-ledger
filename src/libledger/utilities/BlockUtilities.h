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
 * @file BlockUtilities.h
 * @author: kyonRay
 * @date 2021-05-06
 */

#pragma once
#include "Common.h"
#include <bcos-framework/interfaces/ledger/LedgerTypeDef.h>
#include <bcos-framework/interfaces/protocol/BlockFactory.h>

namespace bcos::ledger
{
class BlockFetcher
{
public:
    using Ptr = std::shared_ptr<BlockFetcher>;
    explicit BlockFetcher(const protocol::Block::Ptr& _block) { m_block = _block; }
    protocol::Block::Ptr block() const { return m_block; }
    void setHeaderFetched(bool _headerFetched) { m_headerFetched = _headerFetched; }
    void setTxsFetched(bool _txsFetched) { m_txsFetched = _txsFetched; }
    void setReceiptsFetched(bool _receiptsFetched) { m_receiptsFetched = _receiptsFetched; }
    void setFetchFlag(int32_t _flag) { m_flag = _flag; }
    bool isFetchFinished()
    {
        if (m_flag < 0)
        {
            return false;
        }
        bool headerFlag = true;
        bool txsFlag = true;
        bool receiptFlag = true;
        if (m_flag & ledger::HEADER)
        {
            headerFlag = m_headerFetched;
        }
        if (m_flag & ledger::TRANSACTIONS)
        {
            txsFlag = m_txsFetched;
        }
        if (m_flag & ledger::RECEIPTS)
        {
            receiptFlag = m_receiptsFetched;
        }
        return headerFlag && txsFlag && receiptFlag;
    }

private:
    protocol::Block::Ptr m_block;
    int32_t m_flag = -1;
    std::atomic_bool m_headerFetched = {false};
    std::atomic_bool m_txsFetched = {false};
    std::atomic_bool m_receiptsFetched = {false};
};
protocol::TransactionsPtr blockTransactionListGetter(const protocol::Block::Ptr& _block);

std::shared_ptr<std::vector<std::string>> blockTxHashListGetter(const protocol::Block::Ptr& _block);

int blockTransactionListSetter(
    const protocol::Block::Ptr& _block, const protocol::TransactionsPtr& _txs);

protocol::ReceiptsPtr blockReceiptListGetter(const protocol::Block::Ptr& _block);

int blockReceiptListSetter(
    const protocol::Block::Ptr& _block, const protocol::ReceiptsPtr& _receipts);

bcos::protocol::Block::Ptr decodeBlock(
    const protocol::BlockFactory::Ptr& _blockFactory, const std::string& _blockStr);

bcos::protocol::BlockHeader::Ptr decodeBlockHeader(
    const protocol::BlockHeaderFactory::Ptr& _headerFactory, const std::string& _headerStr);

bcos::protocol::Transaction::Ptr decodeTransaction(
    const protocol::TransactionFactory::Ptr& _txFactory, const std::string& _txStr);

bcos::protocol::TransactionReceipt::Ptr decodeReceipt(
    const protocol::TransactionReceiptFactory::Ptr& _receiptFactory,
    const std::string& _receiptStr);
}  // namespace bcos::ledger
