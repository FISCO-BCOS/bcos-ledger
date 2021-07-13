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
 * @file StorageUtilities.h
 * @author: kyonRay
 * @date 2021-07-14
 */

#pragma once

#include "bcos-framework/interfaces/consensus/ConsensusNode.h"
#include "bcos-framework/interfaces/protocol/Block.h"
#include "bcos-framework/interfaces/protocol/BlockFactory.h"
#include "bcos-framework/interfaces/protocol/BlockHeader.h"
#include "bcos-framework/interfaces/protocol/BlockHeaderFactory.h"
#include "bcos-framework/interfaces/protocol/Transaction.h"
#include "bcos-framework/interfaces/protocol/TransactionReceipt.h"
#include "bcos-framework/interfaces/storage/TableInterface.h"
#include <bcos-framework/interfaces/ledger/LedgerTypeDef.h>
#include <boost/archive/text_iarchive.hpp>
#include <boost/archive/text_oarchive.hpp>
#include <boost/serialization/string.hpp>
#include <boost/serialization/vector.hpp>

namespace bcos::ledger
{
DERIVE_BCOS_EXCEPTION(OpenSysTableFailed);
DERIVE_BCOS_EXCEPTION(CreateSysTableFailed);
class FileInfo
{
public:
    FileInfo() = default;
    FileInfo(const std::string& name, const std::string& type, protocol::BlockNumber number)
      : name(name), type(type), number(number)
    {}
    const std::string& getName() const { return name; }
    const std::string& getType() const { return type; }
    protocol::BlockNumber getNumber() const { return number; }

private:
    friend class boost::serialization::access;
    template <typename Archive>
    void serialize(Archive& ar, const unsigned int)
    {
        ar& name;
        ar& type;
        ar& number;
    }
    std::string name;
    std::string type;
    protocol::BlockNumber number;
};
class DirInfo
{
public:
    DirInfo() = default;
    explicit DirInfo(const std::vector<FileInfo>& subDir) : subDir(subDir) {}
    const std::vector<FileInfo>& getSubDir() const { return subDir; }
    std::vector<FileInfo>& getMutableSubDir() { return subDir; }
    std::string toString();
    static bool fromString(DirInfo& _dir, std::string _str);
    static std::string emptyDirString()
    {
        DirInfo emptyDir;
        return emptyDir.toString();
    }

private:
    friend class boost::serialization::access;
    template <typename Archive>
    void serialize(Archive& ar, const unsigned int)
    {
        ar& subDir;
    }
    std::vector<FileInfo> subDir;
};

class StorageUtilities final
{
public:
    using Ptr = std::shared_ptr<StorageUtilities>;

    static void createTables(const storage::TableFactoryInterface::Ptr& _tableFactory);

    static void createFileSystemTables(const storage::TableFactoryInterface::Ptr& _tableFactory);
    static void recursiveBuildDir(
        const storage::TableFactoryInterface::Ptr& _tableFactory, const std::string& _absoluteDir);
    /**
     * @brief update tableName set fieldName=fieldValue where row=_row
     * @param _tableFactory
     * @param _tableName
     * @param _row
     * @param _fieldName
     * @param _fieldValue
     * @return return update result
     */
    static bool syncTableSetter(const bcos::storage::TableFactoryInterface::Ptr& _tableFactory,
        const std::string& _tableName, const std::string& _row, const std::string& _fieldName,
        const std::string& _fieldValue);

    static bool checkTableExist(const std::string& _tableName,
        const bcos::storage::TableFactoryInterface::Ptr& _tableFactory);
    /**
     * @brief select field from tableName where row=_row
     * @param _tableFactory
     * @param _tableName
     * @param _row
     * @param _field
     * @param _onGetEntry callback when get entry in db
     */
    static void asyncTableGetter(const bcos::storage::TableFactoryInterface::Ptr& _tableFactory,
        const std::string& _tableName, std::string _row,
        std::function<void(Error::Ptr, bcos::storage::Entry::Ptr)> _onGetEntry);
};
}  // namespace bcos::ledger
