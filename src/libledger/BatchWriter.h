#pragma once
#include "utilities/Common.h"
#include <bcos-framework/interfaces/storage/StorageInterface.h>
#include <bcos-framework/interfaces/storage/Table.h>
#include <bcos-framework/libutilities/Error.h>
#include <boost/exception/diagnostic_information.hpp>
#include <string>
#include <variant>

namespace bcos::ledger
{
struct WriteItem
{
    WriteItem(std::variant<std::string_view, std::string> _tableName,
        std::variant<std::string_view, std::string> _key)
      : tableName(std::move(_tableName)), key(std::move(_key))
    {}

    std::variant<std::string_view, std::string> tableName;
    std::variant<std::string_view, std::string> key;
    std::vector<std::variant<std::string_view, std::string>> values;
};

inline WriteItem& operator<<(WriteItem& item,
    std::tuple<std::variant<std::string_view, std::string>,
        std::variant<std::string_view, std::string>>
        value)
{
    item.values.emplace_back(std::move(value));
    return item;
}

inline std::string_view toView(const std::variant<std::string_view, std::string>& v)
{
    if (v.index() == 0)
    {
        return std::get<0>(v);
    }
    else
    {
        return std::get<1>(v);
    }
}

inline std::string toString(std::variant<std::string_view, std::string>& v)
{
    if (v.index() == 0)
    {
        return std::string(std::get<0>(v));
    }
    else
    {
        return std::move(std::get<1>(v));
    }
}

inline void writeTo(bcos::storage::StorageInterface::Ptr storage, gsl::span<WriteItem> batch,
    std::function<void(Error::Ptr&& error, size_t)> callback)
{
    auto totalTables = std::make_shared<std::atomic<size_t>>(batch.size());
    auto hadError = std::make_shared<std::atomic<bool>>(false);
    auto tableReducer = [callback, totalTables, hadError, total = batch.size()](
                            Error::Ptr&& error) {
        --(*totalTables);
        if (error)
        {
            *hadError = true;
            LEDGER_LOG(ERROR) << "Process table failed" << boost::diagnostic_information(*error);
        }

        if (*totalTables == 0)
        {
            if (hadError)
            {
                LEDGER_LOG(ERROR) << "Batch write to failed with error";
                callback(nullptr, 0);
                return;
            }

            callback(nullptr, total);
        }
    };

    auto batchPtr = std::make_shared<gsl::span<WriteItem>>(std::move(batch));

    for (auto& it : *batchPtr)
    {
        storage->asyncOpenTable(
            toView(it.tableName), [tableReducer, batchPtr, &it](Error::Ptr&& error,
                                      std::optional<bcos::storage::Table>&& table) {
                if (error)
                {
                    LEDGER_LOG(ERROR)
                        << "Open table failed" << boost::diagnostic_information(*error);
                    tableReducer(std::move(error));
                    return;
                }

                if (!table)
                {
                    tableReducer(BCOS_ERROR_PTR(-1, "Table doesn't exists"));
                    return;
                }

                auto totalEntries = std::make_shared<std::atomic<size_t>>(it.values.size());
                auto hadError = std::make_shared<std::atomic<bool>>(false);
                auto entryReducer = [tableReducer, totalEntries, hadError](Error::Ptr&& error) {
                    --(*totalEntries);

                    if (error)
                    {
                        *hadError = true;
                        LEDGER_LOG(ERROR)
                            << "Set entry failed" << boost::diagnostic_information(*error);
                    }

                    if (*totalEntries == 0)
                    {
                        if (*hadError)
                        {
                            tableReducer(BCOS_ERROR_PTR(-1, "Set entry failed!"));
                            return;
                        }

                        tableReducer(nullptr);
                    }
                };

                auto entry = table->newEntry();
                for (auto& fieldIt : it.values)
                {
                    entry.setField(toView(std::get<0>(fieldIt)), toString(std::get<1>(fieldIt)));
                }

                table->asyncSetRow(toView(it.key), std::move(entry),
                    [entryReducer](Error::Ptr&& error, bool success) {
                        if (error)
                        {
                            entryReducer(std::move(error));
                        }
                        else
                        {
                            if (success)
                            {
                                entryReducer(nullptr);
                            }
                            else
                            {
                                entryReducer(BCOS_ERROR_PTR(-1, "Set row failed"));
                            }
                        }
                    });
            });
    }
}
}  // namespace bcos::ledger