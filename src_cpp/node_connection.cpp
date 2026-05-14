#include "include/node_connection.h"

#include <iostream>

#include "include/node_database.h"
#include "include/node_query_result.h"
#include "include/node_util.h"
#include "main/lbug.h"
#include "storage/table/arrow_table_support.h"

namespace {

template<typename T>
T* GetPointerArgument(const Napi::Value& value, const char* name) {
    if (value.IsBigInt()) {
        bool lossless = false;
        auto address = value.As<Napi::BigInt>().Uint64Value(&lossless);
        if (!lossless || address == 0) {
            throw std::runtime_error(std::string(name) + " must be a non-zero pointer BigInt.");
        }
        return reinterpret_cast<T*>(static_cast<uintptr_t>(address));
    }
    if (value.IsExternal()) {
        return value.As<Napi::External<T>>().Data();
    }
    throw std::runtime_error(std::string(name) + " must be a pointer BigInt or N-API External.");
}

ArrowSchemaWrapper TakeArrowSchema(ArrowSchema* schema) {
    ArrowSchemaWrapper wrapper;
    static_cast<ArrowSchema&>(wrapper) = *schema;
    schema->release = nullptr;
    return wrapper;
}

ArrowArrayWrapper TakeArrowArray(ArrowArray* array) {
    ArrowArrayWrapper wrapper;
    static_cast<ArrowArray&>(wrapper) = *array;
    array->release = nullptr;
    return wrapper;
}

std::vector<ArrowArrayWrapper> TakeArrowArrays(const Napi::Value& value, uint64_t numArrays) {
    std::vector<ArrowArrayWrapper> wrappers;
    if (value.IsArray()) {
        auto arrayPointers = value.As<Napi::Array>();
        wrappers.reserve(arrayPointers.Length());
        for (auto i = 0u; i < arrayPointers.Length(); ++i) {
            wrappers.push_back(TakeArrowArray(
                GetPointerArgument<ArrowArray>(arrayPointers.Get(i), "ArrowArray")));
        }
        return wrappers;
    }
    auto* arrays = GetPointerArgument<ArrowArray>(value, "arrays");
    wrappers.reserve(numArrays);
    for (auto i = 0u; i < numArrays; ++i) {
        wrappers.push_back(TakeArrowArray(&arrays[i]));
    }
    return wrappers;
}

void AdoptArrowQueryResult(Napi::Env env, NodeQueryResult* nodeQueryResult,
    std::unique_ptr<QueryResult> result, std::shared_ptr<Connection> connection,
    std::shared_ptr<Database> database) {
    if (result == nullptr) {
        throw std::runtime_error("Arrow table registration did not return a query result.");
    }
    if (!result->isSuccess()) {
        Napi::Error::New(env, result->getErrorMessage()).ThrowAsJavaScriptException();
        return;
    }
    nodeQueryResult->AdoptQueryResult(std::move(result), std::move(connection), std::move(database));
}

} // namespace

Napi::Object NodeConnection::Init(Napi::Env env, Napi::Object exports) {
    Napi::HandleScope scope(env);

    Napi::Function t = DefineClass(env, "NodeConnection",
        {InstanceMethod("initAsync", &NodeConnection::InitAsync),
            InstanceMethod("initSync", &NodeConnection::InitSync),
            InstanceMethod("executeAsync", &NodeConnection::ExecuteAsync),
            InstanceMethod("queryAsync", &NodeConnection::QueryAsync),
            InstanceMethod("executeSync", &NodeConnection::ExecuteSync),
            InstanceMethod("querySync", &NodeConnection::QuerySync),
            InstanceMethod("createArrowTableSync", &NodeConnection::CreateArrowTableSync),
            InstanceMethod("createArrowRelTableSync", &NodeConnection::CreateArrowRelTableSync),
            InstanceMethod("dropArrowTableSync", &NodeConnection::DropArrowTableSync),
            InstanceMethod("setMaxNumThreadForExec", &NodeConnection::SetMaxNumThreadForExec),
            InstanceMethod("setQueryTimeout", &NodeConnection::SetQueryTimeout),
            InstanceMethod("close", &NodeConnection::Close)});

    exports.Set("NodeConnection", t);
    return exports;
}

NodeConnection::NodeConnection(const Napi::CallbackInfo& info)
    : Napi::ObjectWrap<NodeConnection>(info) {
    Napi::Env env = info.Env();
    Napi::HandleScope scope(env);
    NodeDatabase* nodeDatabase = Napi::ObjectWrap<NodeDatabase>::Unwrap(info[0].As<Napi::Object>());
    database = nodeDatabase->database;
}

Napi::Value NodeConnection::InitAsync(const Napi::CallbackInfo& info) {
    Napi::Env env = info.Env();
    Napi::HandleScope scope(env);
    auto callback = info[0].As<Napi::Function>();
    auto* asyncWorker = new ConnectionInitAsyncWorker(callback, this);
    asyncWorker->Queue();
    return info.Env().Undefined();
}

Napi::Value NodeConnection::InitSync(const Napi::CallbackInfo& info) {
    Napi::Env env = info.Env();
    Napi::HandleScope scope(env);
    try {
        InitCppConnection();
    } catch (const std::exception& exc) {
        Napi::Error::New(env, exc.what()).ThrowAsJavaScriptException();
    }
    return env.Undefined();
}

void NodeConnection::InitCppConnection() {
    this->connection = std::make_shared<Connection>(database.get());
    ProgressBar::Get(*connection->getClientContext())
        ->setDisplay(std::make_shared<NodeProgressBarDisplay>());
}

void NodeConnection::SetMaxNumThreadForExec(const Napi::CallbackInfo& info) {
    Napi::Env env = info.Env();
    Napi::HandleScope scope(env);
    size_t numThreads = info[0].ToNumber().Int64Value();
    try {
        this->connection->setMaxNumThreadForExec(numThreads);
    } catch (const std::exception& exc) {
        Napi::Error::New(env, std::string(exc.what())).ThrowAsJavaScriptException();
    }
}

void NodeConnection::SetQueryTimeout(const Napi::CallbackInfo& info) {
    Napi::Env env = info.Env();
    Napi::HandleScope scope(env);
    size_t timeout = info[0].ToNumber().Int64Value();
    try {
        this->connection->setQueryTimeOut(timeout);
    } catch (const std::exception& exc) {
        Napi::Error::New(env, std::string(exc.what())).ThrowAsJavaScriptException();
    }
}

void NodeConnection::Close(const Napi::CallbackInfo& info) {
    Napi::Env env = info.Env();
    Napi::HandleScope scope(env);
    this->connection.reset();
    this->database.reset();
}

Napi::Value NodeConnection::ExecuteAsync(const Napi::CallbackInfo& info) {
    Napi::Env env = info.Env();
    Napi::HandleScope scope(env);
    auto nodePreparedStatement =
        Napi::ObjectWrap<NodePreparedStatement>::Unwrap(info[0].As<Napi::Object>());
    auto nodeQueryResult = Napi::ObjectWrap<NodeQueryResult>::Unwrap(info[1].As<Napi::Object>());
    auto callback = info[3].As<Napi::Function>();
    try {
        auto params = Util::TransformParametersForExec(info[2].As<Napi::Array>());
        auto asyncWorker = new ConnectionExecuteAsyncWorker(callback, connection, database,
            nodePreparedStatement->preparedStatement, nodeQueryResult, std::move(params), info[4]);
        asyncWorker->Queue();
    } catch (const std::exception& exc) {
        Napi::Error::New(env, std::string(exc.what())).ThrowAsJavaScriptException();
    }
    return info.Env().Undefined();
}

Napi::Value NodeConnection::QuerySync(const Napi::CallbackInfo& info) {
    Napi::Env env = info.Env();
    Napi::HandleScope scope(env);
    auto statement = info[0].As<Napi::String>().Utf8Value();
    auto nodeQueryResult = Napi::ObjectWrap<NodeQueryResult>::Unwrap(info[1].As<Napi::Object>());
    try {
        auto result = connection->query(statement);
        if (!result->isSuccess()) {
            Napi::Error::New(env, result->getErrorMessage()).ThrowAsJavaScriptException();
        }
        nodeQueryResult->AdoptQueryResult(std::move(result), connection, database);
    } catch (const std::exception& exc) {
        Napi::Error::New(env, std::string(exc.what())).ThrowAsJavaScriptException();
    }
    return env.Undefined();
}

Napi::Value NodeConnection::ExecuteSync(const Napi::CallbackInfo& info) {
    Napi::Env env = info.Env();
    Napi::HandleScope scope(env);
    auto nodePreparedStatement =
        Napi::ObjectWrap<NodePreparedStatement>::Unwrap(info[0].As<Napi::Object>());
    auto nodeQueryResult = Napi::ObjectWrap<NodeQueryResult>::Unwrap(info[1].As<Napi::Object>());
    try {
        auto params = Util::TransformParametersForExec(info[2].As<Napi::Array>());
        auto result = connection->executeWithParams(nodePreparedStatement->preparedStatement.get(),
            std::move(params));
        if (!result->isSuccess()) {
            Napi::Error::New(env, result->getErrorMessage()).ThrowAsJavaScriptException();
        }
        nodeQueryResult->AdoptQueryResult(std::move(result), connection, database);
    } catch (const std::exception& exc) {
        Napi::Error::New(env, std::string(exc.what())).ThrowAsJavaScriptException();
    }
    return env.Undefined();
}

Napi::Value NodeConnection::QueryAsync(const Napi::CallbackInfo& info) {
    Napi::Env env = info.Env();
    Napi::HandleScope scope(env);
    auto statement = info[0].As<Napi::String>().Utf8Value();
    auto nodeQueryResult = Napi::ObjectWrap<NodeQueryResult>::Unwrap(info[1].As<Napi::Object>());
    auto callback = info[2].As<Napi::Function>();
    auto asyncWorker =
        new ConnectionQueryAsyncWorker(
            callback, connection, database, statement, nodeQueryResult, info[3]);
    asyncWorker->Queue();
    return info.Env().Undefined();
}

Napi::Value NodeConnection::CreateArrowTableSync(const Napi::CallbackInfo& info) {
    Napi::Env env = info.Env();
    Napi::HandleScope scope(env);
    auto tableName = info[0].As<Napi::String>().Utf8Value();
    auto* schema = GetPointerArgument<ArrowSchema>(info[1], "schema");
    auto numArrays = info[3].As<Napi::Number>().Int64Value();
    auto nodeQueryResult = Napi::ObjectWrap<NodeQueryResult>::Unwrap(info[4].As<Napi::Object>());
    try {
        auto result = lbug::ArrowTableSupport::createViewFromArrowTable(*connection, tableName,
            TakeArrowSchema(schema), TakeArrowArrays(info[2], numArrays));
        AdoptArrowQueryResult(
            env, nodeQueryResult, std::move(result.queryResult), connection, database);
    } catch (const std::exception& exc) {
        Napi::Error::New(env, std::string(exc.what())).ThrowAsJavaScriptException();
    }
    return env.Undefined();
}

Napi::Value NodeConnection::CreateArrowRelTableSync(const Napi::CallbackInfo& info) {
    Napi::Env env = info.Env();
    Napi::HandleScope scope(env);
    auto tableName = info[0].As<Napi::String>().Utf8Value();
    auto srcTableName = info[1].As<Napi::String>().Utf8Value();
    auto dstTableName = info[2].As<Napi::String>().Utf8Value();
    auto* schema = GetPointerArgument<ArrowSchema>(info[3], "schema");
    auto numArrays = info[5].As<Napi::Number>().Int64Value();
    auto nodeQueryResult = Napi::ObjectWrap<NodeQueryResult>::Unwrap(info[6].As<Napi::Object>());
    try {
        auto result = lbug::ArrowTableSupport::createRelTableFromArrowTable(*connection, tableName,
            srcTableName, dstTableName, TakeArrowSchema(schema), TakeArrowArrays(info[4], numArrays));
        AdoptArrowQueryResult(
            env, nodeQueryResult, std::move(result.queryResult), connection, database);
    } catch (const std::exception& exc) {
        Napi::Error::New(env, std::string(exc.what())).ThrowAsJavaScriptException();
    }
    return env.Undefined();
}

Napi::Value NodeConnection::DropArrowTableSync(const Napi::CallbackInfo& info) {
    Napi::Env env = info.Env();
    Napi::HandleScope scope(env);
    auto tableName = info[0].As<Napi::String>().Utf8Value();
    auto nodeQueryResult = Napi::ObjectWrap<NodeQueryResult>::Unwrap(info[1].As<Napi::Object>());
    try {
        auto result = lbug::ArrowTableSupport::unregisterArrowTable(*connection, tableName);
        AdoptArrowQueryResult(env, nodeQueryResult, std::move(result), connection, database);
    } catch (const std::exception& exc) {
        Napi::Error::New(env, std::string(exc.what())).ThrowAsJavaScriptException();
    }
    return env.Undefined();
}
