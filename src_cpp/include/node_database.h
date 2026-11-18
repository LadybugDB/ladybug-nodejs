#pragma once

#include <memory>
#include <optional>

#include "main/lbug.h"
#include <napi.h>

using namespace lbug::main;

// A native object that keeps a Database alive (a connection or a query result). The registry
// lets NodeDatabase::Close release the ones that are still around, including those whose JS
// wrapper was garbage-collected but not finalized yet, so that the database is destroyed on
// close() instead of whenever the last finalizer runs.
class DatabaseDependent {
public:
    virtual ~DatabaseDependent() = default;
    static void Register(Database* database, DatabaseDependent* dependent);
    static void Unregister(Database* database, DatabaseDependent* dependent);
    static void ReleaseAll(Database* database);

private:
    virtual void ReleaseDatabase() = 0;
};

class NodeDatabase : public Napi::ObjectWrap<NodeDatabase> {
    friend class NodeConnection;
    friend class DatabaseInitAsyncWorker;

public:
    static Napi::Object Init(Napi::Env env, Napi::Object exports);
    explicit NodeDatabase(const Napi::CallbackInfo& info);
    ~NodeDatabase() override = default;

private:
    Napi::Value InitAsync(const Napi::CallbackInfo& info);
    Napi::Value InitSync(const Napi::CallbackInfo& info);
    void InitCppDatabase();
    void Close(const Napi::CallbackInfo& info);
    static Napi::Value GetVersion(const Napi::CallbackInfo& info);
    static Napi::Value GetStorageVersion(const Napi::CallbackInfo& info);

private:
    std::string databasePath;
    std::optional<int64_t> bufferPoolSize;
    std::optional<bool> enableCompression;
    std::optional<bool> readOnly;
    std::optional<int64_t> maxDBSize;
    std::optional<bool> autoCheckpoint;
    std::optional<int64_t> checkpointThreshold;
    std::optional<bool> throwOnWalReplayFailure;
    std::optional<bool> enableChecksums;
    std::optional<bool> enableDefaultHashIndex;
    std::shared_ptr<Database> database;
};

class DatabaseInitAsyncWorker : public Napi::AsyncWorker {
public:
    DatabaseInitAsyncWorker(Napi::Function& callback, NodeDatabase* nodeDatabase)
        : AsyncWorker(callback), nodeDatabase(nodeDatabase) {}

    ~DatabaseInitAsyncWorker() override = default;

    inline void Execute() override {
        try {
            nodeDatabase->InitCppDatabase();

        } catch (const std::exception& exc) {
            SetError(std::string(exc.what()));
        }
    }

    inline void OnOK() override { Callback().Call({Env().Null()}); }

    inline void OnError(Napi::Error const& error) override { Callback().Call({error.Value()}); }

private:
    NodeDatabase* nodeDatabase;
};
