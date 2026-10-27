#include "include/node_database.h"

Napi::Object NodeDatabase::Init(Napi::Env env, Napi::Object exports) {
    Napi::HandleScope scope(env);

    Napi::Function t = DefineClass(env, "NodeDatabase",
        {
            InstanceMethod("initAsync", &NodeDatabase::InitAsync),
            InstanceMethod("initSync", &NodeDatabase::InitSync),
            InstanceMethod("close", &NodeDatabase::Close),
            StaticMethod("getVersion", &NodeDatabase::GetVersion),
            StaticMethod("getStorageVersion", &NodeDatabase::GetStorageVersion),
        });

    exports.Set("NodeDatabase", t);
    return exports;
}

namespace {

// Null/undefined means "use the engine default"; the setting is left unset.
bool getOptionalInt64(const Napi::CallbackInfo& info, size_t index, int64_t& out) {
    if (index >= info.Length() || info[index].IsNull() || info[index].IsUndefined()) {
        return false;
    }
    out = info[index].As<Napi::Number>().Int64Value();
    return true;
}

bool getOptionalBool(const Napi::CallbackInfo& info, size_t index, bool& out) {
    if (index >= info.Length() || info[index].IsNull() || info[index].IsUndefined()) {
        return false;
    }
    out = info[index].As<Napi::Boolean>().Value();
    return true;
}

} // namespace

NodeDatabase::NodeDatabase(const Napi::CallbackInfo& info) : Napi::ObjectWrap<NodeDatabase>(info) {
    Napi::Env env = info.Env();
    Napi::HandleScope scope(env);
    databasePath = info[0].ToString();
    int64_t intValue = 0;
    bool boolValue = false;
    if (getOptionalInt64(info, 1, intValue)) {
        bufferPoolSize = intValue;
    }
    if (getOptionalBool(info, 2, boolValue)) {
        enableCompression = boolValue;
    }
    if (getOptionalBool(info, 3, boolValue)) {
        readOnly = boolValue;
    }
    if (getOptionalInt64(info, 4, intValue)) {
        maxDBSize = intValue;
    }
    if (getOptionalBool(info, 5, boolValue)) {
        autoCheckpoint = boolValue;
    }
    if (getOptionalInt64(info, 6, intValue)) {
        checkpointThreshold = intValue;
    }
    if (getOptionalBool(info, 7, boolValue)) {
        throwOnWalReplayFailure = boolValue;
    }
    if (getOptionalBool(info, 8, boolValue)) {
        enableChecksums = boolValue;
    }
    if (getOptionalBool(info, 9, boolValue)) {
        enableDefaultHashIndex = boolValue;
    }
}

Napi::Value NodeDatabase::InitSync(const Napi::CallbackInfo& info) {
    Napi::Env env = info.Env();
    Napi::HandleScope scope(env);
    try {
        InitCppDatabase();
    } catch (const std::exception& exc) {
        Napi::Error::New(env, exc.what()).ThrowAsJavaScriptException();
    }
    return env.Undefined();
}

Napi::Value NodeDatabase::InitAsync(const Napi::CallbackInfo& info) {
    Napi::Env env = info.Env();
    Napi::HandleScope scope(env);

    auto callback = info[0].As<Napi::Function>();

    auto* asyncWorker = new DatabaseInitAsyncWorker(callback, this);
    asyncWorker->Queue();
    return info.Env().Undefined();
}

void NodeDatabase::InitCppDatabase() {
    // Start from the engine defaults and only apply explicitly set settings,
    // so engine-side default changes propagate automatically.
    auto systemConfig = SystemConfig();
    if (bufferPoolSize && *bufferPoolSize > 0) {
        systemConfig.bufferPoolSize = static_cast<uint64_t>(*bufferPoolSize);
    }
    if (enableCompression) {
        systemConfig.enableCompression = *enableCompression;
    }
    if (readOnly) {
        systemConfig.readOnly = *readOnly;
    }
    if (maxDBSize && *maxDBSize > 0) {
        systemConfig.maxDBSize = static_cast<uint64_t>(*maxDBSize);
    }
    if (autoCheckpoint) {
        systemConfig.autoCheckpoint = *autoCheckpoint;
    }
    if (throwOnWalReplayFailure) {
        systemConfig.throwOnWalReplayFailure = *throwOnWalReplayFailure;
    }
    if (enableChecksums) {
        systemConfig.enableChecksums = *enableChecksums;
    }
    if (enableDefaultHashIndex) {
        systemConfig.enableDefaultHashIndex = *enableDefaultHashIndex;
    }
    if (checkpointThreshold && *checkpointThreshold >= 0) {
        systemConfig.checkpointThreshold = static_cast<uint64_t>(*checkpointThreshold);
    }
    this->database = std::make_shared<Database>(databasePath, systemConfig);
}

void NodeDatabase::Close(const Napi::CallbackInfo& info) {
    Napi::Env env = info.Env();
    Napi::HandleScope scope(env);
    database.reset();
}

Napi::Value NodeDatabase::GetVersion(const Napi::CallbackInfo& info) {
    Napi::Env env = info.Env();
    Napi::HandleScope scope(env);
    return Napi::String::New(env, Version::getVersion());
}

Napi::Value NodeDatabase::GetStorageVersion(const Napi::CallbackInfo& info) {
    Napi::Env env = info.Env();
    Napi::HandleScope scope(env);
    return Napi::Number::New(env, Version::getStorageVersion());
}
