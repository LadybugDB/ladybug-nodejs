/**
 * This file is a customized loader for the lbugjs.node native module.
 * It is used to load the native module with the correct flags on Linux so that
 * extension loading works correctly.
 * @module lbug_native
 * @private
 */

const process = require("process");
const constants = require("constants");
const fs = require("fs");
const path = require("path");
const join = path.join;

/**
 * Resolve the path to the native addon (lbugjs.node).
 *
 * Normally the postinstall script (install.js) copies the prebuilt binary
 * from the per-platform sub-package into this package directory.  However,
 * environments that skip lifecycle scripts (e.g. `pnpm dlx`, `pnpx`,
 * `npm install --ignore-scripts`, sandboxed installs) never run install.js,
 * so the in-package copy is missing even though the prebuilt binary is
 * present in the per-platform sub-package.  In that case, fall back to
 * resolving the binary directly from the sub-package using the same logic
 * install.js uses.
 */
function resolveNativeModulePath() {
  const inPackagePath = join(__dirname, "lbugjs.node");
  if (fs.existsSync(inPackagePath)) {
    return inPackagePath;
  }

  try {
    const MAIN_PKG_NAME = require(join(__dirname, "package.json")).name;
    const subPkgName = `${MAIN_PKG_NAME}-${process.platform}-${process.arch}`;
    const subPkgMain = require.resolve(`${subPkgName}/package.json`, {
      paths: [__dirname],
    });
    const subPkgBinaryPath = path.join(path.dirname(subPkgMain), "lbugjs.node");
    if (fs.existsSync(subPkgBinaryPath)) {
      return subPkgBinaryPath;
    }
  } catch (e) {
    // Sub-package not installed (unsupported platform or missing optionalDep);
    // fall through and let dlopen surface the original error.
  }

  return inPackagePath;
}

const lbugNativeModule = { exports: {} };
const modulePath = resolveNativeModulePath();
if (process.platform === "linux") {
  process.dlopen(
    lbugNativeModule,
    modulePath,
    constants.RTLD_LAZY | constants.RTLD_GLOBAL
  );
} else {
  process.dlopen(lbugNativeModule, modulePath);
}

module.exports = lbugNativeModule.exports;
