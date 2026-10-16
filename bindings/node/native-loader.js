'use strict';

const { join } = require('node:path');

let native;

function platformSuffix() {
  const { platform, arch } = process;
  if (platform === 'win32' && arch === 'x64') return 'win32-x64-msvc';
  if (platform === 'darwin' && (arch === 'x64' || arch === 'arm64')) return `darwin-${arch}`;
  if (platform === 'linux' && (arch === 'x64' || arch === 'arm64')) {
    // Node's own runtime report avoids executing programs found on PATH.
    if (process.report.getReport().header.glibcVersionRuntime) return `linux-${arch}-gnu`;
    throw new Error('Nextral supports glibc Linux only; musl/Alpine is not supported.');
  }
  throw new Error(`Unsupported Nextral platform: ${platform}/${arch}.`);
}

function loadNative() {
  if (native) return native;
  const suffix = platformSuffix();
  const filename = `nextral.${suffix}.node`;
  try {
    native = require(join(__dirname, 'native', filename));
    return native;
  } catch (cause) {
    throw new Error(`Cannot load Nextral native artifact ${filename}. Install an official package containing this platform, or build it from the source checkout with npm run build.`, { cause });
  }
}

module.exports = { loadNative, platformSuffix };
