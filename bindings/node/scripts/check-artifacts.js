'use strict';

const { existsSync } = require('node:fs');
const { join } = require('node:path');
const { platformSuffix } = require('../native-loader');

const suffixes = process.argv.includes('--all')
  ? ['win32-x64-msvc', 'darwin-x64', 'darwin-arm64', 'linux-x64-gnu', 'linux-arm64-gnu']
  : [platformSuffix()];
const missing = suffixes.filter(suffix => !existsSync(join(__dirname, '..', 'native', `nextral.${suffix}.node`)));
if (missing.length) {
  throw new Error(`Missing native artifacts: ${missing.join(', ')}. Build or collect release artifacts before packing.`);
}
