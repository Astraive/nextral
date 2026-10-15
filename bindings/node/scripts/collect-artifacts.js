'use strict';

const { readdirSync, mkdirSync, copyFileSync, constants } = require('node:fs');
const { join, resolve } = require('node:path');

const source = process.argv[2];
if (!source) throw new Error('Usage: npm run artifacts:collect -- PATH_TO_DOWNLOADED_ARTIFACTS');
const destination = resolve(__dirname, '..', 'native');
const allowed = new Set([
  'nextral.win32-x64-msvc.node', 'nextral.darwin-x64.node', 'nextral.darwin-arm64.node',
  'nextral.linux-x64-gnu.node', 'nextral.linux-arm64-gnu.node',
]);
mkdirSync(destination, { recursive: true });
function collect(directory) {
  for (const entry of readdirSync(directory, { withFileTypes: true })) {
    const path = join(directory, entry.name);
    if (entry.isDirectory()) collect(path);
    else if (entry.isFile() && allowed.has(entry.name)) {
      copyFileSync(path, join(destination, entry.name), constants.COPYFILE_EXCL);
    }
  }
}
collect(resolve(source));
