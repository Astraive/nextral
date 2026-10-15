'use strict';

const { spawnSync } = require('node:child_process');
const { mkdirSync } = require('node:fs');
const { resolve } = require('node:path');

const cwd = resolve(__dirname, '..');
const metadata = spawnSync('cargo', ['metadata', '--no-deps', '--format-version', '1'], {
  cwd, encoding: 'utf8',
});
if (metadata.error) throw metadata.error;
if (metadata.status !== 0) {
  process.stderr.write(metadata.stderr);
  process.exit(metadata.status ?? 1);
}
if (metadata.stderr) process.stderr.write(metadata.stderr);

// fix(build): napi v2 does not resolve Cargo config target-dir itself.
const target = JSON.parse(metadata.stdout).target_directory;
const temporary = resolve(target, 'tmp');
mkdirSync(temporary, { recursive: true });
const result = spawnSync(process.execPath, [
  require.resolve('@napi-rs/cli/scripts/index.js'),
  'build', 'native', '--platform', '--js', 'false', '--dts', 'native.d.ts',
  '--cargo-flags=--locked', ...process.argv.slice(2),
], { cwd, env: { ...process.env, CARGO_TARGET_DIR: target, TEMP: temporary, TMP: temporary, TMPDIR: temporary }, stdio: 'inherit' });
if (result.error) throw result.error;
process.exit(result.status ?? 1);
