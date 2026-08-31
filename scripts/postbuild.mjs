import { copyFileSync, existsSync, unlinkSync, writeFileSync } from 'node:fs';
import { resolve } from 'node:path';
import { fileURLToPath } from 'node:url';

const CJS_WRAPPER = [
    '\'use strict\';',
    'const m = require(\'./index.internal.cjs\');',
    'module.exports = Object.assign(m.default, m);',
    'module.exports.default = module.exports;',
    ''
].join('\n');

export function writeCjsEntrypoints() {
    writeFileSync('dist/index.cjs', CJS_WRAPPER);
    copyFileSync('src/require.d.cts', 'dist/index.d.cts');
    ['dist/index.internal.d.cts', 'dist/index.internal.d.cts.map'].forEach(function (extra) {
        if (existsSync(extra)) {
            unlinkSync(extra);
        }
    });
}

const invokedDirectly = Boolean(
    process.argv[1] && fileURLToPath(import.meta.url) === resolve(process.argv[1])
);
if (invokedDirectly) {
    writeCjsEntrypoints();
}
