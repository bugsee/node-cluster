import { appendFileSync } from 'node:fs';
import { defineConfig } from 'tsup';

/** tsup's CJS emit is `{ default, ...named }`. Make require() the callable default. */
const CJS_CALLABLE_DEFAULT = [
    '',
    'const __exp = module.exports;',
    'if (__exp && __exp.default) {',
    '  module.exports = Object.assign(__exp.default, __exp);',
    '  module.exports.default = __exp.default;',
    '}',
    ''
].join('\n');

export default defineConfig({
    entry: ['src/index.ts'],
    format: ['esm', 'cjs'],
    dts: true,
    sourcemap: true,
    clean: true,
    treeshake: true,
    target: 'node18',
    platform: 'node',
    outDir: 'dist',
    splitting: false,
    cjsInterop: true,
    removeNodeProtocol: false,
    async onSuccess() {
        appendFileSync('dist/index.cjs', CJS_CALLABLE_DEFAULT);
    }
});
