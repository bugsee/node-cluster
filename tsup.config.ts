import { defineConfig } from 'tsup';
import { writeCjsEntrypoints } from './scripts/postbuild.mjs';

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
    outExtension: function ({ format }) {
        return format === 'cjs' ? { js: '.internal.cjs' } : { js: '.js' };
    },
    async onSuccess() {
        writeCjsEntrypoints();
    }
});
