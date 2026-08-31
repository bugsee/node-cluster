'use strict';

module.exports = {
    extends: 'airbnb-base',

    parserOptions: {
        sourceType: 'script',
        ecmaVersion: 13
    },

    env: {
        node: true,
        es6: true,
        mocha: true
    },

    rules: {
        quotes: ['error', 'single'],
        'import/extensions': [0],
        indent: ['error', 4, { SwitchCase: 1 }],
        'comma-dangle': ['error', 'never'],
        strict: ['error', 'global'],
        'no-underscore-dangle': ['error', { allow: ['_id', '__v'] }],
        'max-len': ['error', 120, { ignoreTrailingComments: true }],
        'linebreak-style': ['off'],
        'no-restricted-syntax': 0,
        'no-else-return': ['off'],
        'max-classes-per-file': ['off'],
        'function-paren-newline': ['off'],
        'function-call-argument-newline': ['off'],
        'prefer-arrow-callback': ['off'],
        'object-shorthand': ['off'],
        'prefer-object-spread': ['off'],
        'prefer-destructuring': ['off'],
        'prefer-template': ['off'],
        'func-names': ['off'],
        'spaced-comment': ['error', 'always', { markers: ['/', '#region'], exceptions: ['#region', '#endregion'] }],
        'arrow-parens': ['off', 'always'],
        'no-multi-spaces': ['off'],
        camelcase: ['off'],
        'no-multiple-empty-lines': ['off'],
        'object-curly-newline': ['off'],
        'import/order': ['off'],
        'operator-linebreak': ['off']
    },

    overrides: [
        {
            files: ['src/**/*.ts', 'types/**/*.ts'],
            parser: '@typescript-eslint/parser',
            parserOptions: { ecmaVersion: 2022, sourceType: 'module' },
            plugins: ['@typescript-eslint'],
            rules: {
                'no-unused-vars': 'off',
                'no-undef': 'off',
                'no-use-before-define': 'off',
                'no-redeclare': 'off',
                '@typescript-eslint/no-redeclare': 'error',
                'import/no-unresolved': 'off',
                'import/extensions': 'off',
                'lines-between-class-members': 'off',
                strict: 'off',
                'import/prefer-default-export': 'off'
            }
        },
        {
            files: ['types/**/*.ts'],
            parser: '@typescript-eslint/parser',
            parserOptions: { ecmaVersion: 2022, sourceType: 'module' },
            plugins: ['@typescript-eslint'],
            rules: {
                'no-void': 'off',
                'no-new': 'off',
                'import/first': 'off',
                'import/no-named-as-default-member': 'off',
                'no-unused-vars': 'off',
                'no-undef': 'off'
            }
        }
    ]
};
