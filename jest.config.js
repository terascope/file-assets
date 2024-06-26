import path from 'node:path';
import { fileURLToPath } from 'node:url';

const dirname = path.dirname(fileURLToPath(import.meta.url));

export default {
    verbose: true,
    testEnvironment: 'node',
    setupFilesAfterEnv: ['jest-extended/all', '<rootDir>/test/test.setup.js'],
    collectCoverage: true,
    coverageReporters: ['json', 'lcov', 'text', 'html'],
    coverageDirectory: 'coverage',
    testTimeout: 60 * 1000,
    collectCoverageFrom: [
        '<rootDir>/asset/**/*.ts',
        '!<rootDir>/packages/*/**/*.ts',
        '!<rootDir>/packages/*/test/**',
        '!<rootDir>/**/coverage/**',
        '!<rootDir>/**/*.d.ts',
        '!<rootDir>/**/dist/**',
        '!<rootDir>/**/coverage/**'
    ],
    testMatch: [
        '<rootDir>/test/**/*-spec.{ts,js}',
        '<rootDir>/test/*-spec.{ts,js}',
    ],
    moduleNameMapper: {
        '^@terascope/file-asset-apis$': path.join(dirname, '/packages/file-asset-apis/src/index.ts'),
        '^(\\.{1,2}/.*)\\.js$': '$1',
    },
    preset: 'ts-jest',
    extensionsToTreatAsEsm: ['.ts'],
    transform: {
        '\\.[jt]sx?$': ['ts-jest', {
            isolatedModules: true,
            tsconfig: './tsconfig.json',
            diagnostics: true,
            pretty: true,
            useESM: true
        }]
    },
    globals: {
        ignoreDirectories: ['dist'],
        availableExtensions: ['.js', '.ts', '.mjs']
    }
};
