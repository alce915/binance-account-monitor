import { defineConfig } from 'vitest/config';

export default defineConfig({
  test: {
    include: ['tests/frontend/**/*.test.js'],
    pool: 'threads',
    poolOptions: {
      threads: {
        singleThread: true,
      },
    },
  },
});
