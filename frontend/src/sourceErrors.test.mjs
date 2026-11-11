import assert from 'node:assert/strict';
import { humanSourceError } from './sourceErrors.mjs';

const detail = { code: 'NO_TABLES_FOUND', message: "No tables are available in 'chosen-catalog.chosen-schema'." };
assert.equal(humanSourceError({ detail }), `NO_TABLES_FOUND: ${detail.message}`);
assert.equal(humanSourceError(new Error(humanSourceError({ detail }))), `NO_TABLES_FOUND: ${detail.message}`);
