import assert from 'node:assert/strict';
import { replacementSelection } from './selection.mjs';

const localMarketDataId = 10;
const importedEmployee = { id: 27, source_type: 's3', filename: 'employee.csv' };
const selection = replacementSelection([importedEmployee], 31);

assert.deepEqual(selection.datasetIds, [27]);
assert.deepEqual(selection.sourceConnectionIds, { '27': 31 });
assert.equal(selection.datasetIds.includes(localMarketDataId), false);
console.log('Workflow source replacement assertions passed.');
