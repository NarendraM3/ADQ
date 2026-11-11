import assert from 'node:assert/strict';
import { buildJobConfiguration, mergeDatasets, replacementSelection, toggleDatasetSelection } from './selection.mjs';

const localMarketDataId = 10;
const importedEmployee = { id: 27, source_type: 's3', filename: 'employee.csv' };
const selection = replacementSelection([importedEmployee], 31);

assert.deepEqual(selection.datasetIds, [27]);
assert.deepEqual(selection.sourceConnectionIds, { '27': 31 });
assert.equal(selection.datasetIds.includes(localMarketDataId), false);

const columns = ['Employee ID', 'First Name', 'Last Name', 'Gender', 'Age', 'Email',
  'Department', 'Designation', 'Salary', 'Joining Date', 'Phone', 'Location'];
const job = buildJobConfiguration({
  name: '  ', description: '  ', datasetIds: [27], datasetNames: ['Team roster'],
  selectedColumns: { 27: columns }, join_config: null,
});
assert.equal(job.name, '');
assert.equal(job.description, null);
assert.deepEqual(job.dataset_ids, [27]);
assert.deepEqual(job.selected_columns, { '27': columns });

assert.deepEqual(toggleDatasetSelection([27, 31], 27, false), [31]);
assert.deepEqual(toggleDatasetSelection([27], 31, true), [27, 31]);
assert.deepEqual(toggleDatasetSelection([27, 31], 31, true), [27, 31]);
assert.deepEqual(mergeDatasets([{ id: 27, name: 'Old' }, { id: 31, name: 'Kept' }], [{ id: 27, name: 'Updated' }]), [{ id: 27, name: 'Updated' }, { id: 31, name: 'Kept' }]);
console.log('Workflow source replacement assertions passed.');
