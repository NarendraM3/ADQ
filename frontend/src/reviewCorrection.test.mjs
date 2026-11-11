import assert from 'node:assert/strict';
import { correctionSelectionState, failedRecordSuggestionSummary, filterCorrectionSuggestions, hasSuggestedCorrection, isSelectableCorrectionSuggestion, keepFailedRecordSelection, manualCorrectionItems, revalidatedFailedRows, saveCorrection, syncSelectedCorrectionKeys, toggleCorrectionSelection, toggleVisibleCorrectionSelection, unverifiedEmailSuggestion } from './reviewCorrection.mjs';

const row2 = { row_index: 1, record: { Name: 'Ada Lovelace', Email: '' }, failure_reasons: ['Email invalid'] };
const row7 = { row_index: 6, record: { Name: 'Grace Hopper', Email: '' }, failure_reasons: ['Email invalid'] };

// Selecting the same row keeps its detail selection; selecting another row switches it.
{
  let selected = keepFailedRecordSelection([row2, row7], row2.row_index);
  assert.equal(selected, 1, 'clicking Row 2 opens its details panel');
  selected = keepFailedRecordSelection([row2, row7], selected);
  assert.equal(selected, 1, 'clicking Row 2 again keeps the panel open');
  selected = keepFailedRecordSelection([row2, row7], row7.row_index);
  assert.equal(selected, 6, 'clicking Row 7 switches the details panel');
}

// A poll refresh preserves the selected record while it remains in the failed set.
assert.equal(keepFailedRecordSelection([row2, row7], 6), 6, 'validation polling does not clear the selected record');

// Suggestions update suggestion data independently from the selected details row.
{
  const selected = 1;
  const suggestions = await Promise.resolve([{ row_index: 1, suggested_value: 'ada.lovelace@gmail.com' }]);
  assert.equal(suggestions[0].row_index, selected, 'AI suggestion loading does not close the details panel');
}

const suggested = unverifiedEmailSuggestion(row2.record);
assert.equal(suggested, 'ada.lovelace@gmail.com', 'missing email produces a draft suggestion');
assert.equal(unverifiedEmailSuggestion({ Name: 'Neha Patel', Email: 'employee324gmail.com' }), 'neha.patel@gmail.com',
  'invalid email is replaced by a readable name-based draft, never an employee-ID address');
assert.equal(unverifiedEmailSuggestion({ Email: 'bad-email' }), null, 'a missing name does not produce an invented identity');
assert.match('AI suggestion · unverified', /unverified/i, 'suggestion is labelled unverified without trusted evidence');

let editedEmail = suggested;
editedEmail = 'ada@example.test';
assert.equal(editedEmail, 'ada@example.test', 'user can edit the suggested email');

const savedRecord = { ...row2.record, Email: 'ada@example.test' };
let apiRequest;
const saved = await saveCorrection([row2, row7], row2, 'Email', ` ${editedEmail} `, async request => {
  apiRequest = request;
  return { record: savedRecord };
});
assert.deepEqual(apiRequest, { row_index: 1, column_name: 'Email', original_value: '', value: 'ada@example.test' },
  'Fix calls the correction API with the correct record ID and edited email');
assert.equal(saved.rows[0].record.Email, 'ada@example.test', 'successful save updates the displayed record');
assert.equal(saved.rows.length, 2, 'successful save keeps the record until backend revalidation');

// A rejected save never mutates the list; only returned backend revalidation rows decide removal.
await assert.rejects(saveCorrection([row2, row7], row2, 'Email', editedEmail, async () => { throw Error('save failed'); }));
assert.equal([row2, row7].length, 2, 'failed save leaves the record in the failed list');
await assert.rejects(revalidatedFailedRows(async () => { throw Error('revalidation failed'); }, async () => []));
const revalidated = await revalidatedFailedRows(async () => ({ status: 'succeeded' }), async () => [row7]);
assert.deepEqual(revalidated.map(row => row.row_index), [6],
  'only successful backend revalidation removes a corrected record from the failed list');

const selectableSuggestion = { row_index: 1, column_name: 'Email', current_value: '', validation_error: 'Email invalid', suggested_value: 'ada@example.test', safe_to_apply: true, confidence: 'high' };
const unsafeSuggestion = { row_index: 2, column_name: 'Email', current_value: '', validation_error: 'Email invalid', suggested_value: 'maybe@example.test', safe_to_apply: false, confidence: 'low' };
const hiddenSuggestion = { row_index: 3, column_name: 'Department', current_value: 'Unknown', validation_error: 'Department invalid', suggested_value: 'Engineering', safe_to_apply: true, confidence: 'high' };

assert.equal(isSelectableCorrectionSuggestion(selectableSuggestion), true, 'safe high-confidence suggestions are eligible for checkbox selection');
assert.equal(isSelectableCorrectionSuggestion(unsafeSuggestion), false, 'unsafe suggestions are not eligible for automatic checkbox selection');
assert.equal(hasSuggestedCorrection(unsafeSuggestion), true, 'unsafe suggestions with a value still have clickable row checkboxes');

{
  const visible = filterCorrectionSuggestions([selectableSuggestion, unsafeSuggestion, hiddenSuggestion], 'all', 'email');
  assert.deepEqual(visible.map(s => s.row_index), [1, 2], 'search limits the header selection scope to visible rows');
  let selected = toggleCorrectionSelection([], unsafeSuggestion, true);
  assert.deepEqual(selected, ['2:Email'], 'individual unsafe row checkbox can be selected');
  selected = toggleCorrectionSelection(selected, unsafeSuggestion, false);
  assert.deepEqual(selected, [], 'individual row checkbox can be deselected independently');
  selected = toggleVisibleCorrectionSelection([], visible, true);
  assert.deepEqual(selected, ['1:Email', '2:Email'], 'header select all chooses every visible suggestion row');
  assert.deepEqual(correctionSelectionState(selected, visible), { checked: true, indeterminate: false, eligibleCount: 2 },
    'header checkbox is checked when every visible suggestion row is selected');
  selected = toggleVisibleCorrectionSelection(selected, visible, false);
  assert.deepEqual(selected, [], 'header checkbox deselects the visible eligible suggestions');
}

{
  const visible = [selectableSuggestion, hiddenSuggestion];
  const selected = ['1:Email'];
  assert.deepEqual(correctionSelectionState(selected, visible), { checked: false, indeterminate: true, eligibleCount: 2 },
    'header checkbox is indeterminate when some visible eligible rows are selected');
  assert.deepEqual(syncSelectedCorrectionKeys(['1:Email', '2:Email', '1:Email'], [selectableSuggestion, unsafeSuggestion]), ['1:Email', '2:Email'],
    'selection stays deduplicated and synchronized with available suggestion data');
}

{
  const failedRows = [
    { row_index: 4, record: { Email: 'bad', Age: '-5' }, failure_reasons: ['Invalid Email', 'Invalid Age'] },
    { row_index: 5, record: { Name: '', Salary: '-10' }, failure_reasons: ['Missing Name', 'Invalid Salary'] },
  ];
  const suggestions = [
    { row_index: 4, column_name: 'Email', suggested_value: 'fixed@example.test', safe_to_apply: false, confidence: 'low', validation_error: 'Invalid Email' },
    { row_index: 4, column_name: 'Age', suggested_value: null, generation_status: 'completed', validation_error: 'Invalid Age', reason: 'Manual review required.' },
    { row_index: 5, column_name: 'Name', suggested_value: null, generation_status: 'completed', validation_error: 'Missing Name', reason: 'No evidence for name.' },
  ];
  assert.deepEqual(failedRecordSuggestionSummary(failedRows, suggestions), { aiSuggestionRecords: 1, manualReviewRecords: 2 },
    'dashboard counts records once while preserving manual-review records');
  const manual = manualCorrectionItems(failedRows, suggestions, row => Object.keys(row.record));
  assert.deepEqual(manual.map(item => `${item.row.row_index}:${item.column}`), ['4:Age', '5:Name'],
    'manual panel is field-aware and keeps rows with mixed AI and manual failures');
}
