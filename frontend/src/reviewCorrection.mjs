export function keepFailedRecordSelection(rows, selectedRow) {
  return rows.some(row => row.row_index === selectedRow)
    ? selectedRow
    : rows[0]?.row_index ?? null;
}

export function correctionRequest(row, columnName, value) {
  return {
    row_index: row.row_index,
    column_name: columnName,
    original_value: row.record?.[columnName] == null ? '' : String(row.record[columnName]),
    value: value.trim(),
  };
}

export async function saveCorrection(rows, row, columnName, value, save) {
  const result = await save(correctionRequest(row, columnName, value));
  return { ...result, rows: updateFailedRecord(rows, row.row_index, result.record) };
}

export async function revalidatedFailedRows(revalidate, readFailedRows) {
  await revalidate();
  return readFailedRows();
}

export function updateFailedRecord(rows, rowIndex, record) {
  return rows.map(row => row.row_index === rowIndex ? { ...row, record } : row);
}

export function unverifiedEmailSuggestion(record) {
  if (!record) return null;
  const emailKey = Object.keys(record).find(key => key.toLowerCase() === 'email');
  const nameKey = Object.keys(record).find(key => key.toLowerCase() === 'name');
  if (!emailKey || !nameKey) return null;
  const email = String(record[emailKey] ?? '').trim();
  if (email && /^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(email)) return null;
  const name = String(record[nameKey] ?? '').trim().normalize('NFKD').replace(/[\u0300-\u036f]/g, '').toLowerCase();
  const parts = name.split(/[^a-z0-9]+/).filter(Boolean);
  if (parts.length < 2) return null;
  return `${parts.join('.')}@gmail.com`;
}

export function correctionKey(suggestion) {
  return `${suggestion.row_index}:${suggestion.column_name}`;
}

export function isSelectableCorrectionSuggestion(suggestion) {
  return suggestion?.suggested_value != null && suggestion.safe_to_apply === true && suggestion.confidence === 'high';
}

export function hasSuggestedCorrection(suggestion) {
  return suggestion?.suggested_value != null;
}

export function filterCorrectionSuggestions(suggestions, filter = 'all', search = '') {
  const term = search.trim().toLowerCase();
  return suggestions
    .filter(suggestion => suggestion.suggested_value != null)
    .filter(suggestion => filter === 'all'
      || (filter === 'safe'
        ? isSelectableCorrectionSuggestion(suggestion)
        : !isSelectableCorrectionSuggestion(suggestion)))
    .filter(suggestion => !term || `${suggestion.row_index} ${suggestion.column_name} ${String(suggestion.current_value ?? '')} ${suggestion.validation_error}`.toLowerCase().includes(term));
}

export function syncSelectedCorrectionKeys(selected, suggestions) {
  const eligible = new Set(suggestions.filter(hasSuggestedCorrection).map(correctionKey));
  const next = [];
  for (const key of selected) {
    if (eligible.has(key) && !next.includes(key)) next.push(key);
  }
  return next.length === selected.length && next.every((key, index) => key === selected[index]) ? selected : next;
}

export function toggleVisibleCorrectionSelection(selected, visibleSuggestions, checked) {
  const visibleEligible = visibleSuggestions.filter(hasSuggestedCorrection).map(correctionKey);
  if (!checked) return selected.filter(key => !visibleEligible.includes(key));
  return Array.from(new Set([...selected, ...visibleEligible]));
}

export function toggleCorrectionSelection(selected, suggestion, checked) {
  const key = correctionKey(suggestion);
  if (!hasSuggestedCorrection(suggestion)) return selected.filter(item => item !== key);
  return checked
    ? Array.from(new Set([...selected, key]))
    : selected.filter(item => item !== key);
}

export function correctionSelectionState(selected, visibleSuggestions) {
  const visibleEligible = visibleSuggestions.filter(hasSuggestedCorrection).map(correctionKey);
  const selectedVisible = visibleEligible.filter(key => selected.includes(key)).length;
  return {
    checked: visibleEligible.length > 0 && selectedVisible === visibleEligible.length,
    indeterminate: selectedVisible > 0 && selectedVisible < visibleEligible.length,
    eligibleCount: visibleEligible.length,
  };
}

export function failedRecordSuggestionSummary(rows, suggestions) {
  const suggestedRows = new Set(suggestions.filter(suggestion => suggestion.suggested_value != null).map(suggestion => suggestion.row_index));
  const manualRows = new Set(suggestions
    .filter(suggestion => suggestion.suggested_value == null || suggestion.generation_status === 'failed')
    .map(suggestion => suggestion.row_index));
  return {
    aiSuggestionRecords: suggestedRows.size,
    manualReviewRecords: rows.filter(row => manualRows.has(row.row_index) || !suggestions.some(suggestion => suggestion.row_index === row.row_index)).length,
  };
}

export function manualCorrectionItems(rows, suggestions, fallbackColumns) {
  const byRow = new Map(rows.map(row => [row.row_index, row]));
  const items = suggestions
    .filter(suggestion => suggestion.suggested_value == null || suggestion.generation_status === 'failed')
    .map(suggestion => {
      const row = byRow.get(suggestion.row_index);
      return row ? { row, column: suggestion.column_name, suggestion } : null;
    })
    .filter(Boolean);
  const keyed = new Set(items.map(item => `${item.row.row_index}:${item.column}`));
  for (const row of rows) {
    if (suggestions.some(suggestion => suggestion.row_index === row.row_index)) continue;
    for (const column of fallbackColumns(row)) {
      const key = `${row.row_index}:${column}`;
      if (!keyed.has(key)) {
        items.push({ row, column, suggestion: null });
        keyed.add(key);
      }
    }
  }
  return items;
}
