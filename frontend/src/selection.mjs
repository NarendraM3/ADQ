// A source import is the complete selection for that source step in this workflow.
export function replacementSelection(datasets, connectionId) {
  const ids = datasets.map((dataset) => dataset.id);
  return {
    datasetIds: ids,
    sourceConnectionIds: Object.fromEntries(ids.map((id) => [String(id), connectionId])),
  };
}

export function toggleDatasetSelection(datasetIds, id, checked) {
  const withoutCurrent = datasetIds.filter((datasetId) => datasetId !== id);
  return checked ? [...withoutCurrent, id] : withoutCurrent;
}

export function mergeDatasets(existingDatasets, selectedDatasets) {
  const byId = new Map(existingDatasets.map((dataset) => [dataset.id, dataset]));
  selectedDatasets.forEach((dataset) => byId.set(dataset.id, dataset));
  return [...byId.values()];
}

export function buildJobConfiguration({ name, description, datasetIds, datasetNames, selectedColumns, ...configuration }) {
  const trimmedName = typeof name === 'string' ? name.trim() : '';
  return {
    ...configuration,
    name: trimmedName,
    description: typeof description === 'string' && description.trim() ? description.trim() : null,
    dataset_ids: datasetIds,
    selected_columns: Object.fromEntries(datasetIds.map((id) => [String(id), selectedColumns[id] || []])),
  };
}
