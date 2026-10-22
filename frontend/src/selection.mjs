// A source import is the complete selection for that source step in this workflow.
export function replacementSelection(datasets, connectionId) {
  const ids = datasets.map((dataset) => dataset.id);
  return {
    datasetIds: ids,
    sourceConnectionIds: Object.fromEntries(ids.map((id) => [String(id), connectionId])),
  };
}
