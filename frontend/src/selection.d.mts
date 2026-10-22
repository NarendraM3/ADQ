export function replacementSelection(
  datasets: Array<{ id: number }>,
  connectionId: number,
): { datasetIds: number[]; sourceConnectionIds: Record<string, number> };
