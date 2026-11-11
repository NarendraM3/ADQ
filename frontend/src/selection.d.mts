export function replacementSelection(
  datasets: Array<{ id: number }>,
  connectionId: number,
): { datasetIds: number[]; sourceConnectionIds: Record<string, number> };
export function toggleDatasetSelection(datasetIds: number[], id: number, checked: boolean): number[];
export function mergeDatasets<T extends { id: number }>(existingDatasets: T[], selectedDatasets: T[]): T[];
export function buildJobConfiguration(input: {
  name: string;
  description: string;
  datasetIds: number[];
  datasetNames: string[];
  selectedColumns: Record<number, string[]>;
  [key: string]: unknown;
}): Record<string, unknown>;
