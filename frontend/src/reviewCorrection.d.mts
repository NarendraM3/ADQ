export function keepFailedRecordSelection<T extends { row_index: number }>(rows: T[], selectedRow: number | null): number | null;
export function correctionRequest(row: { row_index: number; record?: Record<string, unknown> }, columnName: string, value: string): {
  row_index: number;
  column_name: string;
  original_value: string;
  value: string;
};
export function updateFailedRecord<T extends { row_index: number; record?: Record<string, unknown> }>(rows: T[], rowIndex: number, record: Record<string, unknown>): T[];
export function saveCorrection<T extends { row_index: number; record?: Record<string, unknown> }, R extends { record: Record<string, unknown> }>(rows: T[], row: T, columnName: string, value: string, save: (request: ReturnType<typeof correctionRequest>) => Promise<R>): Promise<R & { rows: T[] }>;
export function revalidatedFailedRows<T>(revalidate: () => Promise<unknown>, readFailedRows: () => Promise<T>): Promise<T>;
export function unverifiedEmailSuggestion(record: Record<string, unknown> | undefined): string | null;
export function correctionKey(suggestion: { row_index: number; column_name: string }): string;
export function isSelectableCorrectionSuggestion(suggestion: { suggested_value?: unknown; safe_to_apply?: boolean; confidence?: string } | null | undefined): boolean;
export function hasSuggestedCorrection(suggestion: { suggested_value?: unknown } | null | undefined): boolean;
export function filterCorrectionSuggestions<T extends { row_index: number; column_name: string; current_value?: unknown; validation_error?: string; suggested_value?: unknown; safe_to_apply?: boolean; confidence?: string }>(suggestions: T[], filter?: string, search?: string): T[];
export function syncSelectedCorrectionKeys<T extends { row_index: number; column_name: string; suggested_value?: unknown; safe_to_apply?: boolean; confidence?: string }>(selected: string[], suggestions: T[]): string[];
export function toggleVisibleCorrectionSelection<T extends { row_index: number; column_name: string; suggested_value?: unknown; safe_to_apply?: boolean; confidence?: string }>(selected: string[], visibleSuggestions: T[], checked: boolean): string[];
export function toggleCorrectionSelection<T extends { row_index: number; column_name: string; suggested_value?: unknown }>(selected: string[], suggestion: T, checked: boolean): string[];
export function correctionSelectionState<T extends { row_index: number; column_name: string; suggested_value?: unknown; safe_to_apply?: boolean; confidence?: string }>(selected: string[], visibleSuggestions: T[]): {
  checked: boolean;
  indeterminate: boolean;
  eligibleCount: number;
};
export function failedRecordSuggestionSummary<T extends { row_index: number }, S extends { row_index: number; suggested_value?: unknown; generation_status?: string }>(rows: T[], suggestions: S[]): {
  aiSuggestionRecords: number;
  manualReviewRecords: number;
};
export function manualCorrectionItems<T extends { row_index: number }, S extends { row_index: number; column_name: string; suggested_value?: unknown; generation_status?: string }>(rows: T[], suggestions: S[], fallbackColumns: (row: T) => string[]): Array<{
  row: T;
  column: string;
  suggestion: S | null;
}>;
