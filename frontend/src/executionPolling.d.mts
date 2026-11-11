export function executionTotal(run: { status: string; passed_count?: number; failed_count?: number } | null | undefined): number | null;

export function pollExecution(options: {
  executionId: number;
  getExecution: (id: number) => Promise<any>;
  getFailed: (id: number) => Promise<any>;
  onUpdate: (result: any, details?: any[]) => void;
  onError: (error: unknown, details: boolean) => void;
  isCurrent?: () => boolean;
  delay?: (ms: number) => Promise<void>;
}): () => void;
