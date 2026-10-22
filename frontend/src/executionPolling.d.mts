export function pollExecution(options: {
  executionId: number;
  getExecution: (id: number) => Promise<any>;
  getFailed: (id: number) => Promise<any>;
  onUpdate: (result: any, details?: any[]) => void;
  onError: (error: unknown, details: boolean) => void;
  isCurrent?: () => boolean;
  delay?: (ms: number) => Promise<void>;
}): () => void;
