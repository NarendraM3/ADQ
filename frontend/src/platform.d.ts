export type PlatformJob = {
  environment?: string | null;
  destination?: Record<string, unknown>;
  settings?: Record<string, unknown>;
};

export function displayPlatform(job: PlatformJob): string;
export const SELECTED_PLATFORM_KEY: string;
export function getSelectedPlatform(): string;
export function saveSelectedPlatform(platform: string): void;
