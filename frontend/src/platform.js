/** @typedef {{environment?: string | null, destination?: Record<string, unknown>, settings?: Record<string, unknown>}} PlatformJob */

export function platformKey(value) {
  if (typeof value !== 'string' || !value.trim()) return '';
  const key = value.trim().replace(/[_-]+/g, ' ').replace(/\s+/g, ' ').toLowerCase();
  return {
    databricks: 'databricks',
    snowflake: 'snowflake',
    fabric: 'fabric',
    onelake: 'fabric',
    microsoftfabric: 'fabric',
    'microsoft fabric': 'fabric',
  }[key] || '';
}

export function platformDestinationType(value) {
  return platformKey(value) || 'local';
}

/** @param {PlatformJob} job */
export function displayPlatform(job) {
  const settings = job.settings || {};
  const raw = job.environment || settings.environment || settings.platform;
  if (typeof raw !== 'string' || !raw.trim()) return 'Unspecified';

  const key = raw.trim().replace(/[_-]+/g, ' ').replace(/\s+/g, ' ').toLowerCase();
  const labels = {
    databricks: 'Databricks',
    snowflake: 'Snowflake',
    fabric: 'Microsoft Fabric',
    onelake: 'Microsoft Fabric',
    microsoftfabric: 'Microsoft Fabric',
    'microsoft fabric': 'Microsoft Fabric',
    local: 'Local',
    s3: 'Amazon S3',
    'amazon s3': 'Amazon S3',
    azureblob: 'Azure Blob Storage',
    'azure blob': 'Azure Blob Storage',
    'azure blob storage': 'Azure Blob Storage',
  };
  return labels[key] || raw.trim();
}

export const SELECTED_PLATFORM_KEY = 'adq-selected-platform';

export function getSelectedPlatform() {
  if (typeof localStorage === 'undefined') return '';
  return localStorage.getItem(SELECTED_PLATFORM_KEY) || '';
}

export function saveSelectedPlatform(platform) {
  const label = displayPlatform({ environment: platform });
  if (typeof localStorage !== 'undefined' && label !== 'Unspecified') {
    localStorage.setItem(SELECTED_PLATFORM_KEY, label);
  }
}
