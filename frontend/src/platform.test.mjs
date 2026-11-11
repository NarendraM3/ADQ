import assert from 'node:assert/strict';
import { displayPlatform, getSelectedPlatform, platformDestinationType, platformKey, saveSelectedPlatform } from './platform.js';

assert.equal(displayPlatform({ environment: 'databricks' }), 'Databricks');
assert.equal(displayPlatform({ environment: 'DATABRICKS' }), 'Databricks');
assert.equal(displayPlatform({ environment: 'Microsoft_Fabric' }), 'Microsoft Fabric');
assert.equal(displayPlatform({ environment: 'Snowflake' }), 'Snowflake');
assert.equal(displayPlatform({ environment: 'fabric' }), 'Microsoft Fabric');
assert.equal(platformKey('Microsoft Fabric'), 'fabric');
assert.equal(platformDestinationType('Snowflake'), 'snowflake');
assert.equal(platformDestinationType(''), 'local');

assert.equal(displayPlatform({ settings: { platform: 'fabric' }, destination: { type: 'local' } }), 'Microsoft Fabric');
assert.equal(displayPlatform({ destination: { type: 'local' } }), 'Unspecified');
assert.equal(displayPlatform({ destination: { type: 's3' } }), 'Unspecified');

const saved = new Map();
globalThis.localStorage = {
  getItem: key => saved.get(key) ?? null,
  setItem: (key, value) => saved.set(key, value),
};
assert.equal(getSelectedPlatform(), '');
saveSelectedPlatform('Databricks');
assert.equal(getSelectedPlatform(), 'Databricks');
assert.equal(displayPlatform({ environment: getSelectedPlatform() }), 'Databricks');
console.log('Frontend platform display assertions passed.');
