'use strict';

const { loadNative } = require('./native-loader');

class NextralError extends Error {
  constructor(code, message, options) {
    super(message, options);
    this.name = 'NextralError';
    this.code = code;
  }
}

function normalizeError(error) {
  if (error instanceof NextralError) return error;
  if (error && typeof error.message === 'string') {
    try {
      const details = JSON.parse(error.message);
      if (typeof details.code === 'string' && typeof details.message === 'string') {
        return new NextralError(details.code, details.message, { cause: error });
      }
    } catch { /* Native transport errors are not always JSON. */ }
  }
  return new NextralError(error instanceof TypeError ? 'invalid_input' : 'native_error',
    error instanceof Error ? error.message : String(error), { cause: error });
}

function encode(value) {
  const json = JSON.stringify(value, (_key, item) => {
    if (typeof item === 'number' && !Number.isFinite(item)) {
      throw new NextralError('invalid_input', 'JSON numeric inputs must be finite.');
    }
    return item;
  });
  if (json === undefined) throw new NextralError('invalid_input', 'A JSON object is required.');
  return json;
}

function invoke(method, ...args) {
  try {
    return loadNative()[method](...args);
  } catch (error) {
    throw normalizeError(error);
  }
}

function call(tool, request) {
  try {
    return JSON.parse(invoke('mcpCall', encode({ tool, payload_json: encode(request) })));
  } catch (error) {
    throw normalizeError(error);
  }
}

function configureRuntime(config) {
  try {
    return JSON.parse(invoke('configureRuntime', encode(config)));
  } catch (error) {
    throw normalizeError(error);
  }
}

function validateConfig(config) {
  try {
    return JSON.parse(invoke('validateConfig', encode(config)));
  } catch (error) {
    throw normalizeError(error);
  }
}

async function serve(config, modes) {
  try {
    return JSON.parse(await invoke('runServiceHosts', encode(config), encode(modes)));
  } catch (error) {
    throw normalizeError(error);
  }
}

exports.NextralError = NextralError;
exports.configureRuntime = configureRuntime;
exports.validateConfig = validateConfig;
exports.serve = serve;
exports.lexicalScore = (text, query) => invoke('lexicalScore', text, query);
exports.health = () => call('nextral.health', {});
exports.ingestMemory = request => call('nextral.memory.ingest', request);
exports.retrieve = request => call('nextral.memory.retrieve', request);
exports.forgetMemory = request => call('nextral.memory.forget', request);
exports.updateMemory = request => call('nextral.memory.update', request);
exports.listMemories = request => call('nextral.memory.list', request);
exports.memoryStats = request => call('nextral.memory.stats', request);
exports.searchByTag = request => call('nextral.memory.search_by_tag', request);
exports.searchByEntity = request => call('nextral.memory.search_by_entity', request);
exports.appendSessionMessage = request => call('nextral.session.append', request);
exports.eligibleSessions = request => call('nextral.session.eligible', request);
exports.workingContext = request => call('nextral.session.context', request);
exports.queryGraph = request => call('nextral.graph.query', request);
exports.graphify = request => call('nextral.graph.graphify', request);
exports.consolidate = request => call('nextral.consolidation.run', request);
exports.scheduleReminder = request => call('nextral.reminders.schedule', request);
exports.executeDueReminders = request => call('nextral.reminders.due', request);
exports.planReembed = request => call('nextral.reembed.plan', request);
exports.batchIngest = request => call('nextral.batch.ingest', request);
exports.batchRetrieve = request => call('nextral.batch.retrieve', request);
exports.batchForget = request => call('nextral.batch.forget', request);
exports.migrateDatabase = () => call('nextral.database.migrate', {});
exports.getMemory = request => call('nextral.memory.get', request);
exports.listReminders = request => call('nextral.reminders.list', request);
exports.executeReembed = request => call('nextral.jobs.reembed.execute', request);
exports.enqueueJob = request => call('nextral.jobs.enqueue', request);
exports.runJobs = request => call('nextral.jobs.run', request);
exports.listJobs = request => call('nextral.jobs.list', request);
exports.getJob = request => call('nextral.jobs.get', request);
exports.runOutbox = request => call('nextral.outbox.run', request);
exports.stopServing = () => invoke('stopServing');
exports.upsertPolicy = request => call('nextral.policy.upsert', request);
exports.listPolicies = request => call('nextral.policy.list', request);
exports.removePolicy = request => call('nextral.policy.remove', request);
exports.createExperiment = request => call('experiments.create', request);
exports.promoteExperiment = request => call('experiments.promote', request);
exports.rollbackExperiment = request => call('experiments.rollback', request);
exports.experimentStatus = (request = {}) => call('experiments.status', request);
exports.getSafetyPolicy = () => call('safety.policy.get', {});
exports.setSafetyPolicy = request => call('safety.policy.set', request);
exports.scoredSearch = request => call('nextral.runtime.scored_search', request);
