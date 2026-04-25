type NativeModule = {
  lexical_score?: (text: string, query: string) => number;
  lexicalScore?: (text: string, query: string) => number;
  validate_config?: (configJson: string) => string;
  validateConfig?: (configJson: string) => string;
  e2e_smoke?: () => string;
  e2ESmoke?: () => string;
  e2eSmoke?: () => string;
  reembed_plan?: (requestJson: string) => string;
  reembedPlan?: (requestJson: string) => string;
  ingest_request_schema?: () => string;
  ingestRequestSchema?: () => string;
};

let nativeModule: NativeModule | null = null;

export function lexicalScore(text: string, query: string): number {
  if (!nativeModule) {
    nativeModule = require("./index.node") as NativeModule;
  }

  const scorer = nativeModule.lexicalScore ?? nativeModule.lexical_score;
  if (!scorer) {
    throw new Error("Nextral native module is missing lexical score export.");
  }
  return scorer(text, query);
}

export function validateConfig(config: unknown): { status: string } {
  if (!nativeModule) {
    nativeModule = require("./index.node") as NativeModule;
  }

  const validator = nativeModule.validateConfig ?? nativeModule.validate_config;
  if (!validator) {
    throw new Error("Nextral native module is missing config validation export.");
  }
  return JSON.parse(validator(JSON.stringify(config))) as { status: string };
}

export function e2eSmoke(): unknown {
  if (!nativeModule) {
    nativeModule = require("./index.node") as NativeModule;
  }
  const run = nativeModule.e2eSmoke ?? nativeModule.e2ESmoke ?? nativeModule.e2e_smoke;
  if (!run) {
    throw new Error("Nextral native module is missing e2e smoke export.");
  }
  return JSON.parse(run());
}

export function reembedPlan(request: unknown): unknown {
  if (!nativeModule) {
    nativeModule = require("./index.node") as NativeModule;
  }
  const plan = nativeModule.reembedPlan ?? nativeModule.reembed_plan;
  if (!plan) {
    throw new Error("Nextral native module is missing reembed plan export.");
  }
  return JSON.parse(plan(JSON.stringify(request)));
}

export function ingestRequestSchema(): unknown {
  if (!nativeModule) {
    nativeModule = require("./index.node") as NativeModule;
  }
  const schema = nativeModule.ingestRequestSchema ?? nativeModule.ingest_request_schema;
  if (!schema) {
    throw new Error("Nextral native module is missing ingest schema export.");
  }
  return JSON.parse(schema());
}
