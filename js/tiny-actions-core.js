// @ts-check

// This is the subset of @actions/core that this action actually uses.

/**
 * @param {string} name
 * @returns {string}
 */
const envKey = (name) => {
  return name.replace(/ /g, "_").toUpperCase();
};

/**
 * @param {string} file
 * @param {string} key
 * @param {string} value
 */
const appendFileCommand = (file, key, value) => {
  const fs = require("node:fs");
  const os = require("node:os");

  fs.appendFileSync(
    file,
    `${key}<<__SUB60_EOF__${os.EOL}${value}${os.EOL}__SUB60_EOF__${os.EOL}`,
  );
};

/**
 * @param {string} name
 * @param {{ required?: boolean }} [options]
 * @returns {string}
 */
const getInput = (name, options = {}) => {
  const value = process.env[`INPUT_${envKey(name)}`]?.trim() || "";

  if (!value && options.required) {
    throw new Error(`Input required and not supplied: ${name}`);
  }

  return value;
};

/**
 * @param {string} name
 * @returns {string}
 */
const getState = (name) => {
  return process.env[`STATE_${name}`] || "";
};

/**
 * @param {string} name
 * @param {string} value
 */
const saveState = (name, value) => {
  process.env[`STATE_${name}`] = value;

  if (process.env.GITHUB_STATE) {
    appendFileCommand(process.env.GITHUB_STATE, name, value);
    return;
  }

  process.stdout.write(`::save-state name=${name}::${value}\n`);
};

/**
 * @param {string} name
 * @param {string} value
 */
const exportVariable = (name, value) => {
  process.env[name] = value;

  if (process.env.GITHUB_ENV) {
    appendFileCommand(process.env.GITHUB_ENV, name, value);
    return;
  }

  process.stdout.write(`::set-env name=${name}::${value}\n`);
};

/**
 * @param {string} message
 */
const info = (message) => {
  process.stdout.write(`${message}\n`);
};

/**
 * @template T
 * @param {string} name
 * @param {() => T | Promise<T>} fn
 * @returns {Promise<T>}
 */
const group = async (name, fn) => {
  process.stdout.write(`::group::${name}\n`);

  try {
    return await fn();
  } finally {
    process.stdout.write("::endgroup::\n");
  }
};

/**
 * @param {string} message
 */
const warning = (message) => {
  process.stdout.write(`::warning::${message}\n`);
};

/**
 * @param {string} message
 */
const setFailed = (message) => {
  process.exitCode = 1;
  process.stderr.write(`::error::${message}\n`);
};

module.exports = {
  exportVariable,
  getInput,
  getState,
  group,
  info,
  saveState,
  setFailed,
  warning,
};
