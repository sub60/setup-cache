// @ts-check

const core = require("./tiny-actions-core");
const fs = require("node:fs");
const fsp = require("node:fs/promises");
const { spawn } = require("node:child_process");
const os = require("node:os");
const path = require("node:path");
const { Readable } = require("node:stream");
const { pipeline } = require("node:stream/promises");
const zlib = require("node:zlib");
const {
  printDaemonLog,
  STATE_BINARY_PATH,
  STATE_DAEMON_LOG_PATH,
  STATE_SOCKET_PATH,
} = require("./common");

const DEFAULT_RELEASE_REPOSITORY = "sub60/setup-cache";
const BINARY_NAME = "cache-action";

/**
 * @typedef {{ name: string, commit?: { sha?: string } }} GitHubTag
 * @typedef {{ name: string, url: string }} GitHubReleaseAsset
 * @typedef {{ tag_name: string, assets: GitHubReleaseAsset[] }} GitHubRelease
 */

/**
 * @param {string} name
 * @returns {string}
 */
const requiredEnv = (name) => {
  const value = process.env[name]?.trim();

  if (!value) {
    throw new Error(`${name} is not set`);
  }

  return value;
};

/**
 * @param {string} runnerOs
 * @param {string} runnerArch
 * @returns {string}
 */
const platformAssetTarget = (runnerOs, runnerArch) => {
  const platformByRunnerOs = /** @type {Record<string, string>} */ ({
    Linux: "linux",
    macOS: "darwin",
  });
  const archByRunnerArch = /** @type {Record<string, string>} */ ({
    X64: "x86_64",
    ARM64: "aarch64",
  });

  const platform = platformByRunnerOs[runnerOs];
  if (!platform) {
    throw new Error(`Unsupported RUNNER_OS '${runnerOs}'`);
  }

  const arch = archByRunnerArch[runnerArch];
  if (!arch) {
    throw new Error(`Unsupported RUNNER_ARCH '${runnerArch}'`);
  }

  return `${arch}-${platform}`;
};

/**
 * @param {string} ref
 * @returns {boolean}
 */
const isCommitSha = (ref) => {
  return /^[0-9a-f]{7,40}$/i.test(ref);
};

/**
 * @param {string} [githubToken]
 * @returns {Record<string, string>}
 */
const githubApiHeaders = (githubToken) => {
  const headers = /** @type {Record<string, string>} */ ({
    accept: "application/vnd.github+json",
    "x-github-api-version": "2022-11-28",
  });

  if (githubToken) {
    headers.authorization = `Bearer ${githubToken}`;
  }

  return headers;
};

/**
 * @param {string} releaseRepository
 * @param {string} commitSha
 * @param {string} [githubToken]
 * @returns {Promise<string | null>}
 */
const tagForCommit = async (releaseRepository, commitSha, githubToken) => {
  const apiUrl = requiredEnv("GITHUB_API_URL");

  for (let page = 1; ; page += 1) {
    const response = await fetch(
      `${apiUrl}/repos/${releaseRepository}/tags?per_page=100&page=${page}`,
      {
        headers: githubApiHeaders(githubToken),
        redirect: "follow",
      },
    );

    if (!response.ok) {
      throw new Error(
        `Failed to list tags in '${releaseRepository}' while resolving commit '${commitSha}': ${response.status} ${response.statusText}`,
      );
    }

    const tags = /** @type {GitHubTag[]} */ (await response.json());

    if (tags.length === 0) {
      return null;
    }

    const match = tags.find(({ commit }) => commit?.sha?.startsWith(commitSha));

    if (match) {
      return match.name;
    }
  }
};

/**
 * @param {string} releaseRepository
 * @param {string} actionRef
 * @param {string} [githubToken]
 * @returns {Promise<string>}
 */
const releaseTag = async (releaseRepository, actionRef, githubToken) => {
  if (!isCommitSha(actionRef)) {
    return actionRef;
  }

  const tag = await tagForCommit(releaseRepository, actionRef, githubToken);

  if (!tag) {
    throw new Error(
      `Action ref '${actionRef}' is a commit without a tag in '${releaseRepository}'. This action downloads binaries from GitHub releases, which are only published for tags. Pin the action to a tag to use prebuilt artifacts`,
    );
  }

  return tag;
};

/**
 * @param {string} releaseRepository
 * @param {string} tag
 * @param {string} assetName
 * @returns {{ url: string, headers: Record<string, string> }}
 */
const directReleaseAssetRequest = (releaseRepository, tag, assetName) => {
  return {
    url: `https://github.com/${releaseRepository}/releases/download/${tag}/${assetName}`,
    headers: {},
  };
};

/**
 * @param {string} releaseRepository
 * @param {string} actionRef
 * @param {string} assetName
 * @param {string} [githubToken]
 * @returns {Promise<{ url: string, headers: Record<string, string> }>}
 */
const githubApiReleaseAssetRequest = async (
  releaseRepository,
  actionRef,
  assetName,
  githubToken,
) => {
  const apiUrl = requiredEnv("GITHUB_API_URL");
  const tag = await releaseTag(releaseRepository, actionRef, githubToken);
  const releaseUrl = `${apiUrl}/repos/${releaseRepository}/releases/tags/${encodeURIComponent(tag)}`;

  const response = await fetch(releaseUrl, {
    headers: githubApiHeaders(githubToken),
    redirect: "follow",
  });

  if (!response.ok) {
    throw new Error(
      `Failed to resolve release in '${releaseRepository}': ${response.status} ${response.statusText}`,
    );
  }

  const release = /** @type {GitHubRelease} */ (await response.json());
  const asset = release.assets.find(({ name }) => name === assetName);

  if (!asset) {
    throw new Error(
      `Release '${release.tag_name}' in '${releaseRepository}' does not contain asset '${assetName}'`,
    );
  }

  return {
    url: asset.url,
    headers: {
      accept: "application/octet-stream",
      "x-github-api-version": "2022-11-28",
      ...(githubToken ? { authorization: `Bearer ${githubToken}` } : {}),
    },
  };
};

/**
 * @param {string} url
 * @param {string} destinationPath
 * @param {Record<string, string>} [headers]
 * @returns {Promise<void>}
 */
const downloadGzipFile = async (url, destinationPath, headers = {}) => {
  const response = await fetch(url, {
    redirect: "follow",
    headers,
  });

  if (!response.ok) {
    throw new Error(
      `Failed to download '${url}': ${response.status} ${response.statusText}`,
    );
  }

  if (!response.body) {
    throw new Error(`Download response for '${url}' did not contain a body`);
  }

  const input = Readable.fromWeb(response.body);
  const output = fs.createWriteStream(destinationPath, { mode: 0o755 });
  await pipeline(input, zlib.createGunzip(), output);
  await fsp.chmod(destinationPath, 0o755);
};

/**
 * @param {string} hookPath
 * @param {string} binaryPath
 * @param {string} socketPath
 * @returns {Promise<void>}
 */
const writeHookScript = async (hookPath, binaryPath, socketPath) => {
  const script = [
    "#!/bin/sh",
    "set -eu",
    "set -f",
    "export IFS=' '",
    `exec "${binaryPath}" push --socket "${socketPath}" $OUT_PATHS`,
    "",
  ].join("\n");

  await fsp.writeFile(hookPath, script, { mode: 0o755 });
  await fsp.chmod(hookPath, 0o755);
};

/**
 * @param {string} configPath
 * @param {string} hookPath
 * @returns {Promise<void>}
 */
const writeNixConfig = async (configPath, hookPath) => {
  const config = `post-build-hook = ${hookPath}\n`;
  await fsp.writeFile(configPath, config, { mode: 0o644 });
};

/**
 * @param {string} configPath
 * @returns {string}
 */
const mergedUserConfFiles = (configPath) => {
  const nix_user_conf_files = process.env.NIX_USER_CONF_FILES;

  if (nix_user_conf_files && nix_user_conf_files.trim()) {
    return `${configPath}:${nix_user_conf_files}`;
  }

  // Setting $NIX_USER_CONF_FILES disables Nix's default XDG config lookup, so
  // we must include those paths explicitly.

  const xdgConfigHome =
    process.env.XDG_CONFIG_HOME || path.join(os.homedir(), ".config");

  const xdgConfigDirs = (process.env.XDG_CONFIG_DIRS || "/etc/xdg").split(":");

  const defaultConfFiles = [xdgConfigHome, ...xdgConfigDirs].map(
    (dir) => `${dir}/nix/nix.conf`,
  );

  return [configPath, ...defaultConfFiles].join(":");
};

/**
 * @param {import("node:stream").Readable} readyStream
 * @returns {Promise<void>}
 */
const readReady = async (readyStream) => {
  const chunks = [];

  for await (const chunk of readyStream) {
    chunks.push(Buffer.from(chunk));
  }

  const message = Buffer.concat(chunks);

  if (message.length === 0) {
    throw new Error("daemon exited before reporting readiness");
  }

  if (message[0] === 0) {
    return;
  }

  if (message[0] === 1) {
    throw new Error(
      message.subarray(1).toString("utf8") || "daemon startup failed",
    );
  }

  throw new Error(`daemon reported unknown startup status ${message[0]}`);
};

/**
 * @param {import("node:child_process").ChildProcess} child
 * @returns {Promise<void>}
 */
const waitForDaemonReady = async (child) => {
  const readyStream = child.stdio[3];

  if (!(readyStream instanceof Readable)) {
    throw new Error("daemon readiness pipe was not created");
  }

  const readableReadyStream = /** @type {import("node:stream").Readable} */ (
    readyStream
  );

  /** @type {(error: Error) => void} */
  let onError = () => {};
  /** @type {(code: number | null, signal: NodeJS.Signals | null) => void} */
  let onExit = () => {};

  const exitedEarly = new Promise((_, reject) => {
    onError = reject;
    onExit = (code, signal) => {
      reject(
        new Error(
          `daemon exited before readiness: code=${code}, signal=${signal}`,
        ),
      );
    };

    child.once("error", onError);
    child.once("exit", onExit);
  });

  try {
    await Promise.race([readReady(readableReadyStream), exitedEarly]);
  } finally {
    child.off("error", onError);
    child.off("exit", onExit);
  }
};

/**
 * @param {string} input
 * @returns {string[]}
 */
const parseUpstreamCaches = (input) => {
  const values = input.split(/\s+/).filter(Boolean);
  return values.length === 1 && values[0].toLowerCase() === "none"
    ? []
    : values;
};

/**
 * @returns {Promise<void>}
 */
const main = async () => {
  const origin = core.getInput("origin", { required: true });
  const namespace = core.getInput("namespace", { required: true });
  const cache = core.getInput("cache", { required: true });
  const token = core.getInput("token");
  const upstreamCaches = parseUpstreamCaches(core.getInput("upstream-caches"));
  const githubToken = core.getInput("github-token");
  const releaseRepository =
    core.getInput("release-repository") ||
    process.env.GITHUB_ACTION_REPOSITORY?.trim() ||
    DEFAULT_RELEASE_REPOSITORY;
  const runnerTemp = process.env.RUNNER_TEMP || os.tmpdir();

  // GitHub only sets these when the job has the `id-token: write` permission.
  const canRequestOidcToken =
    !!process.env.ACTIONS_ID_TOKEN_REQUEST_URL &&
    !!process.env.ACTIONS_ID_TOKEN_REQUEST_TOKEN;

  if (!token && !canRequestOidcToken) {
    core.warning(
      "Store paths won't be pushed to the cache, because there's no token " +
        "to push them with. Give the job the `id-token: write` permission, " +
        "or set the `token` input.",
    );
    return;
  }

  const target = await core.group("Detect platform", () => {
    const runnerOs = requiredEnv("RUNNER_OS");
    const runnerArch = requiredEnv("RUNNER_ARCH");
    const target = platformAssetTarget(runnerOs, runnerArch);
    core.info(
      `Detected platform: ${target} (OS=${runnerOs}, ARCH=${runnerArch})`,
    );
    return target;
  });

  const paths = await core.group("Download binary", async () => {
    const tempDirPrefix = `${releaseRepository.replaceAll("/", "-")}-`;
    const daemonDir = await fsp.mkdtemp(path.join(runnerTemp, tempDirPrefix));
    const socketPath = path.join(daemonDir, "daemon.sock");
    const binaryPath = path.join(daemonDir, BINARY_NAME);
    const postBuildHookPath = path.join(daemonDir, "post-build-hook.sh");
    const configPath = path.join(daemonDir, "nix.conf");
    const daemonLogPath = path.join(daemonDir, "daemon.log");
    const assetName = `${BINARY_NAME}-${target}.gz`;
    const actionRef =
      core.getInput("action-ref") || requiredEnv("GITHUB_ACTION_REF");
    const releaseRef = actionRef === "main" ? "pre-release" : actionRef;

    const assetRequest = isCommitSha(releaseRef)
      ? await githubApiReleaseAssetRequest(
          releaseRepository,
          releaseRef,
          assetName,
          githubToken,
        )
      : directReleaseAssetRequest(releaseRepository, releaseRef, assetName);

    core.info(`Downloading '${assetName}' from '${assetRequest.url}'`);
    await downloadGzipFile(assetRequest.url, binaryPath, assetRequest.headers);

    return {
      binary: binaryPath,
      config: configPath,
      daemonLog: daemonLogPath,
      postBuildHook: postBuildHookPath,
      socket: socketPath,
    };
  });

  await core.group("Start daemon", async () => {
    await writeHookScript(paths.postBuildHook, paths.binary, paths.socket);
    await writeNixConfig(paths.config, paths.postBuildHook);

    const daemonArgs = [
      "run",
      "--socket",
      paths.socket,
      "--ready-fd",
      "3",
      "--origin",
      origin,
      "--namespace",
      namespace,
      "--cache",
      cache,
    ];

    if (upstreamCaches.length > 0) {
      daemonArgs.push("--upstream-caches", upstreamCaches.join(","));
    }

    let child;
    try {
      const daemonLogFd = fs.openSync(paths.daemonLog, "a");
      try {
        child = spawn(paths.binary, daemonArgs, {
          // The token is passed in the environment, since other users'
          // processes, like Nix's builds, can read a process's arguments but
          // not its environment.
          env: token ? { ...process.env, SUB60_CACHE_TOKEN: token } : undefined,
          detached: true,
          stdio: ["ignore", daemonLogFd, daemonLogFd, "pipe"],
        });
      } finally {
        fs.closeSync(daemonLogFd);
      }

      await waitForDaemonReady(child);
    } catch (error) {
      await printDaemonLog(paths.daemonLog);
      throw error;
    }

    core.saveState(STATE_SOCKET_PATH, paths.socket);
    core.saveState(STATE_BINARY_PATH, paths.binary);
    core.saveState(STATE_DAEMON_LOG_PATH, paths.daemonLog);
    core.exportVariable(
      "NIX_USER_CONF_FILES",
      mergedUserConfFiles(paths.config),
    );
    core.exportVariable("SUB60_SETUP_CACHE_STARTED", "true");

    core.info(
      `Started daemon with process ID ${child.pid}, listening on ${paths.socket}`,
    );

    child.unref();
  });
};

main().catch((error) => {
  core.setFailed(error.stack || error.message);
});
