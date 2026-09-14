#!/usr/bin/env node
// Builds the Daytona snapshot that Codex agents run in, from .daytona/Dockerfile.
//
// The snapshot name is derived from the Dockerfile contents, so editing the
// Dockerfile produces a new snapshot and leaves the old one untouched. After a
// successful build the script writes the name into .codex-daytona.json.
//
// Usage: node .daytona/build-snapshot.mjs
//
// The script borrows the Daytona SDK and the DAYTONA_API_KEY from a checkout of
// codex-daytona. Set CODEX_DAYTONA_DIR if it is not at ~/code/codex-daytona.

import { createHash } from 'node:crypto';
import { readFile, writeFile } from 'node:fs/promises';
import { homedir } from 'node:os';
import { dirname, join, resolve } from 'node:path';
import { fileURLToPath, pathToFileURL } from 'node:url';

const repo = resolve(dirname(fileURLToPath(import.meta.url)), '..');
const launcher = process.env.CODEX_DAYTONA_DIR || join(homedir(), 'code', 'codex-daytona');
const dockerfile = join(repo, '.daytona', 'Dockerfile');
const configPath = join(repo, '.codex-daytona.json');

const env = await readFile(join(launcher, '.env'), 'utf8');
const apiKey = env.match(/^DAYTONA_API_KEY=(.+)$/m)?.[1]?.trim();
if (!apiKey) throw new Error(`DAYTONA_API_KEY not found in ${join(launcher, '.env')}`);

const { Daytona, Image } = await import(pathToFileURL(join(launcher, 'node_modules', '@daytona', 'sdk', 'esm', 'index.js')).href);

const hash = createHash('sha256').update(await readFile(dockerfile)).digest('hex').slice(0, 12);
const name = `swarmy-dev-${hash}`;
const daytona = new Daytona({ apiKey });

let exists = false;
try {
  const snapshot = await daytona.snapshot.get(name);
  exists = snapshot.state === 'active';
  console.log(`Snapshot ${name} already exists (state: ${snapshot.state}).`);
} catch {
  exists = false;
}

if (!exists) {
  console.log(`Building snapshot ${name} from ${dockerfile}`);
  await daytona.snapshot.create(
    { name, image: Image.fromDockerfile(dockerfile), resources: { cpu: 4, memory: 8, disk: 10 } },
    { onLogs: chunk => process.stdout.write(chunk), timeout: 3600 },
  );
  console.log(`Snapshot ${name} is ready.`);
}

const config = JSON.parse(await readFile(configPath, 'utf8'));
if (config.snapshot !== name) {
  config.snapshot = name;
  await writeFile(configPath, `${JSON.stringify(config, null, 2)}\n`);
  console.log(`Updated ${configPath} to use ${name}.`);
}
