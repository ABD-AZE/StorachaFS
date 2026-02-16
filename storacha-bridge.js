#!/usr/bin/env node

/**
 * Storacha Bridge - Handles communication between Go StorachaFS and Storacha JS client
 *
 * Communication protocol:
 * - Receives JSON commands via stdin
 * - Sends JSON responses via stdout
 * - Errors are logged to stderr
 *
 * Commands:
 * - { type: "upload", payload: { path: string, spaceDid?: string } }
 * - { type: "upload-cid", payload: { cid: string, spaceDid?: string } }
 * - { type: "shutdown" }
 */

const readline = require("readline");
const { spawn } = require("child_process");
const fs = require("fs");
const path = require("path");
const os = require("os");

// Send JSON response to stdout
function respond(response) {
  console.log(JSON.stringify(response));
}

// Log to stderr (won't interfere with protocol)
function log(message) {
  process.stderr.write(`[storacha-bridge] ${message}\n`);
}

// Execute storacha CLI command
async function execStoracha(args) {
  return new Promise((resolve, reject) => {
    const proc = spawn("storacha", args, {
      stdio: ["pipe", "pipe", "pipe"],
    });

    let stdout = "";
    let stderr = "";

    proc.stdout.on("data", (data) => {
      stdout += data.toString();
    });

    proc.stderr.on("data", (data) => {
      stderr += data.toString();
    });

    proc.on("close", (code) => {
      if (code === 0) {
        resolve(stdout.trim());
      } else {
        reject(
          new Error(stderr || stdout || `Process exited with code ${code}`),
        );
      }
    });

    proc.on("error", (err) => {
      reject(err);
    });
  });
}

// Upload a file or directory to Storacha
async function handleUpload(payload) {
  const { path: filePath, spaceDid } = payload;

  if (!filePath) {
    throw new Error("path is required");
  }

  if (!fs.existsSync(filePath)) {
    throw new Error(`Path does not exist: ${filePath}`);
  }

  const args = ["up", filePath];

  // Add space DID if provided
  if (spaceDid) {
    args.push("--space", spaceDid);
  }

  log(`Uploading: ${filePath}`);
  const output = await execStoracha(args);

  // Parse CID from output
  // The output format is typically: "⁂ https://w3s.link/ipfs/<CID>"
  // or just the CID on a line
  const cidMatch = output.match(/bafy[a-zA-Z0-9]+/);
  if (!cidMatch) {
    throw new Error(`Could not parse CID from output: ${output}`);
  }

  return cidMatch[0];
}

// Fetch content from IPFS as CAR and upload to Storacha
async function handleUploadCID(payload) {
  const { cid, spaceDid } = payload;

  if (!cid) {
    throw new Error("cid is required");
  }

  // Create a temporary directory for the CAR file
  const tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), "storachafs-"));
  const carPath = path.join(tmpDir, `${cid}.car`);

  try {
    log(`Exporting CID ${cid} as CAR from IPFS...`);

    // Use ipfs dag export to get the exact DAG as a CAR file
    // This preserves the CID and structure exactly
    await new Promise((resolve, reject) => {
      const carFile = fs.createWriteStream(carPath);
      const proc = spawn("ipfs", ["dag", "export", cid], {
        stdio: ["pipe", "pipe", "pipe"],
      });

      proc.stdout.pipe(carFile);

      let stderr = "";
      proc.stderr.on("data", (data) => {
        stderr += data.toString();
      });

      carFile.on("finish", () => {
        resolve();
      });

      proc.on("close", (code) => {
        if (code !== 0) {
          reject(
            new Error(stderr || `ipfs dag export failed with code ${code}`),
          );
        }
      });

      proc.on("error", (err) => {
        reject(err);
      });
    });

    const stats = fs.statSync(carPath);
    log(`CAR file created: ${carPath} (${stats.size} bytes)`);

    // Upload CAR file to Storacha with --car flag
    const args = ["up", carPath, "--car"];

    if (spaceDid) {
      args.push("--space", spaceDid);
    }

    log(`Uploading CAR to Storacha: storacha ${args.join(" ")}`);
    const output = await execStoracha(args);
    log(`Upload output: ${output}`);

    // Parse CID from output
    const cidMatch = output.match(/bafy[a-zA-Z0-9]+/);
    if (!cidMatch) {
      throw new Error(`Could not parse CID from output: ${output}`);
    }

    log(`Uploaded CID: ${cidMatch[0]}`);
    return cidMatch[0];
  } finally {
    // Cleanup temp directory
    try {
      fs.rmSync(tmpDir, { recursive: true, force: true });
    } catch (e) {
      log(`Warning: failed to cleanup temp dir: ${e.message}`);
    }
  }
}

// Main command handler
async function handleCommand(command) {
  try {
    switch (command.type) {
      case "upload": {
        const cid = await handleUpload(command.payload);
        respond({ success: true, cid });
        break;
      }

      case "upload-cid": {
        const cid = await handleUploadCID(command.payload);
        respond({ success: true, cid });
        break;
      }

      case "shutdown": {
        log("Shutting down");
        process.exit(0);
        break;
      }

      default:
        respond({
          success: false,
          error: `Unknown command type: ${command.type}`,
        });
    }
  } catch (error) {
    log(`Error: ${error.message}`);
    respond({ success: false, error: error.message });
  }
}

// Check if storacha CLI is available
async function checkStorachaCLI() {
  try {
    await execStoracha(["--version"]);
    return true;
  } catch (error) {
    return false;
  }
}

// Main entry point
async function main() {
  // Check for storacha CLI
  const hasStoracha = await checkStorachaCLI();
  if (!hasStoracha) {
    respond({
      success: false,
      error:
        "Storacha CLI not found. Please install with: npm install -g @storacha/cli && storacha login",
    });
    process.exit(1);
  }

  // Signal ready
  respond({ success: true });
  log("Bridge ready");

  // Set up readline for stdin
  const rl = readline.createInterface({
    input: process.stdin,
    output: process.stdout,
    terminal: false,
  });

  rl.on("line", async (line) => {
    try {
      const command = JSON.parse(line);
      await handleCommand(command);
    } catch (error) {
      log(`Failed to parse command: ${error.message}`);
      respond({ success: false, error: `Invalid JSON: ${error.message}` });
    }
  });

  rl.on("close", () => {
    log("Stdin closed, shutting down");
    process.exit(0);
  });
}

main().catch((error) => {
  log(`Fatal error: ${error.message}`);
  respond({ success: false, error: error.message });
  process.exit(1);
});
