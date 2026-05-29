#!/usr/bin/env node
import { runCli } from "../dist/cli/index.js";

process.exitCode = runCli();
