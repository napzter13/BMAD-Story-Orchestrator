#!/usr/bin/env node

/**
 * BMAD Story Orchestrator - Simple & Powerful
 *
 * Uses existing BMAD agents (@analyst.mdc, @pm.mdc, etc.) for proper workflow orchestration
 * Maintains core business logic: CLI commands, workflow states, REVIEW files, state persistence
 * Delegates all specialized work to BMAD agents - no custom implementations
 *
 */

// Load AI model from environment or use default
const AI_MODEL = 'grok-code-fast-1';

const fs = require('fs');
const path = require('path');
const { execSync } = require('child_process');

const STORIES_DIR = path.join(__dirname, '..', 'apps', 'web', 'stories');
const WORKFLOW_STATE_FILE = path.join(STORIES_DIR, '.workflow-state.json');

// Allow multiple concurrent instances - no global lock file

const WORKFLOW_STATES = {
  READY: 'ready',
  ANALYZED: 'analyzed',
  REFINED: 'refined',
  AWAITING_HUMAN_INPUT: 'in_review',
};

const PRIORITY_FILE = path.join(STORIES_DIR, '.workflow-priority.json');
const LOCKS_DIR = path.join(STORIES_DIR, '.locks');
const WORKFLOW_STATE_LOCK_FILE = path.join(LOCKS_DIR, 'workflow-state.lock');
const ORCHESTRATOR_INSTANCE_LOCK_FILE = path.join(
  STORIES_DIR,
  '.orchestrator-instance.lock'
);

// Color codes for terminal output
const colors = {
  reset: '\x1b[0m',
  red: '\x1b[31m',
  green: '\x1b[32m',
  yellow: '\x1b[33m',
  blue: '\x1b[34m',
  magenta: '\x1b[35m',
  cyan: '\x1b[36m',
};

function log(message, color = 'white') {
  console.log(`${colors[color]}${message}${colors.reset}`);
}

function logError(message, error = null) {
  log(`❌ ${message}`, 'red');
  if (error) {
    if (error instanceof Error) {
      console.error(`   Full error: ${error.message}`);
      if (error.stack) {
        console.error(`   Stack trace: ${error.stack}`);
      }
    } else {
      console.error(`   Error details: ${JSON.stringify(error, null, 2)}`);
    }
  }
}
function logSuccess(message) {
  log(`✅ ${message}`, 'green');
}
function logInfo(message) {
  log(`ℹ️  ${message}`, 'blue');
}
function logWarning(message) {
  log(`⚠️  ${message}`, 'yellow');
}

/**
 * Load workflow state from JSON file
 */
function loadWorkflowState() {
  try {
    if (fs.existsSync(WORKFLOW_STATE_FILE)) {
      return JSON.parse(fs.readFileSync(WORKFLOW_STATE_FILE, 'utf8'));
    }
  } catch (error) {
    logWarning(`Could not load workflow state: ${error.message}`);
  }
  return {};
}

/**
 * Save workflow state to JSON file
 */
function saveWorkflowState(state) {
  try {
    fs.writeFileSync(WORKFLOW_STATE_FILE, JSON.stringify(state, null, 2));
  } catch (error) {
    logError(`Failed to save workflow state: ${error.message}`);
  }
}

/**
 * Get workflow state for a story (without .md extension)
 */
function getStoryWorkflowState(storyFilename, state) {
  const key = storyFilename.replace(/\.md$/, '');
  return state[key] || WORKFLOW_STATES.READY;
}

/**
 * Update workflow state for a story
 */
function updateStoryWorkflowState(storyFilename, newState, state) {
  const key = storyFilename.replace(/\.md$/, '');
  state[key] = newState;
  saveWorkflowState(state);
  logInfo(`Updated ${storyFilename} workflow state to: ${newState}`);
}

/**
 * Acquire an atomic lock for workflow state file access
 */
function acquireWorkflowStateLock(operation = 'access') {
  try {
    // Ensure locks directory exists
    if (!fs.existsSync(LOCKS_DIR)) {
      fs.mkdirSync(LOCKS_DIR, { recursive: true });
    }

    // Check if lock file exists and if we can steal it
    if (fs.existsSync(WORKFLOW_STATE_LOCK_FILE)) {
      try {
        const existingLock = JSON.parse(
          fs.readFileSync(WORKFLOW_STATE_LOCK_FILE, 'utf8')
        );
        const lockAge = Date.now() - new Date(existingLock.timestamp).getTime();
        const MAX_LOCK_AGE = 5 * 60 * 1000; // 5 minutes max lock age

        // Lock stealing: if process is dead OR lock is older than max age
        if (!isProcessRunning(existingLock.pid) || lockAge > MAX_LOCK_AGE) {
          logWarning(
            `🔓 Stealing stale workflow state lock (PID: ${existingLock.pid}, age: ${Math.round(lockAge / 1000)}s)`
          );
          fs.unlinkSync(WORKFLOW_STATE_LOCK_FILE);
        } else {
          // Lock is still valid
          return false;
        }
      } catch (error) {
        // Lock file is corrupted, remove it
        logWarning(`🔧 Removing corrupted workflow state lock file`);
        fs.unlinkSync(WORKFLOW_STATE_LOCK_FILE);
      }
    }

    // Try to create lock file exclusively (wx flag prevents overwriting existing file)
    const lockData = {
      pid: process.pid,
      operation,
      timestamp: new Date().toISOString(),
    };

    fs.writeFileSync(
      WORKFLOW_STATE_LOCK_FILE,
      JSON.stringify(lockData, null, 2),
      {
        flag: 'wx',
      }
    );
    return true;
  } catch (error) {
    // Lock file already exists or other file system error
    logWarning(`Could not acquire workflow state lock: ${error.message}`);
    return false;
  }
}

/**
 * Release the atomic lock for workflow state file access
 */
function releaseWorkflowStateLock() {
  try {
    if (fs.existsSync(WORKFLOW_STATE_LOCK_FILE)) {
      fs.unlinkSync(WORKFLOW_STATE_LOCK_FILE);
    }
  } catch (error) {
    logWarning(`Could not release workflow state lock: ${error.message}`);
  }
}

/**
 * Atomically load workflow state with proper locking
 */
async function loadWorkflowStateAtomic() {
  let lockAcquired = false;
  try {
    // Acquire lock with retry logic
    let retries = 0;
    const maxRetries = 10;
    const baseDelay = 100; // 100ms base delay

    while (!lockAcquired && retries < maxRetries) {
      lockAcquired = acquireWorkflowStateLock('load');
      if (!lockAcquired) {
        retries++;
        if (retries < maxRetries) {
          // Exponential backoff with jitter to avoid thundering herd
          const delay = baseDelay * Math.pow(2, retries) + Math.random() * 50;
          await new Promise((resolve) => setTimeout(resolve, delay));
        }
      }
    }

    if (!lockAcquired) {
      throw new Error(
        'Failed to acquire workflow state lock after maximum retries'
      );
    }

    // Load the state
    return loadWorkflowState();
  } finally {
    if (lockAcquired) {
      releaseWorkflowStateLock();
    }
  }
}

/**
 * Atomically save workflow state with proper locking
 */
async function saveWorkflowStateAtomic(state) {
  let lockAcquired = false;
  try {
    // Acquire lock with retry logic
    let retries = 0;
    const maxRetries = 10;
    const baseDelay = 100; // 100ms base delay

    while (!lockAcquired && retries < maxRetries) {
      lockAcquired = acquireWorkflowStateLock('save');
      if (!lockAcquired) {
        retries++;
        if (retries < maxRetries) {
          // Exponential backoff with jitter to avoid thundering herd
          const delay = baseDelay * Math.pow(2, retries) + Math.random() * 50;
          await new Promise((resolve) => setTimeout(resolve, delay));
        }
      }
    }

    if (!lockAcquired) {
      throw new Error(
        'Failed to acquire workflow state lock after maximum retries'
      );
    }

    // Save the state
    saveWorkflowState(state);
  } finally {
    if (lockAcquired) {
      releaseWorkflowStateLock();
    }
  }
}

/**
 * Atomically update workflow state for a story with proper locking
 */
async function updateStoryWorkflowStateAtomic(storyFilename, newState) {
  let lockAcquired = false;
  try {
    // Acquire lock with retry logic
    let retries = 0;
    const maxRetries = 10;
    const baseDelay = 100; // 100ms base delay

    while (!lockAcquired && retries < maxRetries) {
      lockAcquired = acquireWorkflowStateLock('update');
      if (!lockAcquired) {
        retries++;
        if (retries < maxRetries) {
          // Exponential backoff with jitter to avoid thundering herd
          const delay = baseDelay * Math.pow(2, retries) + Math.random() * 50;
          await new Promise((resolve) => setTimeout(resolve, delay));
        }
      }
    }

    if (!lockAcquired) {
      throw new Error(
        'Failed to acquire workflow state lock after maximum retries'
      );
    }

    // Load current state, update, and save atomically
    const state = loadWorkflowState();
    const key = storyFilename.replace(/\.md$/, '');
    state[key] = newState;
    saveWorkflowState(state);
    logInfo(`Updated ${storyFilename} workflow state to: ${newState}`);
  } finally {
    if (lockAcquired) {
      releaseWorkflowStateLock();
    }
  }
}

/**
 * Get all story files from stories directory
 */
function getStoryFiles() {
  try {
    const files = fs.readdirSync(STORIES_DIR);
    return files
      .filter((file) => file.endsWith('.md') && !file.startsWith('.'))
      .map((file) => path.join(STORIES_DIR, file))
      .sort();
  } catch (error) {
    logError(`Failed to read stories directory: ${error.message}`);
    return [];
  }
}

/**
 * Read and parse a story file
 */
function readStoryFile(storyPath) {
  try {
    const filename = path.basename(storyPath);

    return {
      path: storyPath,
      filename,
    };
  } catch (error) {
    logError(`Failed to read story file ${storyPath}: ${error.message}`);
    return null;
  }
}

/**
 * Execute BMAD agent using cursor-agent CLI
 */
function runBMADAgent(agentType, command, storyPath, options = {}) {
  // Use BMAD agent file paths as references
  const agentRoles = {
    analyst: '@/.cursor/rules/bmad/analyst.mdc',
    pm: '@/.cursor/rules/bmad/pm.mdc',
    sm: '@/.cursor/rules/bmad/sm.mdc',
    architect: '@/.cursor/rules/bmad/architect.mdc',
    dev: '@/.cursor/rules/bmad/dev.mdc',
    qa: '@/.cursor/rules/bmad/qa.mdc',
  };

  const role = agentRoles[agentType] || 'AI Assistant';

  // Reference the story file
  let storyReference = '';
  if (storyPath !== '/dev/null') {
    storyReference = `\n\nStory File: ${storyPath}`;
  }

  const fullPrompt = `${role}, ${command} ${storyReference}`;

  console.log(`\n${'='.repeat(80)}`);
  console.log(`🚀 SPAWNING AGENT: ${role} (${agentType})`);
  console.log(`📝 COMMAND: ${command}`);
  console.log(`📁 STORY: ${storyPath}`);
  console.log(
    `🔧 EXECUTING: cursor-agent agent "${fullPrompt.substring(0, 300)}..."`
  );
  console.log(`${'='.repeat(80)}\n`);

  // Use spawn for better process control and cancellation
  const { spawn } = require('child_process');
  const timeout = options.allowCode ? 1800000 : 600000;

  return new Promise((resolve, reject) => {
    // Create the agent process - NO detached mode for proper cleanup
    const agentProcess = spawn(
      'cursor-agent',
      ['agent', '--model', AI_MODEL, '--print', '--output-format', 'text'],
      {
        stdio: ['pipe', 'pipe', 'pipe'],
        cwd: process.cwd(),
        // REMOVED: detached: true - this prevents proper process killing
      }
    );

    // Track the process for cleanup
    activeChildProcesses.set(agentType, agentProcess);

    let stdout = '';
    let stderr = '';
    let timeoutId;
    let isCompleted = false;

    // Handle stdout
    agentProcess.stdout.on('data', (data) => {
      stdout += data.toString();
    });

    // Handle stderr
    agentProcess.stderr.on('data', (data) => {
      stderr += data.toString();
    });

    // Cleanup function for process termination
    const cleanupProcess = () => {
      if (timeoutId) {
        clearTimeout(timeoutId);
        timeoutId = null;
      }
      activeChildProcesses.delete(agentType);
      isCompleted = true;
    };

    // Handle process completion
    agentProcess.on('close', (code) => {
      if (isCompleted) return; // Already handled
      cleanupProcess();

      console.log(`\n${'='.repeat(80)}`);
      if (code === 0) {
        console.log(`✅ AGENT COMPLETED: ${role} (${agentType})`);
        console.log(`${'='.repeat(80)}\n`);
        resolve(stdout.trim());
      } else {
        console.log(
          `❌ AGENT FAILED: ${role} (${agentType}) - Exit code: ${code}`
        );
        if (stderr) {
          console.log(`STDERR: ${stderr}`);
        }
        console.log(`${'='.repeat(80)}\n`);
        reject(new Error(`Agent process exited with code ${code}: ${stderr}`));
      }
    });

    // Handle process errors
    agentProcess.on('error', (error) => {
      if (isCompleted) return; // Already handled
      cleanupProcess();

      console.log(`\n${'='.repeat(80)}`);
      console.log(`❌ AGENT ERROR: ${role} (${agentType}) - ${error.message}`);
      console.log(`${'='.repeat(80)}\n`);
      reject(error);
    });

    // Send the prompt to the agent
    agentProcess.stdin.write(fullPrompt);
    agentProcess.stdin.end();

    // Set timeout with proper cleanup
    timeoutId = setTimeout(() => {
      if (isCompleted) return; // Already handled

      console.log(`\n${'='.repeat(80)}`);
      console.log(
        `⏰ AGENT TIMEOUT: ${role} (${agentType}) - ${timeout}ms exceeded`
      );
      console.log(`${'='.repeat(80)}\n`);

      // Kill the process with graceful shutdown
      const killProcess = () => {
        try {
          if (!agentProcess.killed) {
            agentProcess.kill('SIGTERM');
            // Give it 3 seconds to terminate gracefully, then force kill
            setTimeout(() => {
              try {
                if (!agentProcess.killed) {
                  console.log(
                    `🔪 Force-killing ${agentType} agent (PID: ${agentProcess.pid})`
                  );
                  agentProcess.kill('SIGKILL');
                }
              } catch (forceKillError) {
                console.error(
                  `❌ Failed to force-kill process: ${forceKillError.message}`
                );
              }
            }, 3000);
          }
        } catch (killError) {
          console.error(
            `❌ Failed to kill timed out process: ${killError.message}`
          );
        }
      };

      cleanupProcess();
      killProcess();
      reject(new Error(`Agent timeout after ${timeout}ms`));
    }, timeout);
  });
}

/**
 * Show status of all stories
 */
async function showStatus() {
  const state = await loadWorkflowStateAtomic();
  const storyFiles = getStoryFiles();
  const stories = storyFiles
    .map(readStoryFile)
    .filter((story) => story !== null);

  logInfo('📋 Story Status Overview:');
  console.log('');

  stories.forEach((story) => {
    const workflowState = getStoryWorkflowState(story.filename, state);

    let stateColor = 'white';
    let stateIcon = '📝';

    switch (workflowState) {
      case WORKFLOW_STATES.READY:
        stateColor = 'yellow';
        stateIcon = '⏳';
        break;
      case WORKFLOW_STATES.ANALYZED:
        stateColor = 'blue';
        stateIcon = '📊';
        break;
      case WORKFLOW_STATES.REFINED:
        stateColor = 'magenta';
        stateIcon = '🔧';
        break;
      case WORKFLOW_STATES.AWAITING_HUMAN_INPUT:
        stateColor = 'red';
        stateIcon = '⏸️';
        break;
    }

    console.log(
      `${stateIcon} ${colors[stateColor]}${story.filename}${colors.reset}`
    );
    console.log(`  Workflow: ${workflowState.replace(/_/g, ' ')}`);
    console.log('');
  });

  console.log(
    `${colors.cyan}Workflow state file: ${WORKFLOW_STATE_FILE}${colors.reset}`
  );
  console.log('');
}

/**
 * Acquire a file-based lock for story processing with PID validation and lock stealing
 */
function acquireStoryLock(storyFilename, command) {
  const lockFile = path.join(LOCKS_DIR, `${storyFilename}.${command}.lock`);

  try {
    // Ensure locks directory exists
    if (!fs.existsSync(LOCKS_DIR)) {
      fs.mkdirSync(LOCKS_DIR, { recursive: true });
    }

    // Check if lock file exists and if we can steal it
    if (fs.existsSync(lockFile)) {
      try {
        const existingLock = JSON.parse(fs.readFileSync(lockFile, 'utf8'));
        const lockAge = Date.now() - new Date(existingLock.timestamp).getTime();
        const MINUTES = 15 * 60 * 1000;

        // Lock stealing: if process is dead OR lock is older than 5 minutes
        if (!isProcessRunning(existingLock.pid) || lockAge > MINUTES) {
          logWarning(
            `🔓 Stealing stale lock for ${storyFilename} (PID: ${existingLock.pid}, age: ${Math.round(lockAge / 1000)}s)`
          );
          fs.unlinkSync(lockFile);
        } else {
          // Lock is still valid
          return false;
        }
      } catch (error) {
        // Lock file is corrupted, remove it
        logWarning(`🔧 Removing corrupted lock file for ${storyFilename}`);
        fs.unlinkSync(lockFile);
      }
    }

    // Try to create lock file exclusively (wx flag prevents overwriting existing file)
    const lockData = {
      pid: process.pid,
      command,
      storyFile: storyFilename,
      timestamp: new Date().toISOString(),
    };

    fs.writeFileSync(lockFile, JSON.stringify(lockData, null, 2), {
      flag: 'wx',
    });
    return true;
  } catch (error) {
    // Lock file already exists or other file system error
    logWarning(`Could not acquire lock for ${storyFilename}: ${error.message}`);
    return false;
  }
}

/**
 * Release a file-based lock
 */
function releaseStoryLock(storyFilename, command) {
  const lockFile = path.join(LOCKS_DIR, `${storyFilename}.${command}.lock`);
  try {
    if (fs.existsSync(lockFile)) {
      fs.unlinkSync(lockFile);
    }
  } catch (error) {
    logWarning(`Could not release lock: ${error.message}`);
  }
}

/**
 * Load priority ordering from JSON file
 */
function loadPriorityOrder() {
  try {
    if (fs.existsSync(PRIORITY_FILE)) {
      return JSON.parse(fs.readFileSync(PRIORITY_FILE, 'utf8'));
    }
  } catch (error) {
    logWarning(`Could not load priority order: ${error.message}`);
  }
  return { order: [], stopped: [], lastUpdated: null, version: 1 };
}

/**
 * Save priority ordering to JSON file
 */
function savePriorityOrder(priorityData) {
  try {
    fs.writeFileSync(PRIORITY_FILE, JSON.stringify(priorityData, null, 2));
    logInfo(
      `📋 Priority order saved with ${priorityData.order.length} stories`
    );
  } catch (error) {
    logError(`Failed to save priority order: ${error.message}`);
  }
}

/**
 * Validate file size to prevent processing extremely large files
 */
function validateFileSize(filePath, maxLines = 10000) {
  const content = fs.readFileSync(filePath, 'utf8');
  const lines = content.split('\n').length;

  if (lines > maxLines) {
    throw new Error(`File too large: ${lines} lines (max: ${maxLines})`);
  }
}

/**
 * Detect content loops to prevent infinite agent output
 */
function detectContentLoops(filePath, newContent) {
  const existing = fs.readFileSync(filePath, 'utf8');

  // Use a longer snippet and be less aggressive
  const snippet = newContent.substring(0, 300);

  // Skip if snippet is too short or contains only whitespace
  if (snippet.length < 50 || snippet.trim().length === 0) {
    return;
  }

  const occurrences = (
    existing.match(new RegExp(escapeRegex(snippet), 'g')) || []
  ).length;

  // Allow more occurrences to be less aggressive
  if (occurrences > 5) {
    throw new Error(`Content loop detected: ${occurrences} similar sections`);
  }
}

/**
 * Escape regex special characters for safe pattern matching
 */
function escapeRegex(string) {
  return string.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
}

/**
 * Validate if a process with given PID is still running
 */
function isProcessRunning(pid) {
  try {
    // Use pgrep to check if process exists (Unix-like systems)
    execSync(`ps -p ${pid} > /dev/null 2>&1`);
    return true;
  } catch (error) {
    return false;
  }
}

/**
 * Clean up stale locks from dead processes
 */
function cleanupStaleLocks() {
  const MINUTES = 15 * 60 * 1000; // 15 minutes in milliseconds

  try {
    if (!fs.existsSync(LOCKS_DIR)) {
      return;
    }

    const lockFiles = fs
      .readdirSync(LOCKS_DIR)
      .filter((file) => file.endsWith('.lock'));

    for (const lockFile of lockFiles) {
      const lockPath = path.join(LOCKS_DIR, lockFile);

      try {
        const lockData = JSON.parse(fs.readFileSync(lockPath, 'utf8'));
        const lockTime = new Date(lockData.timestamp).getTime();
        const now = Date.now();

        // Check if lock is older than 5 minutes OR process is dead
        if (now - lockTime > MINUTES || !isProcessRunning(lockData.pid)) {
          try {
            fs.unlinkSync(lockPath);
            logWarning(`🧹 Cleaned up stale lock: ${lockFile}`);
          } catch (unlinkError) {
            logError(
              `Failed to remove stale lock ${lockFile}: ${unlinkError.message}`
            );
          }
        }
      } catch (error) {
        // If we can't read the lock file, it's probably corrupted - remove it
        try {
          fs.unlinkSync(lockPath);
          logWarning(`🧹 Removed corrupted lock file: ${lockFile}`);
        } catch (cleanupError) {
          logError(
            `Failed to cleanup lock ${lockFile}: ${cleanupError.message}`
          );
        }
      }
    }
  } catch (error) {
    logError(`Failed to cleanup stale locks: ${error.message}`);
  }

  // Also clean up stale workflow state lock
  try {
    if (fs.existsSync(WORKFLOW_STATE_LOCK_FILE)) {
      const lockData = JSON.parse(
        fs.readFileSync(WORKFLOW_STATE_LOCK_FILE, 'utf8')
      );
      const lockTime = new Date(lockData.timestamp).getTime();
      const now = Date.now();

      // Check if workflow state lock is older than 5 minutes OR process is dead
      if (now - lockTime > MINUTES || !isProcessRunning(lockData.pid)) {
        try {
          fs.unlinkSync(WORKFLOW_STATE_LOCK_FILE);
          logWarning(`🧹 Cleaned up stale workflow state lock`);
        } catch (unlinkError) {
          logError(
            `Failed to remove stale workflow state lock: ${unlinkError.message}`
          );
        }
      }
    }
  } catch (error) {
    // If we can't read the workflow state lock file, it's probably corrupted - remove it
    try {
      fs.unlinkSync(WORKFLOW_STATE_LOCK_FILE);
      logWarning(`🧹 Removed corrupted workflow state lock file`);
    } catch (cleanupError) {
      logError(
        `Failed to cleanup workflow state lock: ${cleanupError.message}`
      );
    }
  }
}

/**
 * Validate setup before running commands
 */
function validateSetup() {
  // Clean up any stale locks first
  cleanupStaleLocks();

  // Check if cursor-agent is available
  try {
    execSync('which cursor-agent', { stdio: 'pipe' });
  } catch {
    throw new Error('cursor-agent not found in PATH');
  }

  // Check BMAD rules exist
  const requiredRules = [
    'analyst.mdc',
    'pm.mdc',
    'sm.mdc',
    'architect.mdc',
    'dev.mdc',
    'qa.mdc',
  ];
  for (const rule of requiredRules) {
    const rulePath = path.join('.cursor', 'rules', 'bmad', rule);
    if (!fs.existsSync(rulePath)) {
      throw new Error(`BMAD rule missing: ${rule}`);
    }
  }

  // Check stories directory exists
  if (!fs.existsSync(STORIES_DIR)) {
    throw new Error(`Stories directory missing: ${STORIES_DIR}`);
  }
}

/**
 * Validate terminal environment (must be TTY)
 */
function validateTerminal() {
  // Temporarily disabled for testing agent output
  // if (!process.stdout.isTTY || !process.stdin.isTTY) {
  //   console.error('🚫 CRITICAL: Bots must run in visual terminal!');
  //   console.error('💡 Run: node scripts/bmad-story-orchestrator.js <command>');
  //   process.exit(1);
  // }
}

/**
 * Run bot command - process REFINED stories by priority with Architect→Dev→QA cycles
 */
async function runBot() {
  validateTerminal();
  validateSetup();

  const priority = loadPriorityOrder();
  const state = await loadWorkflowStateAtomic();

  // Get REFINED stories in priority order
  const refinedStories = priority.order
    .map((storyName) => {
      const storyPath = path.join(STORIES_DIR, `${storyName}.md`);
      if (fs.existsSync(storyPath)) {
        const story = readStoryFile(storyPath);
        if (
          story &&
          getStoryWorkflowState(story.filename, state) ===
            WORKFLOW_STATES.REFINED
        ) {
          return story;
        }
      }
      return null;
    })
    .filter((story) => story !== null);

  logInfo(
    `🤖 Processing ${refinedStories.length} refined stories in priority order`
  );

  for (const story of refinedStories) {
    // Check if shutdown was requested
    if (shutdownRequested) {
      console.log(
        `\n🛑 Shutdown requested - stopping bot processing immediately`
      );
      throw new Error('EMERGENCY_SHUTDOWN_REQUESTED');
    }

    // Re-check current state to handle concurrent updates
    const currentState = loadWorkflowState();
    const storyCurrentState = getStoryWorkflowState(
      story.filename,
      currentState
    );
    if (storyCurrentState !== WORKFLOW_STATES.REFINED) {
      logWarning(
        `⏳ Story ${story.filename} is no longer refined (state: ${storyCurrentState}) - skipping`
      );
      continue;
    }

    if (!acquireStoryLock(story.filename, 'bot')) {
      logWarning(`⏳ Skipping locked story: ${story.filename}`);
      continue;
    }

    try {
      validateFileSize(story.path);

      let qaPassed = false;
      let cycleCount = 0;
      const maxCycles = 5;

      // Architect runs once to design the solution
      logInfo(`🏗️ Architect designing solution for: ${story.filename}`);
      await runBMADAgent(
        'architect',
        'design solution architecture and write detailed implementation plan to the story file',
        story.path,
        { allowCode: false } // Documentation only
      );
      const contentAfterArchitect = fs.readFileSync(story.path, 'utf8');
      detectContentLoops(story.path, contentAfterArchitect);

      // Dev→QA cycles until implementation is 100% complete
      while (!qaPassed && cycleCount < maxCycles) {
        cycleCount++;
        logInfo(`🔄 Implementation cycle ${cycleCount} for: ${story.filename}`);

        // Dev - develop and summarize changes
        const storyKey = story.filename.replace(/\.md$/, '');
        await runBMADAgent(
          'dev',
          `develop-story ${storyKey} and summarize all changes made to the story file so QA can review what was implemented`,
          story.path,
          {
            allowCode: true,
          }
        ); // CODE ALLOWED
        const contentAfterDev = fs.readFileSync(story.path, 'utf8');
        detectContentLoops(story.path, contentAfterDev);

        // QA - comprehensive review using full BMAD review-story task
        const qaResult = await runBMADAgent(
          'qa',
          `review ${storyKey}`,
          story.path,
          {
            allowCode: true, // Allow QA to refactor code if appropriate
          }
        );

        // Check gate file for final decision (text-based analysis)
        const qaDir = path.join(__dirname, '..', 'qa');
        const gateFilePath = path.join(qaDir, 'gates', `${storyKey}.yml`);

        let qaPassed = false;
        if (fs.existsSync(gateFilePath)) {
          try {
            const gateContent = fs.readFileSync(gateFilePath, 'utf8');
            qaPassed = gateContent.includes('gate: PASS');
          } catch (error) {
            // Ignore file read errors, use text analysis
          }
        }

        // Fallback to text analysis of QA result
        if (!qaPassed) {
          qaPassed =
            qaResult.toLowerCase().includes('gate: pass') ||
            qaResult.toLowerCase().includes('ready for done') ||
            (qaResult.toLowerCase().includes('recommended status') &&
              qaResult.toLowerCase().includes('ready for done'));
        }

        if (!qaPassed) {
          // Append QA feedback to story file for Dev to see what needs to be fixed
          const qaFeedbackSection = `\n## 🔄 QA Feedback - Cycle ${cycleCount}\n\n${qaResult.trim()}\n`;
          fs.appendFileSync(story.path, qaFeedbackSection);
          logInfo(
            `📝 Added QA feedback to ${story.filename} for Dev to address`
          );

          logWarning(
            `❌ QA failed cycle ${cycleCount} - implementation not 100% complete, running Dev+QA cycle again`
          );
        }
      }

      if (qaPassed) {
        // Remove from both JSON files
        const storyKey = story.filename.replace(/\.md$/, '');
        delete state[storyKey];
        await saveWorkflowStateAtomic(state);

        // Remove from priority order
        priority.order = priority.order.filter((name) => name !== storyKey);
        savePriorityOrder(priority);

        // Delete story file
        try {
          fs.unlinkSync(story.path);
          logSuccess(`🗑️ Completed and cleaned up: ${story.filename}`);
        } catch (error) {
          logWarning(`Could not cleanup files: ${error.message}`);
        }
      } else {
        logError(
          `❌ Implementation failed after ${maxCycles} cycles: ${story.filename}`
        );
        // Don't set to error state - leave in current state for retry by another agent
      }
    } catch (error) {
      logError(
        `❌ Implementation failed for ${story.filename}: ${error.message}`
      );
      // Don't set to error state - leave in current state for retry by another agent
    } finally {
      releaseStoryLock(story.filename, 'bot');
    }
  }
}

/**
 * Run prioritize command - PM agent creates priority ordering with logical chunks
 */
async function runPrioritize() {
  validateTerminal();
  validateSetup();

  // Get ALL stories regardless of state
  const allStories = getStoryFiles();
  const storyDetails = allStories
    .map(readStoryFile)
    .filter((story) => story !== null);

  logInfo(
    `🎯 Starting prioritization with ${storyDetails.length} stories (ALL states)`
  );

  // Analyze story relationships and create logical chunks
  const storyChunks = await analyzeStoryRelationships(storyDetails);

  logInfo(`📊 Identified ${storyChunks.length} logical story chunks:`);
  storyChunks.forEach((chunk, index) => {
    logInfo(`  ${index + 1}. ${chunk.name}: ${chunk.stories.length} stories`);
    chunk.stories.forEach((story) => {
      logInfo(`     - ${story.filename}`);
    });
  });

  // Create comprehensive priority ordering that respects chunks
  const priorityOrder = await createPriorityOrderWithChunks(
    storyChunks,
    storyDetails
  );

  const priorityData = {
    order: priorityOrder,
    chunks: storyChunks.map((chunk) => ({
      name: chunk.name,
      stories: chunk.stories.map((s) => s.filename.replace(/\.md$/, '')),
      rationale: chunk.rationale,
    })),
    stopped: [],
    lastUpdated: new Date().toISOString(),
    version: 2,
  };

  savePriorityOrder(priorityData);
  logSuccess(
    `✅ Priority order created with ${priorityOrder.length} stories in ${storyChunks.length} logical chunks`
  );
}

/**
 * Analyze story relationships to create logical chunks
 */
async function analyzeStoryRelationships(storyDetails) {
  const storyList = storyDetails
    .map((story) => `- ${story.filename}`)
    .join('\n');

  const analysisPrompt = `
Analyze these user stories and group them into logical chunks where stories belong together and should be implemented in sequence. Consider:

1. **Feature Dependencies**: Stories that build upon each other
2. **Business Logic Groups**: Stories that implement related business functionality
3. **UI/UX Consistency**: Stories that affect the same user interface areas
4. **Technical Dependencies**: Stories that require the same backend infrastructure
5. **User Journey Flow**: Stories that represent steps in the same user workflow

For each chunk, provide:
- A descriptive name for the chunk
- The stories that belong in this chunk (in implementation order)
- Rationale for why these stories belong together

Stories to analyze:
${storyList}

Please read the story files directly to understand their content and requirements.

Return your analysis in a structured format that clearly identifies the chunks and their story groupings.
  `;

  const response = await runBMADAgent('pm', analysisPrompt, '/dev/null', {
    allowCode: false,
  });

  // Parse the response to extract chunks
  return parseStoryChunks(response, storyDetails);
}

/**
 * Parse story chunks from PM response
 */
function parseStoryChunks(response, allStories) {
  const chunks = [];
  const lines = response.split('\n');

  let currentChunk = null;

  for (const line of lines) {
    // Look for chunk headers (numbered items, bold text, etc.)
    const chunkMatch =
      line.match(/^(\d+)\.?\s*\*\*?(.+?)\*\*?:\s*(.+)?$/) ||
      line.match(/^[\*\-\•]\s*\*\*?(.+?)\*\*?:\s*(.+)?$/) ||
      line.match(/^#+\s*(.+?)(?:\s*-\s*(.+))?$/);

    if (chunkMatch) {
      if (currentChunk) {
        chunks.push(currentChunk);
      }

      const chunkName = chunkMatch[1] || chunkMatch[2] || 'Unnamed Chunk';
      const description = chunkMatch[2] || chunkMatch[3] || '';

      currentChunk = {
        name: chunkName.trim(),
        rationale: description.trim(),
        stories: [],
      };
    } else if (currentChunk && line.match(/^[•\-\*]\s*(.+?\.md)/)) {
      // Story reference within current chunk
      const storyMatch = line.match(/(.+?\.md)/);
      if (storyMatch) {
        const storyName = storyMatch[1];
        const story = allStories.find((s) => s.filename === storyName);
        if (story) {
          currentChunk.stories.push(story);
        }
      }
    }
  }

  if (currentChunk) {
    chunks.push(currentChunk);
  }

  // If no chunks were identified, create a single chunk with all stories
  if (chunks.length === 0) {
    chunks.push({
      name: 'All Stories',
      rationale: 'No specific chunking identified',
      stories: allStories,
    });
  }

  return chunks;
}

/**
 * Create priority order that respects logical chunks
 */
async function createPriorityOrderWithChunks(chunks, allStories) {
  // First, prioritize chunks themselves
  const chunkOrder = await prioritizeChunks(chunks);

  // Then order stories within each chunk
  const finalOrder = [];

  for (const chunkName of chunkOrder) {
    const chunk = chunks.find((c) => c.name === chunkName);
    if (chunk) {
      // Stories within chunk maintain their relative order
      const chunkStoryKeys = chunk.stories.map((s) =>
        s.filename.replace(/\.md$/, '')
      );
      finalOrder.push(...chunkStoryKeys);
    }
  }

  // Add any stories not in chunks
  const chunkedStories = new Set(
    chunks.flatMap((c) => c.stories.map((s) => s.filename))
  );
  const unchunkedStories = allStories.filter(
    (s) => !chunkedStories.has(s.filename)
  );

  if (unchunkedStories.length > 0) {
    logWarning(
      `⚠️ ${unchunkedStories.length} stories not assigned to chunks, adding at end`
    );
    finalOrder.push(
      ...unchunkedStories.map((s) => s.filename.replace(/\.md$/, ''))
    );
  }

  return finalOrder;
}

/**
 * Prioritize chunks based on business value and dependencies
 */
async function prioritizeChunks(chunks) {
  if (chunks.length <= 1) {
    return chunks.map((c) => c.name);
  }

  const chunkDescriptions = chunks.map((chunk) => ({
    name: chunk.name,
    rationale: chunk.rationale,
    storyCount: chunk.stories.length,
    storyTitles: chunk.stories.map((s) => s.filename).join(', '),
  }));

  const priorityPrompt = `
Prioritize these story chunks for implementation. Consider:

1. **Business Value**: Which chunks deliver the most customer value first?
2. **Dependencies**: Which chunks must be implemented before others?
3. **Risk Reduction**: Which chunks reduce the most risk or uncertainty?
4. **Technical Foundation**: Which chunks provide foundation for other features?

Chunks to prioritize:
${chunkDescriptions
  .map(
    (c, i) => `${i + 1}. ${c.name} (${c.storyCount} stories): ${c.rationale}
   Stories: ${c.storyTitles}`
  )
  .join('\n\n')}

Return the chunks in priority order (highest priority first), with brief rationale for the ordering.
  `;

  const response = await runBMADAgent('pm', priorityPrompt, '/dev/null', {
    allowCode: false,
  });

  // Parse chunk priority order
  return parseChunkPriorityOrder(response, chunks);
}

/**
 * Parse chunk priority order from response
 */
function parseChunkPriorityOrder(response, chunks) {
  const orderedChunks = [];
  const lines = response.split('\n');

  for (const line of lines) {
    // Look for numbered priority order
    const priorityMatch =
      line.match(/^(\d+)\.?\s*\*\*?(.+?)\*\*?/) ||
      line.match(/^[\•\-\*]\s*(.+?)(?:\s*-|:)/);

    if (priorityMatch) {
      const chunkName = priorityMatch[1] || priorityMatch[2];
      const chunk = chunks.find(
        (c) =>
          c.name.includes(chunkName.trim()) || chunkName.trim().includes(c.name)
      );
      if (chunk && !orderedChunks.includes(chunk.name)) {
        orderedChunks.push(chunk.name);
      }
    }
  }

  // If parsing failed, return original order
  if (orderedChunks.length === 0) {
    return chunks.map((c) => c.name);
  }

  return orderedChunks;
}

/**
 * Helper function to perform SM splitting - only for PM-approved stories
 */
async function performSMSplitting(story) {
  logInfo(`✂️ SM analyzing splitting for PM-approved story: ${story.filename}`);

  // Read the full story content to preserve all data
  const originalContent = fs.readFileSync(story.path, 'utf8');

  // Extract key sections to preserve in split stories
  const architectDesignMatch = originalContent.match(
    /## 🏗️ Architect Design[\s\S]*?(?=##|$)/
  );
  const analysisMatch = originalContent.match(
    /## 🔍 Analyst Findings[\s\S]*?(?=##|$)/
  );
  const pmReviewMatch = originalContent.match(
    /## 👨‍💼 PM Review[\s\S]*?(?=##|$)/
  );
  const pmReReviewMatch = originalContent.match(
    /## 🔄 PM Re-Review[\s\S]*?(?=##|$)/
  );

  const preservedSections = {
    architectDesign: architectDesignMatch ? architectDesignMatch[0] : '',
    analysis: analysisMatch ? analysisMatch[0] : '',
    pmReview: pmReviewMatch ? pmReviewMatch[0] : '',
    pmReReview: pmReReviewMatch ? pmReReviewMatch[0] : '',
  };

  const smOutput = await runBMADAgent(
    'sm',
    `This story has been approved by PM and is ready for splitting if needed.

Analyze the story complexity and determine if it should be split into smaller, independent user stories.

CRITICAL REQUIREMENTS:
- Each split story must be COMPLETELY INDEPENDENT - no shared work or dependencies
- Each story must have its own specific, actionable acceptance criteria
- Split based on functional areas, not implementation steps
- If splitting, create exactly the split stories as separate .md files in the same directory
- Name new files as: [original-name]-part[1,2,3,etc].md
- If no splitting needed, simply mark the story as refined
- PRESERVE ALL EXISTING DATA relevant for the splitted story in each new story file.
- If split: Delete the original 'big' file, but make sure no information/data is lost during splitting!

Create the split stories NOW if splitting is needed.`,
    story.path,
    { allowCode: true } // Allow SM to create new files
  );

  // Check if new story files were created by the SM agent
  const storyDir = path.dirname(story.path);
  const storyBaseName = path.basename(story.filename, '.md');
  const filesAfterSM = fs
    .readdirSync(storyDir)
    .filter(
      (file) =>
        file.endsWith('.md') &&
        file !== story.filename &&
        !file.startsWith('.') &&
        file.includes(`${storyBaseName}-part`)
    );

  if (filesAfterSM.length > 0) {
    logInfo(
      `✂️ SM split ${story.filename} into ${filesAfterSM.length} independent stories:`
    );

    // Process each split story
    for (const splitFile of filesAfterSM) {
      const splitPath = path.join(storyDir, splitFile);
      logInfo(`  📄 ${splitFile}`);

      // Read the split story content
      let splitContent = fs.readFileSync(splitPath, 'utf8');

      // Ensure all preserved sections are included
      let enhancedContent = splitContent;

      // Add architect design if not present
      if (
        preservedSections.architectDesign &&
        !enhancedContent.includes('## 🏗️ Architect Design')
      ) {
        enhancedContent += '\n' + preservedSections.architectDesign;
      }

      // Add analysis if not present
      if (
        preservedSections.analysis &&
        !enhancedContent.includes('## 🔍 Analyst Findings')
      ) {
        enhancedContent += '\n' + preservedSections.analysis;
      }

      // Add PM review if not present
      if (
        preservedSections.pmReview &&
        !enhancedContent.includes('## 👨‍💼 PM Review')
      ) {
        enhancedContent += '\n' + preservedSections.pmReview;
      }

      // Add PM re-review if not present
      if (
        preservedSections.pmReReview &&
        !enhancedContent.includes('## 🔄 PM Re-Review')
      ) {
        enhancedContent += '\n' + preservedSections.pmReReview;
      }

      // Add splitting context
      enhancedContent += `\n## ✂️ Story Splitting\n\nThis story was split from ${story.filename} by SM analysis.\n\n${smOutput.trim()}\n`;

      // Write back the enhanced content
      fs.writeFileSync(splitPath, enhancedContent);

      // Mark split story as REFINED (ready for implementation)
      const splitStoryKey = splitFile.replace(/\.md$/, '');
      const state = loadWorkflowState();
      state[splitStoryKey] = WORKFLOW_STATES.REFINED;
      saveWorkflowState(state);

      logInfo(`  ✅ ${splitFile} marked as REFINED with preserved data`);
    }

    // DELETE the original story file since it has been split
    try {
      fs.unlinkSync(story.path);
      logInfo(`🗑️ Deleted original story file: ${story.filename}`);
    } catch (error) {
      logWarning(`Could not delete original story file: ${error.message}`);
    }

    logSuccess(
      `✅ Story ${story.filename} successfully split into ${filesAfterSM.length} independent stories`
    );
  } else {
    // No splitting needed, mark as refined
    logInfo(`✅ No splitting needed for ${story.filename}`);

    // Add SM analysis to the story
    const smSection = `\n## ✂️ SM Splitting Analysis\n\n${smOutput.trim()}\n`;
    fs.appendFileSync(story.path, smSection);

    // Mark as refined
    await updateStoryWorkflowStateAtomic(
      story.filename,
      WORKFLOW_STATES.REFINED
    );
    logSuccess(`✅ Refinement complete: ${story.filename}`);
  }
}

/**
 * Run refine command - process ANALYZED and IN_REVIEW stories through PM review and SM splitting
 *
 * CRITICAL BUG FIX: PM agents cannot delete files directly. The orchestrator now handles file operations
 * based on PM response codes ("ANSWERS_SUFFICIENT" or "ANSWERS_INSUFFICIENT") to prevent stories
 * from getting stuck in IN_REVIEW state when answers are sufficient but review files aren't deleted.
 */
async function runRefine() {
  validateTerminal();
  validateSetup();

  const state = await loadWorkflowStateAtomic();
  const storyFiles = getStoryFiles();

  // Find ANALYZED and IN_REVIEW stories (refine command processes both)
  const refinementStories = storyFiles.map(readStoryFile).filter((story) => {
    if (!story) return false;
    const storyState = getStoryWorkflowState(story.filename, state);
    return (
      storyState === WORKFLOW_STATES.ANALYZED ||
      storyState === WORKFLOW_STATES.AWAITING_HUMAN_INPUT
    );
  });

  logInfo(`🔧 Found ${refinementStories.length} stories ready for refinement`);

  for (const story of refinementStories) {
    // Check if shutdown was requested
    if (shutdownRequested) {
      console.log(`\n🛑 Shutdown requested - stopping refinement immediately`);
      throw new Error('EMERGENCY_SHUTDOWN_REQUESTED');
    }

    // Re-check current state to handle concurrent updates
    const currentState = loadWorkflowState();
    const storyCurrentState = getStoryWorkflowState(
      story.filename,
      currentState
    );
    if (
      storyCurrentState !== WORKFLOW_STATES.ANALYZED &&
      storyCurrentState !== WORKFLOW_STATES.AWAITING_HUMAN_INPUT
    ) {
      logWarning(
        `⏳ Story ${story.filename} is no longer ready for refinement (state: ${storyCurrentState}) - skipping`
      );
      continue;
    }

    if (!acquireStoryLock(story.filename, 'refine')) {
      logWarning(`⏳ Skipping locked story: ${story.filename}`);
      continue;
    }

    try {
      validateFileSize(story.path);

      // PM always gets the same command for both ANALYZED and IN_REVIEW stories
      logInfo(`👨‍💼 PM reviewing: ${story.filename}`);

      const reviewFilePath = path.join(
        STORIES_DIR,
        `0_REVIEW_${story.filename.replace(/\.md$/, '')}.txt`
      );

      let pmCommand;
      if (storyCurrentState === WORKFLOW_STATES.ANALYZED) {
        // For analyzed stories: review and create review file if needed
        pmCommand = `review this story and provide feedback on business value and user experience. If you have any business logic doubts or need clarification from the product owner, create a review file with the EXACT filename "${reviewFilePath}" (note: starts with dot, ends with .txt extension) and write your specific questions (only the questions simply) to it (note: first search the big file of previous questions/answers "/REVIEW_ANSWERS.md", and other documentations from this codebase, and ONLY ask human if it is important and not already answered in the documentation!).`;
      } else if (storyCurrentState === WORKFLOW_STATES.AWAITING_HUMAN_INPUT) {
        // For in_review stories: check if human answered sufficiently
        if (fs.existsSync(reviewFilePath)) {
          pmCommand = `review this story again. Read the human answers from this review file: "${reviewFilePath}". Check if the human has answered your questions sufficiently. If yes, write your updated findings to the story file and respond with "ANSWERS_SUFFICIENT". If no, keep the review file for more answers and respond with "ANSWERS_INSUFFICIENT".`;
        } else {
          // Review file was deleted, assume answers were sufficient
          pmCommand =
            'review this story and provide final feedback on business value and user experience.';
        }
      }

      const pmOutput = await runBMADAgent('pm', pmCommand, story.path, {
        allowCode: false,
      });

      // Add or update PM review findings in the story file
      if (pmOutput.trim()) {
        const currentContent = fs.readFileSync(story.path, 'utf8');
        const reviewSection = `## 👨‍💼 PM Review\n\n${pmOutput.trim()}\n`;

        // Remove ALL existing PM Review sections first (including duplicates)
        // Match the wrapper section and all PM-generated content within it
        const reviewRegex =
          /## 👨‍💼 PM Review[\s\S]*?(?=\n## [^\n]*|\n---|\n## ✅|$)/g;
        let newContent = currentContent.replace(reviewRegex, '');

        // Also remove any orphaned PM-generated sections that might remain
        const pmContentRegex =
          /## ✅ \*\*PM Review Process Complete\*\*[\s\S]*?(?=\n## [^\n]*|\n---|\n## ✅|$)/g;
        newContent = newContent.replace(pmContentRegex, '');

        // Remove any duplicate final assessment sections
        const finalAssessmentRegex =
          /## 📊 \*\*Final Assessment\*\*[\s\S]*?(?=\n## [^\n]*|\n---|$)/g;
        // Only keep the last occurrence by replacing all but keeping track
        const finalAssessments = newContent.match(finalAssessmentRegex);
        if (finalAssessments && finalAssessments.length > 1) {
          // Remove all final assessment sections
          newContent = newContent.replace(finalAssessmentRegex, '');
          // Add back only the last one
          newContent += '\n' + finalAssessments[finalAssessments.length - 1];
        }

        // 🐛 BUGFIX: Remove additional duplicate sections that PM agents create
        // Remove duplicate "## Review Complete ✅" sections
        const reviewCompleteRegex =
          /## Review Complete ✅[\s\S]*?(?=\n## [^\n]*|\n---|$)/g;
        const reviewCompleteMatches = newContent.match(reviewCompleteRegex);
        if (reviewCompleteMatches && reviewCompleteMatches.length > 1) {
          newContent = newContent.replace(reviewCompleteRegex, '');
          // Keep only the last occurrence
          newContent +=
            '\n' + reviewCompleteMatches[reviewCompleteMatches.length - 1];
        }

        // Remove duplicate "## ✅ **Review Process Complete**" sections
        const reviewProcessRegex =
          /## ✅ \*\*Review Process Complete\*\*[\s\S]*?(?=\n## [^\n]*|\n---|$)/g;
        const reviewProcessMatches = newContent.match(reviewProcessRegex);
        if (reviewProcessMatches && reviewProcessMatches.length > 1) {
          newContent = newContent.replace(reviewProcessRegex, '');
          // Keep only the last occurrence
          newContent +=
            '\n' + reviewProcessMatches[reviewProcessMatches.length - 1];
        }

        // Remove duplicate "## 🎯 **Executive Summary**" sections
        const executiveSummaryRegex =
          /## 🎯 \*\*Executive Summary\*\*[\s\S]*?(?=\n## [^\n]*|\n---|$)/g;
        const executiveSummaryMatches = newContent.match(executiveSummaryRegex);
        if (executiveSummaryMatches && executiveSummaryMatches.length > 1) {
          newContent = newContent.replace(executiveSummaryRegex, '');
          // Keep only the last occurrence
          newContent +=
            '\n' + executiveSummaryMatches[executiveSummaryMatches.length - 1];
        }

        newContent += reviewSection;

        fs.writeFileSync(story.path, newContent);
        logInfo(`📝 Updated PM review in ${story.filename}`);
      }

      // Check for content loops after PM execution
      const contentAfterPM = fs.readFileSync(story.path, 'utf8');
      detectContentLoops(story.path, contentAfterPM);

      // Check PM response for sufficiency determination
      const pmResponse = pmOutput.toLowerCase();

      if (storyCurrentState === WORKFLOW_STATES.AWAITING_HUMAN_INPUT) {
        // For IN_REVIEW stories, check PM's sufficiency determination
        if (pmResponse.includes('answers_sufficient')) {
          // PM determined answers are sufficient - delete review file and proceed
          try {
            if (fs.existsSync(reviewFilePath)) {
              fs.unlinkSync(reviewFilePath);
              logSuccess(
                `🗑️ Deleted review file for ${story.filename} - answers sufficient`
              );
            }
          } catch (error) {
            logWarning(`Could not delete review file: ${error.message}`);
          }

          logInfo(
            `✅ PM approved ${story.filename} - proceeding to SM splitting`
          );
          await performSMSplitting(story);
        } else if (pmResponse.includes('answers_insufficient')) {
          // PM determined answers are insufficient - keep review file
          logInfo(
            `⏳ PM determined answers insufficient for ${story.filename} - keeping review file`
          );
          // Don't proceed to SM splitting
          continue;
        } else {
          // PM didn't give clear sufficiency signal - assume insufficient and keep waiting
          logWarning(
            `⚠️ PM response unclear for ${story.filename} - keeping review file for safety`
          );
          continue;
        }
      } else if (storyCurrentState === WORKFLOW_STATES.ANALYZED) {
        // For ANALYZED stories, check if review file was created
        if (fs.existsSync(reviewFilePath)) {
          // Review file was created - move to AWAITING_HUMAN_INPUT
          await updateStoryWorkflowStateAtomic(
            story.filename,
            WORKFLOW_STATES.AWAITING_HUMAN_INPUT
          );
          logWarning(
            `📝 REVIEW file created for ${story.filename} - awaiting human input`
          );
        } else {
          // No review file created - PM approved, proceed to SM splitting
          logInfo(
            `✅ PM approved ${story.filename} - proceeding to SM splitting`
          );
          await performSMSplitting(story);
        }
      }
    } catch (error) {
      logError(
        `❌ Refinement failed for ${story.filename}: ${error.message}`,
        error
      );
      // Don't set to error state - leave in current state for retry by another agent
    } finally {
      releaseStoryLock(story.filename, 'refine');
    }
  }
}

/**
 * Run analyze command - initialize all stories to READY state and process with analyst
 */
async function runAnalyze() {
  validateTerminal();
  validateSetup();

  const storyFiles = getStoryFiles();
  const state = await loadWorkflowStateAtomic();

  // Initialize all stories to READY state
  for (const storyPath of storyFiles) {
    const filename = path.basename(storyPath);
    const key = filename.replace(/\.md$/, '');
    if (!(key in state)) {
      state[key] = WORKFLOW_STATES.READY;
    }
  }
  await saveWorkflowStateAtomic(state);

  // Find ONLY READY stories or stories with no entry (initialize to READY)
  const allStories = storyFiles
    .map(readStoryFile)
    .filter((story) => story !== null);

  const readyStories = [];
  const skippedStories = [];

  for (const story of allStories) {
    const currentState = getStoryWorkflowState(story.filename, state);
    if (currentState === WORKFLOW_STATES.READY || currentState === undefined) {
      // Initialize to READY if no state exists
      if (currentState === undefined) {
        const key = story.filename.replace(/\.md$/, '');
        state[key] = WORKFLOW_STATES.READY;
      }
      readyStories.push(story);
    } else {
      skippedStories.push({
        story,
        reason: `state is ${currentState} (analyze only picks READY)`,
      });
    }
  }

  // Save state updates for newly initialized stories
  if (
    readyStories.some(
      (s) => getStoryWorkflowState(s.filename, state) === WORKFLOW_STATES.READY
    )
  ) {
    await saveWorkflowStateAtomic(state);
  }

  logInfo(`📊 Found ${readyStories.length} stories ready for analysis`);
  if (skippedStories.length > 0) {
    logInfo(`⏭️  Skipping ${skippedStories.length} stories:`);
    for (const { story, reason } of skippedStories) {
      logInfo(`   - ${story.filename}: ${reason}`);
    }
  }

  // Process READY stories one by one
  for (const story of readyStories) {
    // Check if shutdown was requested
    if (shutdownRequested) {
      console.log(`\n🛑 Shutdown requested - stopping analysis immediately`);
      throw new Error('EMERGENCY_SHUTDOWN_REQUESTED');
    }

    // Re-check current state to handle concurrent updates
    const currentState = loadWorkflowState();
    const storyCurrentState = getStoryWorkflowState(
      story.filename,
      currentState
    );
    if (storyCurrentState !== WORKFLOW_STATES.READY) {
      logWarning(
        `⏳ Story ${story.filename} is no longer ready (state: ${storyCurrentState}) - skipping`
      );
      continue;
    }

    if (!acquireStoryLock(story.filename, 'analyze')) {
      logWarning(`⏳ Skipping locked story: ${story.filename}`);
      continue; // Skip locked stories
    }

    try {
      validateFileSize(story.path);

      logInfo(`📊 Analyzing: ${story.filename}`);

      const analystOutput = await runBMADAgent(
        'analyst',
        'analyze the existing codebase and current implementation. Document technical findings and solution insights in the story file.',
        story.path,
        { allowCode: false } // STRICTLY documentation only
      );

      // Append analyst findings to the story file
      if (analystOutput.trim()) {
        const analysisSection = `\n## 🔍 Analyst Findings\n\n${analystOutput.trim()}\n`;
        fs.appendFileSync(story.path, analysisSection);
        logInfo(`📝 Added analyst findings to ${story.filename}`);
      }

      // Check for content loops after agent execution
      const contentAfter = fs.readFileSync(story.path, 'utf8');
      detectContentLoops(story.path, contentAfter);

      validateFileSize(story.path);

      updateStoryWorkflowStateAtomic(story.filename, WORKFLOW_STATES.ANALYZED);
      logSuccess(`✅ Analysis complete: ${story.filename}`);
    } catch (error) {
      logError(`❌ Analysis failed for ${story.filename}: ${error.message}`);
      // Don't set to error state - leave in current state for retry by another agent
    } finally {
      releaseStoryLock(story.filename, 'analyze');
    }
  }
}

/**
 * Validate and clean up workflow state
 */
async function validateWorkflowState() {
  logInfo('🔍 Validating workflow state...');

  const state = loadWorkflowState();
  const storyFiles = getStoryFiles();
  const existingStories = new Set(
    storyFiles.map((file) => path.basename(file, '.md'))
  );

  let cleaned = 0;
  let stuckStories = 0;

  // Remove entries for stories that no longer exist
  for (const [storyKey] of Object.entries(state)) {
    if (!existingStories.has(storyKey)) {
      delete state[storyKey];
      logWarning(`🧹 Removed stale workflow entry: ${storyKey}`);
      cleaned++;
    }
  }

  // Check for stuck IN_REVIEW stories without review files (PM bug recovery)
  for (const [storyKey, workflowState] of Object.entries(state)) {
    if (workflowState === WORKFLOW_STATES.AWAITING_HUMAN_INPUT) {
      const reviewFilePath = path.join(STORIES_DIR, `0_REVIEW_${storyKey}.txt`);

      if (!fs.existsSync(reviewFilePath)) {
        // Story is IN_REVIEW but no review file exists - this indicates the PM bug
        // where answers were sufficient but file wasn't deleted
        logWarning(
          `🐛 Found stuck story ${storyKey} - IN_REVIEW without review file`
        );

        // Check the story file to see if PM determined answers were sufficient
        const storyPath = path.join(STORIES_DIR, `${storyKey}.md`);
        if (fs.existsSync(storyPath)) {
          const content = fs.readFileSync(storyPath, 'utf8');
          if (
            content.toLowerCase().includes('answers_sufficient') ||
            content.toLowerCase().includes('human answers status: sufficient')
          ) {
            // PM determined answers were sufficient but file wasn't deleted
            // Move to REFINED state to unstick it
            state[storyKey] = WORKFLOW_STATES.REFINED;
            logSuccess(`🔧 Unstuck ${storyKey} - moved to REFINED state`);
            stuckStories++;
          } else {
            // PM hasn't reviewed yet, create a placeholder review file
            try {
              fs.writeFileSync(
                reviewFilePath,
                'REVIEW: Story review pending\n\nQUESTIONS FOR PRODUCT OWNER CLARIFICATION:\n\n1. Please review the story and provide any business logic clarification needed.\n\n**WAITING FOR PM REVIEW**'
              );
              logInfo(`📝 Created missing review file for ${storyKey}`);
              stuckStories++;
            } catch (error) {
              logError(
                `Could not create review file for ${storyKey}: ${error.message}`
              );
            }
          }
        }
      }
    }
  }

  if (cleaned > 0 || stuckStories > 0) {
    await saveWorkflowStateAtomic(state);
    logSuccess(
      `✅ Cleaned up ${cleaned} stale entries and unstuck ${stuckStories} stories`
    );
  } else {
    logSuccess('✅ Workflow state is clean and valid');
  }

  const validEntries = Object.keys(state).length;
  logInfo(`📊 Workflow state contains ${validEntries} valid entries`);
}

/**
 * Main entry point
 */
async function main() {
  const args = process.argv.slice(2);
  const command = args[0];

  // No instance locking - allow concurrent execution of all commands

  // Validate setup for all commands except help
  if (command !== 'help') {
    validateSetup();
  }
  switch (command) {
    case 'analyze':
      try {
        await runAnalyze();
      } catch (error) {
        if (error.message === 'EMERGENCY_SHUTDOWN_REQUESTED') {
          // Shutdown was requested, exit gracefully
          return;
        }
        throw error;
      }
      break;

    case 'refine':
      try {
        await runRefine();
      } catch (error) {
        if (error.message === 'EMERGENCY_SHUTDOWN_REQUESTED') {
          // Shutdown was requested, exit gracefully
          return;
        }
        throw error;
      }
      break;

    case 'bot':
      try {
        await runBot();
      } catch (error) {
        if (error.message === 'EMERGENCY_SHUTDOWN_REQUESTED') {
          // Shutdown was requested, exit gracefully
          return;
        }
        throw error;
      }
      break;

    case 'status':
      await showStatus();
      break;

    case 'prioritize':
      try {
        await runPrioritize();
      } catch (error) {
        if (error.message === 'EMERGENCY_SHUTDOWN_REQUESTED') {
          // Shutdown was requested, exit gracefully
          return;
        }
        throw error;
      }
      break;

    case 'reset': {
      const storyName = args[1];
      if (storyName) {
        await updateStoryWorkflowStateAtomic(
          `${storyName}.md`,
          WORKFLOW_STATES.READY
        );
        logSuccess(`Reset ${storyName} to READY state`);
      } else {
        logError(
          'Please specify a story name to reset (without .md extension)'
        );
      }
      break;
    }

    case 'cleanup':
      // Kill ALL cursor-agent processes and orchestrator scripts
      console.log(
        '🧹 CLEANUP: Killing ALL cursor-agent processes and orchestrator scripts...'
      );

      try {
        // Kill all cursor-agent processes aggressively
        execSync('pkill -9 -f cursor-agent 2>/dev/null || true', {
          stdio: 'pipe',
        });
        execSync('pkill -9 -f "cursor-agent.*agent" 2>/dev/null || true', {
          stdio: 'pipe',
        });
        execSync('pkill -9 -f "cursor-agent.*grok" 2>/dev/null || true', {
          stdio: 'pipe',
        });

        // Kill all bmad-story-orchestrator processes
        execSync(
          'pkill -9 -f "bmad-story-orchestrator.js" 2>/dev/null || true',
          { stdio: 'pipe' }
        );

        console.log(
          '✅ ALL cursor-agent processes and orchestrator scripts killed'
        );
      } catch (error) {
        console.log(`⚠️  Cleanup warning: ${error.message}`);
      }

      // Also validate workflow state
      await validateWorkflowState();
      break;

    case 'validate':
      await validateWorkflowState();
      break;

    case 'help':
    default:
      console.log(`
🎭 BMAD Story Orchestrator - Simple & Powerful

USAGE:
  node scripts/bmad-story-orchestrator.js <command> [options]

COMMANDS:
  analyze                Initialize new stories to READY and analyze READY stories with BMAD analyst
  refine                 Process ANALYZED and IN_REVIEW stories with PM review and SM splitting
  prioritize             PM agent creates priority ordering with convergence
  bot                    Process REFINED stories by priority with Architect→Dev→QA cycles
  status                 Show status of all stories and workflow state
  reset <story-name>     Reset a story to READY state (without .md)
  cleanup                Kill ALL cursor-agent processes and orchestrator scripts, then validate state
  validate               Validate and repair workflow state (fixes PM review bugs, unstucks stories)
  help                   Show this help

WORKFLOW SEQUENCE:
  1. analyze: Initialize new stories → READY state, process READY stories with @analyst.mdc → ANALYZED
  2. refine: Process ANALYZED and AWAITING_HUMAN_INPUT stories with @pm.mdc → REFINED or AWAITING_HUMAN_INPUT
  3. prioritize: PM agent creates priority ordering → .workflow-priority.json
  4. bot: Process REFINED stories by priority with Architect→Dev→QA cycles

REVIEW SYSTEM:
  - PM agent creates 0_REVIEW_{story}.txt files for human clarification
  - Stories marked AWAITING_HUMAN_INPUT until REVIEW is answered
  - Run 'refine' again after answering REVIEW files

BMAD INTEGRATION:
  - Uses existing @bmad/*.mdc agents properly
  - Delegates all specialized work to BMAD agents
  - No custom agent implementations or code generation

EXAMPLES:
  # Check status
  node scripts/bmad-story-orchestrator.js status

  # Reset a story
  node scripts/bmad-story-orchestrator.js reset my-story

  # Kill ALL agents and orchestrator scripts
  node scripts/bmad-story-orchestrator.js cleanup

  # Clean up state inconsistencies
  node scripts/bmad-story-orchestrator.js validate

REVIEW FILE FORMAT:
  Review files are created as: 0_REVIEW_{story-name}.txt (with dot prefix and .txt extension)
      `);
      break;
  }
}

// Global tracking of all spawned child processes for proper cleanup
const activeChildProcesses = new Map();

// Global shutdown flag - set to true when SIGINT/SIGTERM received
let shutdownRequested = false;

// Global orchestrator instance lock to prevent multiple concurrent instances
let orchestratorInstanceLock = null;

// Function to acquire orchestrator instance lock
function acquireOrchestratorInstanceLock() {
  const lockFile = path.join(STORIES_DIR, '.orchestrator-instance.lock');

  try {
    // Check if lock file exists
    if (fs.existsSync(lockFile)) {
      try {
        const lockData = JSON.parse(fs.readFileSync(lockFile, 'utf8'));
        const lockAge = Date.now() - new Date(lockData.timestamp).getTime();
        const MAX_INSTANCE_AGE = 30 * 60 * 1000; // 30 minutes max instance age

        // Check if process is still running or lock is too old
        if (!isProcessRunning(lockData.pid) || lockAge > MAX_INSTANCE_AGE) {
          logWarning(
            `🧹 Removing stale orchestrator instance lock (PID: ${lockData.pid}, age: ${Math.round(lockAge / 1000)}s)`
          );
          fs.unlinkSync(lockFile);
        } else {
          logError(
            `🚫 Another orchestrator instance is already running (PID: ${lockData.pid})`
          );
          logInfo(
            `💡 Use 'cleanup' command to force stop the running instance`
          );
          return false;
        }
      } catch (error) {
        // Lock file corrupted, remove it
        logWarning(`🔧 Removing corrupted orchestrator instance lock`);
        fs.unlinkSync(lockFile);
      }
    }

    // Create new instance lock
    const lockData = {
      pid: process.pid,
      timestamp: new Date().toISOString(),
      command: process.argv.slice(2).join(' ') || 'unknown',
    };

    fs.writeFileSync(lockFile, JSON.stringify(lockData, null, 2));
    orchestratorInstanceLock = lockFile;
    logInfo(`🔒 Acquired orchestrator instance lock`);
    return true;
  } catch (error) {
    logError(`Failed to acquire orchestrator instance lock: ${error.message}`);
    return false;
  }
}

// Function to release orchestrator instance lock
function releaseOrchestratorInstanceLock() {
  if (orchestratorInstanceLock && fs.existsSync(orchestratorInstanceLock)) {
    try {
      fs.unlinkSync(orchestratorInstanceLock);
      logInfo(`🔓 Released orchestrator instance lock`);
    } catch (error) {
      logWarning(
        `Could not release orchestrator instance lock: ${error.message}`
      );
    }
    orchestratorInstanceLock = null;
  }
}

// Function to kill all active child processes
function killAllChildProcesses(signal = 'SIGTERM') {
  if (activeChildProcesses.size > 0) {
    console.log(
      `\n🛑 EMERGENCY SHUTDOWN: Killing ${activeChildProcesses.size} active agent processes to prevent token waste...`
    );
    console.log(
      `${'🚨'.repeat(20)} COST PROTECTION ACTIVATED ${'🚨'.repeat(20)}`
    );

    for (const [agentType, child] of activeChildProcesses) {
      try {
        console.log(
          `💀 FORCE-KILLING ${agentType} agent (PID: ${child.pid}) with ${signal}`
        );
        child.kill(signal);

        // Also try SIGKILL if SIGTERM doesn't work quickly
        setTimeout(() => {
          try {
            if (!child.killed) {
              console.log(
                `🔪 SIGKILL ${agentType} agent (PID: ${child.pid}) - wasn't responding to ${signal}`
              );
              child.kill('SIGKILL');
            }
          } catch (killError) {
            // Process might already be dead
          }
        }, 500);
      } catch (error) {
        console.error(`❌ Failed to kill ${agentType} agent: ${error.message}`);
      }
    }
    activeChildProcesses.clear();

    // Give processes time to terminate cleanly
    setTimeout(() => {
      console.log(
        `✅ All agent processes terminated - token consumption stopped`
      );
      console.log(
        `${'💰'.repeat(20)} COST PROTECTION SUCCESSFUL ${'💰'.repeat(20)}`
      );
      process.exit(0);
    }, 2000);
  } else {
    console.log(`✅ No active agent processes to clean up`);
    process.exit(0);
  }
}

// Handle process termination signals - only kill our own processes
process.on('SIGINT', () => {
  console.log(
    `\n🚨 CTRL+C DETECTED - shutting down this orchestrator instance`
  );
  shutdownRequested = true; // Set global shutdown flag

  console.log(
    `🛑 Terminating ${activeChildProcesses.size} agent processes spawned by this instance...`
  );

  // Kill only the tracked processes spawned by this instance
  for (const [agentType, child] of activeChildProcesses) {
    try {
      console.log(`💀 KILLING ${agentType} agent (PID: ${child.pid})`);
      child.kill('SIGTERM');

      // Give it 1 second to terminate gracefully, then force kill
      setTimeout(() => {
        try {
          if (!child.killed) {
            console.log(
              `🔪 Force-killing ${agentType} agent (PID: ${child.pid})`
            );
            child.kill('SIGKILL');
          }
        } catch (killError) {
          // Process might already be dead
        }
      }, 1000);
    } catch (error) {
      console.error(`❌ Failed to kill ${agentType}: ${error.message}`);
    }
  }

  activeChildProcesses.clear();
  console.log(`✅ This orchestrator instance shutdown complete`);

  // Exit with SIGINT code
  process.exit(130);
});

process.on('SIGTERM', () => {
  console.log(
    `\n🚨 SIGTERM received - shutting down this orchestrator instance`
  );
  shutdownRequested = true; // Set global shutdown flag

  console.log(
    `🛑 Terminating ${activeChildProcesses.size} agent processes spawned by this instance...`
  );

  // Kill only the tracked processes spawned by this instance
  for (const [agentType, child] of activeChildProcesses) {
    try {
      console.log(`💀 KILLING ${agentType} agent (PID: ${child.pid})`);
      child.kill('SIGTERM');

      // Give it 1 second to terminate gracefully, then force kill
      setTimeout(() => {
        try {
          if (!child.killed) {
            console.log(
              `🔪 Force-killing ${agentType} agent (PID: ${child.pid})`
            );
            child.kill('SIGKILL');
          }
        } catch (killError) {
          // Process might already be dead
        }
      }, 1000);
    } catch (error) {
      console.error(`❌ Failed to kill ${agentType}: ${error.message}`);
    }
  }

  activeChildProcesses.clear();
  console.log(`✅ This orchestrator instance shutdown complete`);

  // Exit cleanly
  process.exit(0);
});

// Display active processes on exit
process.on('exit', (code) => {
  if (activeChildProcesses.size > 0) {
    console.log(
      `\n⚠️  WARNING: ${activeChildProcesses.size} agent processes may still be running!`
    );
    console.log(
      `This should not happen - please check for orphaned processes.`
    );
  }
});

// Handle uncaught errors gracefully
process.on('uncaughtException', (error) => {
  logError(`💥 Uncaught exception: ${error.message}`);

  // Kill only our own processes, not system-wide
  console.log(
    `🛑 Terminating ${activeChildProcesses.size} agent processes spawned by this instance...`
  );
  for (const [agentType, child] of activeChildProcesses) {
    try {
      console.log(`💀 KILLING ${agentType} agent (PID: ${child.pid})`);
      child.kill('SIGTERM');
      setTimeout(() => {
        try {
          if (!child.killed) {
            console.log(
              `🔪 Force-killing ${agentType} agent (PID: ${child.pid})`
            );
            child.kill('SIGKILL');
          }
        } catch (killError) {
          // Process might already be dead
        }
      }, 1000);
    } catch (error) {
      console.error(`❌ Failed to kill ${agentType}: ${error.message}`);
    }
  }
  activeChildProcesses.clear();
  process.exit(1);
});

process.on('unhandledRejection', (reason, promise) => {
  logError(`💥 Unhandled promise rejection: ${reason}`);

  // Kill only our own processes, not system-wide
  console.log(
    `🛑 Terminating ${activeChildProcesses.size} agent processes spawned by this instance...`
  );
  for (const [agentType, child] of activeChildProcesses) {
    try {
      console.log(`💀 KILLING ${agentType} agent (PID: ${child.pid})`);
      child.kill('SIGTERM');
      setTimeout(() => {
        try {
          if (!child.killed) {
            console.log(
              `🔪 Force-killing ${agentType} agent (PID: ${child.pid})`
            );
            child.kill('SIGKILL');
          }
        } catch (killError) {
          // Process might already be dead
        }
      }, 1000);
    } catch (error) {
      console.error(`❌ Failed to kill ${agentType}: ${error.message}`);
    }
  }
  activeChildProcesses.clear();
  process.exit(1);
});

// Run the orchestrator
if (require.main === module) {
  main().catch((error) => {
    logError(`💥 Fatal error: ${error.message}`);
    process.exit(1);
  });
}

module.exports = {
  getStoryFiles,
  readStoryFile,
  loadWorkflowState,
  saveWorkflowState,
  loadWorkflowStateAtomic,
  saveWorkflowStateAtomic,
  updateStoryWorkflowStateAtomic,
  showStatus,
  validateWorkflowState,
};
