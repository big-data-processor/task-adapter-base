const path = require("path")
  , os = require("os")
  , url = require("url")
  , readline = require("readline")
  , EventEmitter = require("events")
  , queue = require("queue")
  , yaml = require("js-yaml")
  , uniqid = require("uniqid")
  , dashify = require("dashify")
  , byteConvert = require("byte-converter")
  , utilities = require("@big-data-processor/utilities")
  , sleep = utilities.sleep
  , fse = utilities.fse
  , __endingWritableStreamAsync = utilities.endingWritableStreamAsync
  , renderNunjucksTemplateAsync = require("./lib/renderNunjucksTemplateAsync")
  , __getRemainingStdoe = require('./lib/getRemainingStdoe')
  , __pipeStdoeFiles = require('./lib/pipeStdoeFiles')
  , __watchStdoeFiles = require('./lib/watchStdoeFiles')
  , plotTitle = require('./lib/plotTitle')

/**
 * Internal variables
 */
let globalReadlineInterface;
let monitorTimer;
let isStopping;

/**
 * @typedef TaskDefinition
 * @type {Object}
 * @description This the format that we store for each task item.
 * @property {string} taskName The task name in the task definitions
 * @property {string} taskID The auto-generated unique id which has the taskName
 * @property {string} pid The jobID that is generated from task executors. (e.g. pid, pbs job ID, ...)
 * @property {string} image The image name in the task definitions
 * @property {string} exec The executable/command for the task in a container
 * @property {array} args The resolved task arguments in the task definitions (These arguments were prepared for different run-time environments).
 * @property {string} command The executed command for display only.
 * @property {number} start The task start timestamp in milliseconds since midnight 01, Jan, 1970 UTC
 * @property {number} end The task start timestamp in milliseconds since midnight 01, Jan, 1970 UTC
 * @property {path} stdout The file path for standard output
 * @property {path} stderr The file path for standard error
 * @property {number} cpus Used for different runtime environments; It depends on the runtime environments; Might have different meanings.
 * @property {string} mem Used for different runtime environements; It depends on the runtime environments; Might have different meanings.
 * @property {number} nodes Used for different runtime environements; Might not be used if the adapter only send a task to one single computing node;
 * @property {number} exitCode The exit code after process exit;
 * @property {array} taskArgs The arguments before transforming into the runtime arguments. Different adapters will form taskArgs into different runtime arguments, e.g. the docker adapter will make the command begins with `docker run ...`, while the pbs adapter starts with `qsub ...`.
 * @property {AdapterOption} option Additional options that initiate the adapter.
 */

/**
 * @typedef AdapterOption
 * @description The option object 
 * @type {Object}
 * @property {string} adapter The adapter name (Default = 'docker')
 * @property {number | string} cpu The required cpu cores (might have different meanings, e.g. vcpus; Default = 1)
 * @property {string} mem The memory size (Default = '4g')
 * @property {number | string} nodes The computing node number
 * @property {number} concurrency The concurrency number. If your runtime environment does not support parallel computing such as PBS. Setting to a smaller number is preferred (Default = 1).
 * @property {boolean} batch If set to true, the standard output/error will be hidden in the standard output/error of the process. Instead, a job counter of BatchStatus will be shown.
 * @property {number} updateInterval The interval in milliseconds to check the job status. Setting an appropriate number to prevent checking jobs too often or too long. The interval counter starts after each checking process. (Default = 30000).
 * @property {string} stdoeMode Specifies the ways to monitor job status and recording stdout/stderr..
 * @property {path} dockerPath
 * @property {path} projectFolder This project folder stores the users' data.
 * @property {path} packageFolder This package folder stores the package files.
 * @property {path} taskLogFolder This folder stores the task runtime logs.
 * @property {path} sharedFolder 
 * @property {path[]} extFolders Additional folders that may be required to mount to containers.
 * @property {path} cwd The current working directory for the adapter process.
 * @property {path} workdir The working directory of the in-container process.
 * @property {boolean} simplifiedTitle Set to true if you do not want to print the fancy Bio-Data Processor wellcome title.
 * @property {boolean} setUser Used to specify if the user id and group id, e.g. '--user uid: gid' for docker adapter.
 * @property {boolean} debug true/false. When debug is set to true, the adapter can display additional messages about running processes.
 * @property {Object} env Setting environment variables.
 * @property {boolean} stopOnError Set to true if you want to stop all other jobs when one of the jobs have errors. (Default = false)
 * @property {number} delayInterval The interval in milliseconds between job submissions. This delay time is set to avoid submitting too many jobs in a short time.
 * @property {boolean} forceRerun Ignore the job exitCode and re-run jobs anyway.
 * @property {*} ... Additional options can be provided for different requirements of adapters.
 */


/**
 * @typedef ProcessExit
 * @type {Object}
 * @property {number} exitCode The process exit code
 * @property {string} signal The SIGNAL to terminate the process; default is null.
 */

/**
 * @typedef ProcessRunning
 * @type {Object}
 * @property {string} jobID
 * @property {stream | number} stdoutStream Only used in the stdoeMode === 'pipe'; if in the stdoeMode of 'watch', it is used as an object storing the stream loading progress in bytes.
 * @property {stream | number} stderrStream Only used in the stdoeMode === 'pipe'; if in the stdoeMode of 'watch', it is used as an object storing the stream loading progress in bytes.
 * @property {eventemitter} taskEmitter This event emitter emits 'finish' or 'error' events when the process is finished or has errors, respectively.
 */

 /**
  * @typedef FileHandler
  * @type {Object}
  * @property {stream} stdoutFS
  * @property {stream} stderrFS
  * @property {*} stdoeWatcher an object returned by chokidar.
  * @property {object} readFileSizes An object that stores the sizes of stdout/stderr that the stdoeWatcher has been read.
  * @property {number} readFileSizes.stdout The read sizes of stdout in bytes
  * @property {number} readFileSizes.stderr The read sizes  of in bytes
  */

/**
 * @typedef BatchStatus
 * @type {Object}
 * @property {number} pending
 * @property {number} queued
 * @property {number} running
 * @property {number} finishing
 * @property {number} exit
 * @property {number} done
 * @property {number} total
 * @property {array} others
 */

/**
 * @typedef TaskAdapterInstance
 * @type {Object}
 * @property {function} getOptions use this function to get the `BdpTaskAdapter.options` object.
 * @property {function} addTasks calls the `BdpTaskAdapter._addTasks` functions to add tasks.
 * @property {function} run execute tasks and exit the Adapter process.
 * @property {function} stop stop the queue and calling beforeExit() function.
 * @property {function} execute This is the main function to execute tasks in Big Data Processor.
 *                              The adapter process wil not exit after this function. 
 */


/**
 * @interface IAdapter
 */

class IAdapter {
  /**
   * @async
   * @function IAdapter#beforeStart
   * @description An async function that preparing all required resources to run tasks.
   */
  async beforeStart() {
    return false;
  }

  /**
   * @async
   * @function IAdapter#taskOverrides
   * @param {TaskDefinition} taskObj
   * @param {string} argumentRecipe The recipe file path to transform the TaskDefinition.taskArgs into runtime arguments.
   * @return {TaskDefinition}
   * @description implement this function to overrides the task definitions.
   * Each implemented adapter should override this function to formulate your command patterns for your computing resources
   * If you use an argument-recipe yaml file to formulate your runtime arguments, remember to call `super.taskOverrides(argumentRecipe);` inside the implemented version
   * Also, developers need to change the TaskDefinition.option.mem to an accepted format. The TaskDefinition.option.mem is memory size in bytes.
   * Use the humanizeMemory filter function (@big-data-processor/utilities.memHumanize) to convert bytes into other formats, e.g. 1000000 to 1MB (or 1m); 1024 to 1KiB.
   */
  async taskOverrides(taskObj, argumentRecipe) {
    // await this._parseRecipe(taskObj, argumentRecipe);
    return taskObj;
  }

  /**
   * @async
   * @function IAdapter#processSpawn
   * @param {string} taskID
   * @description Formuate the commands to spawn
   * @return {ProcessRunning} a runningTaskObj that at least contains the taskEmitter that emits 'finished' or 'error'
   * Remember to update taskObj.command
   */
  async processSpawn(taskID) {
    return {
      jobID: this.taskLogs[taskID].pid, // The id that generted by the task executor (e.g. pid, the jobId from PBS, ...)
      stdoutStream: null,
      stderrStream: null,
      taskEmitter: new EventEmitter()// emit 'finish' or 'error' events
    };
  }

  /**
   * @async
   * @method IAdapter#processExitCallback
   * @param {string} taskID The task ID
   * @param {number} exitCode
   * @param {string} signal The terminating signals such as SIGTERM, SIGINT if exists. For normal task execution, the signal will be `null`.
   * @returns {processExit} Process exit object, e.g. {exitCode: 0, signal: null}
   */
  async processExitCallback(taskID, exitCode, signal) {
    return { exitCode: exitCode, signal: signal };
  }

  
  /**
   * @async
   * @method IAdapter#detectJobStatus
   * @method 
   * @description Some Jobs may be sent to remote computing resources so the we cannot determine when does
   * the `this.runningTasks[taskID].taskEmitter` emit the finish or error event in the processSpawn function.
   * This `detectJobStatus` function can be implemented to check if the job is finished or has errors. Returns nothing.
   */
  async detectJobStatus() {}

  /**
   * @async
   * @function IAdapter#beforeExit
   * @description An async function that cleanup all resources before exit the Adapter process and after all jobs are finished or errored.
   *
   */
  async beforeExit() {}
}





/**
 * @author Chi Yang <chiyang1118@gmail.com>
 * @class BdpTaskAdapter
 * @implements IAdapter
 * @license 
 * MIT Licensed
 * Copyright (c) 2020 Chi Yang(楊崎) <chiyang1118@gmail.com>
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */
class BdpTaskAdapter extends IAdapter {
  /**
   * @constructor
   * @param {AdapterOption} globalOption An option object. See the _setOptions.js for more information.
   * @description The constructor recieves the option object which contains information about the runtime settings. This global option will be overridden by task options.
   * The default behavior of this constructor prints the adapter name and authors.
   * Use globalOption.simplifiedTitle = true to specify a simplied title graph.
   * Feel free to change your style.
   */
  constructor(globalOption) {
    super();
    this.options = this._setOptions(globalOption);
    plotTitle(this.options.adapterName || "Customized", this.options.adapterAuthor || "anonymous", !this.options.simplifiedTitle, this.options.titleFontName);
    /**
     * @member {Object<string, TaskDefinition>} BdpTaskAdapter~taskLogs
     * @description This variable stores all the task objects.
     * @default {}
     */
    this.taskLogs = {};
    this.runningTasks = {};
    /**
     * @member {number} BdpTaskAdapter~concurrencyDelayCount
     * @description In case of setting a high concurrency number that causes submitting too many jobs at one time.
     * We added 1 to a delay count each time a job is sent. The job will be held for a delay interval times this delay count before it is processed.
     * In this way, even if there are so many jobs in a queue are submitted concurrently, the actual commands are delayed and processed one by one with the time interval.
     * Once a job is finished, there will be a small waiting time to determine whether the job submitting events are frequent.
     * If the event is not frequent, meaning that the `delayCount` remains the same after the waiting duration, the delayCount will be reset to 0.
     * @default 1
     */
    this.concurrencyDelayCount = 0;
    /**
     * @member {number} BdpTaskAdapter~delayInterval
     * @description This is a delay interval in milliseconds for concurrent job submission.
     * This prevents submitting too many jobs concurrently, especially when there are too many concurrent jobs to be submitted.
     * This can be set via options.
     * @default 300
     */
    this.delayInterval = this.options.delayInterval >= 100 ? this.options.delayInterval : 100;
    this.progressIndexFS = null; // used to block this.writeTaskLogs() if it is a truthy value.
    this.exitSignalCount = 0; // Used to count the job termination signals, if recieving more than 3 times, forcing process.exit(1).
    /**
     * @member {BatchStatus} BdpTaskAdapter~previousStatus
     * @description This object stores the progress of batch tasks.
     */
    this.previousStatus = {
      pending: 0,
      queued: 0,
      running: 0,
      finishing: 0,
      exit: 0,
      done: 0,
      others: [],
      total: 0
    };
    /**
     * @member {queue} BdpTaskAdapter~queue
     * @description A queue object from https://www.npmjs.com/package/queue
     */
    this.queue = queue({concurrency: 1, autostart: false});
    fse.ensureFile(path.resolve(this.options.taskLogFolder, "adapter-log.txt")).then(() => {
      this.logWriterFS = fse.createWriteStream(path.resolve(this.options.taskLogFolder, "error.txt"));
    }).catch(e => console.log(e));
  }

  /**
   * @function BdpTaskAdapter~_setOptions
   * @param {AdapterOption} options
   * @returns {AdapterOption}
   * @description You may using this in your adapter class, such as `super._setOptions(options)`.
   */
  _setOptions(options) {
    let returnOpt = {};
    const defaultOpt = {
      adapter: "docker",
      cpus: 1,
      mem: "4g",
      nodes: 1,
      concurrency: 0,
      env: {},
      stdoeMode: "none",
      dockerPath: options.dockerPath || process.env.DOCKER_PATH || "docker",
      projectFolder: process.env.PROJECT_FOLDER ? process.env.PROJECT_FOLDER.replace(/\\/g, "\\\\") : "",
      packageFolder: process.env.PACKAGE_FOLDER ? process.env.PACKAGE_FOLDER.replace(/\\/g, "\\\\") : "",
      extFolders: process.env.EXTERNAL_FOLDERS ? process.env.EXTERNAL_FOLDERS.split(",") : [],
      taskLogFolder: path.resolve(process.env.TASKLOG_FOLDER, process.env.RESULT_ID),
      sharedFolder: process.env.SHARED_FOLDER ? process.env.SHARED_FOLDER.replace(/\\/g, "\\\\") : undefined,
      setUser: true,
      batch: false,
      simplifiedTitle: false,
      cwd: options && options.cwd ? options.cwd : process.cwd(),
      updateInterval: options && options.updateInterval && options.updateInterval > 0 ? options.updateInterval: 30000,
      stopOnError: false,
      workdir: options && options.workdir ? options.workdir : '/project',
      debug: false,
      retry: 3,
      forceRerun: false
    };
    returnOpt = Object.assign({}, defaultOpt, options);
    let memMatch = returnOpt.mem.toUpperCase().match(/^(\d+)([KMGTPE])(i?)/);
    if (!memMatch) { memMatch = [null, 1, "G"];}
    const converter = (memMatch[3] === "i") ? byteConvert.converterBase2 : byteConvert.converterBase10;
    returnOpt.mem = converter(memMatch[1], memMatch[2] + "B", "B");
    return returnOpt;
  }


  /**
   * @function BdpTaskAdapter#printStatus
   * @param {BatchStatus} currentStatus
   * This function is used to print job progress in a batch task to stdout and stderr. You may customize this function for your prferred style.
   */
  printStatus(currentStatus) {
    process.stdout.write(`[${new Date().toString()}] status change detected.` + "\n");
    const statusOutput = [
      "-------------------------------"
      , `Pending: ${currentStatus.pending}`
      , `Queued: ${currentStatus.queued}`
      , `Running: ${currentStatus.running}`
      , `Finishing: ${currentStatus.finishing}`
      , `Error: ${currentStatus.exit}`
      , `Done: ${currentStatus.done}`
      , `Total: ${currentStatus.total}`
      , "-------------------------------"
    ];
    process.stdout.write(statusOutput.join("\n") + "\n");
  }


  /**
   * @async
   * @function BdpTaskAdapter~_processBeforeExitCallback
   * @param {string} taskID 
   * @param {FileHandler} fileHandler
   * @description This internal function will be called right after a job has exited (finished or errored) and before the `processExitCallback` function.
   * The default behavior is to get the remaining stdout/stderr messages and clean up the streaming resources.
   */
  async _processBeforeExitCallback(taskID, fileHandler) {
    /**
     * TODO: 
     * For pipe mode: prepare the stdoutFS and stderrFS
     * For watchmode: prepare the stdoeWatcher, which can be generated by chokidar.watch()
     */
    this.runningTasks[taskID].taskEmitter.removeAllListeners("finish");
    this.runningTasks[taskID].taskEmitter.removeAllListeners("error");
    delete this.runningTasks[taskID];
    const taskObj = this.taskLogs[taskID];
    const runtimeStdOut = taskObj.stdout;
    const runtimeStdErr = taskObj.stderr;
    const taskOption = taskObj.option;
    const stdoeMode = taskObj.option.stdoeMode;
    taskObj.end = new Date().valueOf();
    const currentDelay = this.concurrencyDelayCount;
    sleep(this.delayInterval*3).then(() => {
      this.concurrencyDelayCount = currentDelay === this.concurrencyDelayCount ? 0 : this.concurrencyDelayCount;
    });
    if (fileHandler) {
      if (stdoeMode === "watch" && !taskOption.batch && fileHandler.stdoeWatcher) {
        fileHandler.stdoeWatcher.close();
        const stdoutExists = await fse.pathExists(runtimeStdOut);
        const stderrExists = await fse.pathExists(runtimeStdErr);
        if (stdoutExists) { await __getRemainingStdoe("stdout", runtimeStdOut, fileHandler.readFileSizes.stdout);}
        if (stderrExists) { await __getRemainingStdoe("stderr", runtimeStdErr, fileHandler.readFileSizes.stderr);}
      } else if (stdoeMode === "pipe") {
        if (fileHandler.stdoutFS) { await __endingWritableStreamAsync(fileHandler.stdoutFS);}
        if (fileHandler.stderrFS) { await __endingWritableStreamAsync(fileHandler.stderrFS);}
      }
    }
    if (this.options.debug) {
      process.stderr.write(`[${new Date().toString()}] ${taskID} _processBeforeExitCall() finished.` + "\n");
    }
    return taskID;
  }
  /**
   * @async
   * @function BdpTaskAdapter~_processAfterExitCallback
   * @param {string} taskID 
   * @param {FileHandler} fileHandler
   * @description This internal function is called after the `processExitCallback` function.
   * The default behavior is to store the resulting stdout/stderr files.
   */
  async _processAfterExitCallback(taskID, exitObj, runtimeStdoe) {
    const taskObj = this.taskLogs[taskID];
    let msg = "";
    if (exitObj.signal) { process.stderr.write(`[${new Date().toString()}] Recieving signal: ${exitObj.signal}` + ".\n"); }
    msg =  String(exitObj.exitCode) !== "0" ?
           `[${new Date().toString()}] ${taskObj.taskID} has aborted (exit code: ${exitObj.exitCode})` + ".\n" :
           `[${new Date().toString()}] ${taskObj.taskID} has finished.`+ "\n";
    process.stderr.write(msg);
    taskObj.exitCode = exitObj.exitCode;
    if (runtimeStdoe.stderr !== this.taskLogs[taskID].stderr) {
      await fse.ensureFile(this.taskLogs[taskID].stderr);
      let runtimeStderrExists = await fse.pathExists(runtimeStdoe.stderr);
      let trialNumber = 1;
      while(!runtimeStderrExists && trialNumber <= 12) {
        runtimeStderrExists = await fse.pathExists(runtimeStdoe.stderr);
        if (runtimeStderrExists) {break;}
        trialNumber ++;
        await sleep(5000);
      }
      if (trialNumber > 12 && !runtimeStderrExists) {process.stderr.write(`The task (${taskID}) does not have the stderr file` +
        ` after ${trialNumber - 1} times to check the file.` + "\n")}
      if (runtimeStderrExists) {
        await fse.move(runtimeStdoe.stderr, this.taskLogs[taskID].stderr, { overwrite: true });
      }
    }
    if (runtimeStdoe.stdout !== this.taskLogs[taskID].stdout) {
      await fse.ensureFile(this.taskLogs[taskID].stdout);
      let runtimeStdoutExists = await fse.pathExists(runtimeStdoe.stdout);
      let trialNumber = 1;
      while(!runtimeStdoutExists && trialNumber <= 12) {
        runtimeStdoutExists = await fse.pathExists(runtimeStdoe.stdout);
        if (runtimeStdoutExists) {break;}
        trialNumber ++;
        await sleep(5000);
      }
      if (trialNumber > 5 && !runtimeStdoutExists) {process.stderr.write(`The task (${taskID}) does not have the stdout file` +
        `after ${trialNumber - 1} times to check the file.` + "\n")}
      if (runtimeStdoutExists) {
        await fse.move(runtimeStdoe.stdout, this.taskLogs[taskID].stdout, { overwrite: true });
      }
    }
    if (this.options.batch) {
      const status = await this._checkStatus();
      this.printStatus(status);
    }
    if (this.options.debug) {
      process.stderr.write(`[${new Date().toString()}] ${taskID} _processAfterExitCallback() finished.` + "\n");
    }
    // await this._writeTaskLogs();
    return;
  };
  /**
   * @async
   * @function BdpTaskAdapter~_runProcess
   * @description An async function that used to run a single job.
   * @param {string} taskID The auto-generated and uniqe task ID
   * @return {promise} A promise object that will be resovled when the task is finished normally and will be rejected when the task has errors (exitCode !== 0) or leaves unexpectedly.
   */
  async _runProcess(taskID) {
    // To prevent the job submitted too frequently. This is controlled by the delayInterval option.
    this.concurrencyDelayCount ++;
    await sleep(this.delayInterval*this.concurrencyDelayCount);
    if (isStopping) { return false; }
    const startTime = new Date().valueOf();
    const taskObj = this.taskLogs[taskID];
    const taskOption = taskObj.option;
    if (!taskObj.option) {
      this.logWriterFS.write(`${taskID} has no option in its task object.` + "\n");
    }
    const stdoeMode = taskOption.stdoeMode;
    const stopOnError = taskOption.stopOnError;
    const runningTaskObj = await this.processSpawn(taskID);
    taskObj.start = startTime;
    taskObj.end = null;
    taskObj.pid = runningTaskObj.jobID.toString();
    taskObj.exitCode = null;
    if (taskObj.stdout) {
      const stdoutExists = await fse.pathExists(taskObj.stdout);
      if (stdoutExists) {await fse.remove(taskObj.stdout);}
    }
    if (taskObj.stderr) {
      const stderrExists = await fse.pathExists(taskObj.stderr);
      if (stderrExists) {await fse.remove(taskObj.stderr);}
    }
    this.runningTasks[taskID] = runningTaskObj;
    const fileHandler = {stdoutFS: null, stderrFS: null, stdoeWatcher: null, readFileSizes: {stdout: 0, stderr: 0}};
    const runtimeStdoe = {stdout: taskObj.stdout, stderr: taskObj.stderr};
    if (stdoeMode === "pipe") {
      // In this mode, we directly pipe the stdout/stderr to files (and process.stdout/stderr if not batch mode) 
      const {stdoutFS, stderrFS} = await __pipeStdoeFiles(runtimeStdoe, runningTaskObj, taskObj.option.batch);
      fileHandler.stdoutFS = stdoutFS;
      fileHandler.stderrFS = stderrFS;
    } else if (stdoeMode === "watch" && !taskOption.batch) {
      // In the watch mode, we watch the runtimeStdOut/runtimeStdErr files (mainly for remote jobs)
      const {stdoeWatcher, readFileSizes} = await __watchStdoeFiles(runtimeStdoe, taskObj.option.batch);
      fileHandler.stdoeWatcher = stdoeWatcher;
      fileHandler.readFileSizes = readFileSizes;
    }
    let msg = `[${new Date(startTime).toString()}] Start the task ${taskObj.taskID}.` + "\n";
    process.stderr.write(msg);
    if (stdoeMode === "pipe") { fileHandler.stderrFS.write(msg); }
    msg = `[command] ${taskObj.command}` + "\n";
    process.stderr.write(msg);
    if (stdoeMode === "pipe") { fileHandler.stderrFS.write(msg); }
    // await this._writeTaskLogs();
    if (this.options.batch) {
      const status = await this._checkStatus();
      this.printStatus(status);
    }
    return new Promise((resolve, reject) => {
      this.runningTasks[taskID].taskEmitter.on("finish", (exitCode, signal) => {
        (async () => {
          await this._processBeforeExitCallback(taskID, fileHandler);
          const exitObj = await this.processExitCallback(taskID, exitCode, signal);
          await this._processAfterExitCallback(taskID, exitObj, runtimeStdoe);
          return exitObj.exitCode == "0" ? resolve(0) : (stopOnError ? reject(exitObj.exitCode) : resolve(exitObj.exitCode));
        })().catch(err => reject(err));
      });
      this.runningTasks[taskID].taskEmitter.on("error", (exitCode, signal) => {
        (async () => {
          await this._processBeforeExitCallback(taskID, fileHandler);
          const exitObj = await this.processExitCallback(taskID, exitCode, signal);
          await this._processAfterExitCallback(taskID, exitObj, runtimeStdoe);
          return stopOnError ? reject(exitObj.exitCode) : resolve(exitObj.exitCode);
        })().catch(err => reject(err));
      });
    });
  }

  /**
   * @async
   * @function BdpTaskAdapter~_parseRecipe
   * @param {TaskDefinition} taskObj 
   * @param {string} argumentRecipe The recipe file path
   * @return {TaskDefinition} 
   * @description This function can be called inside BdpTaskAdapter.taskOverrides when you implement the taskOverrides function.
   * This function basically parses the recipe file (in yaml format) and overrides the TaskDefinition object. Then, it returns the updated object.
   * (Note: the original taskObj will be mutated.)
   */
  async _parseRecipe(taskObj, argumentRecipe) {
    if (!argumentRecipe) { return taskObj; }
    /**
     * TODO: in a near future, users can specify the formats of cpu and memory using the recipe yaml file.
     * The memHumanize funtion will be humanizeMemory filter funtion in the template
     * _setOption parse the memory in the unit of bytes. With the humanizeMemory filter function, developers can get a number and unit.
     * Both base-2 or base-10 units are supported, e.g. MB and MiB.
     */
    let templateObj;
    try {
      templateObj = yaml.safeLoad(argumentRecipe);
      /**
        * TODO: Parsing templateObj['option'] for cpus, memory and other(?)
      */
      if (templateObj && templateObj["arg-recipe"]) {
        const theTaskObj = Object.assign({}, taskObj, {taskArgs: taskObj.taskArgs.map(arg => String(arg).replace(/\\/g, "\\\\"))});
        const resolvedYAMLString = await renderNunjucksTemplateAsync(
          templateObj["arg-recipe"],
          {
            option: theTaskObj.option,
            taskObj: theTaskObj,
            os: os,
            path: path,
            url: url
          }
        );
        const argArray = yaml.safeLoad(resolvedYAMLString);
        taskObj.args = Array.isArray(argArray) ? argArray.filter(arg => ((arg == "") | (arg == null) ? false : true)) : [];
      }
      return taskObj;
    } catch (e) {
      console.log("Failed to use the argument pattern file");
      console.log(e);
      process.exit(1);
    }
  }


  /**
   * @async
   * @function BdpTaskAdapter~_addTasks
   * @description An async function that construct each task object in the this.taskLogs
   * @returns {TaskDefinition[]} This process should fill the `this.taskLog` object with each process.
   */
  async _addTasks(tasks) {
    const returnTaskObjs = [];
    const loadedTaskIndices = Object.keys(this.taskLogs);
    for (let i = 0; i < tasks.length; i ++) {
      const eachTask = tasks[i];
      const taskID = uniqid(dashify(eachTask.name, { condense: true }) + "-");
      const taskOption = Object.assign({}, this.options, eachTask.option);
      const eachTaskObj = {
        taskName: eachTask.name,
        taskID: taskID,
        pid: null,
        image: eachTask.image,
        exec: eachTask.exec,
        args: eachTask.args
              .filter(arg => arg !== null && arg !== "" && arg.trim && typeof arg === "string")
              .map(arg => `"${arg.trim()}"`),
        command: eachTask.exec + " " + eachTask.args.join(" ") + "\n",
        start: null,
        end: null,
        stdout: path.resolve(taskOption.taskLogFolder, eachTask.name, taskID + "-stdout.txt"),
        stderr: path.resolve(taskOption.taskLogFolder, eachTask.name, taskID + "-stderr.txt"),
        cpus: taskOption.cpus,
        mem: taskOption.mem,
        nodes: taskOption.nodes,
        exitCode: null,
        taskArgs: eachTask.args,
        option: taskOption
      };
      const taskArgString = eachTask.args
                            .filter(a => (a ? true : false))
                            .map(a => (a && a.replace ? a.replace(/\n/g, "  ") : ""))
                            .join(" ");
      let isLoaded = false;
      for (let j = 0; j < loadedTaskIndices.length; j ++) {
        const eachLoadedTaskID = loadedTaskIndices[j];
        const loadedTaskObj = this.taskLogs[eachLoadedTaskID];
        if (taskArgString === loadedTaskObj.taskArgs &&
            eachTask.name === loadedTaskObj.taskName &&
            eachTask.image == loadedTaskObj.image &&
            eachTask.exec === loadedTaskObj.exec) {
          // let theUpdatedTaskObj = Object.assign({}, eachTaskObj, loadedTaskObj);
          eachTaskObj.taskArgs = eachTask.args;
          eachTaskObj.taskID = loadedTaskObj.taskID;
          eachTaskObj.option = taskOption;
          const theUpdatedTaskObj = await this.taskOverrides(eachTaskObj);
          theUpdatedTaskObj.pid = loadedTaskObj.pid;
          theUpdatedTaskObj.taskName = loadedTaskObj.taskName;
          theUpdatedTaskObj.exitCode = loadedTaskObj.exitCode;
          theUpdatedTaskObj.stdout = loadedTaskObj.stdout;
          theUpdatedTaskObj.stderr = loadedTaskObj.stderr;
          theUpdatedTaskObj.start = loadedTaskObj.start;
          theUpdatedTaskObj.end = loadedTaskObj.end;
          theUpdatedTaskObj.option = taskOption;
          if (loadedTaskObj.exitCode == 0 || (loadedTaskObj.start !== null && loadedTaskObj.end == null )) {
            // For tasks that will not be executed again, use the original settings;
            theUpdatedTaskObj.cpus = loadedTaskObj.cpus;
            theUpdatedTaskObj.mem = loadedTaskObj.mem;
            theUpdatedTaskObj.nodes = loadedTaskObj.nodes;
          }
          this.taskLogs[eachLoadedTaskID] = theUpdatedTaskObj;
          loadedTaskIndices.splice(j, 1);
          returnTaskObjs.push(this.taskLogs[eachLoadedTaskID]);
          isLoaded = true;
          break;
        }
      }
      if (!isLoaded) {
        this.taskLogs[taskID] = await this.taskOverrides(eachTaskObj);
        returnTaskObjs.push(this.taskLogs[taskID]);
      }
    }
    return returnTaskObjs;
  }

  /**
   * @async
   * @function BdpTaskAdapter~_checkStatus
   * @description
   * This function checks job status and return the latest status object.
   * If finished/exited jobs detected, emit finish/error event on the this.runningTasks[jobID] eventEmitter.
   * @return {BatchStatus} The updated batch status.
   */
  async _checkStatus() {
    const runningTaskIDs = Object.keys(this.runningTasks);
    const allTaskIDs = Object.keys(this.taskLogs);
    const currentStatus = {
      pending: 0,
      running: 0,
      finishing: 0,
      exit: 0,
      done: 0,
      others: [],
      total: 0
    };
    currentStatus.running = runningTaskIDs.length;
    currentStatus.total = allTaskIDs.length;
    for(let i = 0; i < allTaskIDs.length; i ++) {
      const taskID = allTaskIDs[i];
      const taskObj = this.taskLogs[taskID];
      if (!taskObj.start) {
        currentStatus.pending++;
      } else if (!taskObj.end && !this.runningTasks[taskID]) {
        // the task began but haven't all finished
        // Has start timestamp and no end timestamp. Not running
        currentStatus.finishing++;
      } else if (taskObj.end) {
        if (taskObj.exitCode == 0) {
          currentStatus.done++;
        } else {
          currentStatus.exit++;
        }
      }
    }
    return currentStatus;
  }

  /**
   * @function BdpTaskAdpater~_stopMonitor
   * @description Calls clearTimeout to remove monitor timer. As the name suggests, this function stop monitoring jobs.
   */
  _stopMonitor() {
    if (monitorTimer) {clearTimeout(monitorTimer); }
  }

  /**
   * @async
   * @function BdpTaskAdapter~_monitorTasks
   * @description This function is used to monitor job status and periodically checks job status.
   * @param {} queue The real queue object to deal the `this.runProcess` function of each job.
   */
  async _monitorTasks() {
    this._stopMonitor();
    // let checkingCounts = 0;
    const monitor = async () => {
      if (Object.keys(this.runningTasks).length === 0 && this.queue.jobs.length === 0) { return; }
      try {
        await this._writeTaskLogs();
        await this.detectJobStatus();
        const currentStatus = await this._checkStatus();
        for (const state of Object.keys(currentStatus)) {
          if (
            (state === "others" && currentStatus[state].length !== this.previousStatus[state].length) ||
            (state !== "others" && currentStatus[state] !== this.previousStatus[state])
          ) {
            if (this.options.batch) {
              this.printStatus(currentStatus);
            }
            break;
          }
        }
        this.previousStatus = currentStatus;
      } catch(e) {
        console.log(e);
      }
      await this._monitorTasks();
    };
    monitorTimer = setTimeout(() => monitor().catch(e => console.log(e)), this.options.updateInterval);
    return;
  }

  /**
   * @async
   * @function BdpTaskAdapter~_writeTaskLogs
   * @description Writing task progress to the the `progress-index.txt` file
   */
  async _writeTaskLogs() {
    if (this.progressIndexFS && this.progressIndexFS.end) {
      await new Promise((resolve) => this.progressIndexFS.end(() => {
        this.progressIndexFS = null;
        resolve();
      }));
    }
    await fse.ensureDir(this.options.taskLogFolder);
    return new Promise((resolve) => {
      try {
        this.progressIndexFS = fse.createWriteStream(path.resolve(this.options.taskLogFolder, "progress-index.txt"));
        this.progressIndexFS.write("process\ttask name\tpid\timage\texec\targs\tcommand\tstdout\tstderr\t" +
            "start time\tend time\tcpus\tmemory\tnodes\texit code\n");
        const taskIDs = Object.keys(this.taskLogs);
        for (const taskID of taskIDs) {
          const taskObj = this.taskLogs[taskID];
          const outputLogLineElems = [];
          outputLogLineElems.push.apply(outputLogLineElems, [
            taskID
            , taskObj.taskName
            , taskObj.pid == null ? "null" : taskObj.pid
            , taskObj.image || "null"
            , taskObj.exec || "null"
            , Array.isArray(taskObj.taskArgs) && taskObj.taskArgs ? taskObj.taskArgs.filter(a => a).map(a => (a && a.replace ? a.replace(/\n/g, "  ") : "")).join(" ") : taskObj.taskArgs
            , (taskObj.command ? taskObj.command.replace(/\n/g, "  ") : "")
            , taskObj.stdout || "null"
            , taskObj.stderr || "null"
            , taskObj.start ? new Date(taskObj.start).toString() : "null"
            , taskObj.end ? new Date(taskObj.end).toString() : "null"
            , taskObj.cpus
            , taskObj.mem
            , taskObj.nodes
            , taskObj.exitCode === null ? "null" : taskObj.exitCode
          ]);
          if (this.progressIndexFS && this.progressIndexFS.write) {
            this.progressIndexFS.write(outputLogLineElems.join("\t") + "\n");
          } else {
            return resolve();
          }
        }
        if (this.progressIndexFS && this.progressIndexFS.end) {
          this.progressIndexFS.end("\n", () => {
            this.progressIndexFS = null;
            resolve();
          });
        }
      } catch (e) {
        console.log(e);
        this.progressIndexFS = null;
        resolve();
        // ignore the writing error
      }
    });
  }

  /**
   * @async
   * @function BdpTaskAdapter~_loadProcessIndex
   * @description This function loads the index.txt file in the `options.taskLogFolder` to check if jobs are finished with the `exitCode` of 0 (normal exit).
   * If the job is finished, then it is skipped. This is used for pipeline resuming if the running pipelines stop unexpectedly.
   */
  async _loadProcessIndex() {
    const taskIndexFile = path.resolve(this.options.taskLogFolder, "progress-index.txt");
    const taskIndexExists = await fse.pathExists(taskIndexFile);
    if (!taskIndexExists) {return false;}
    return new Promise(resolve => {
      const taskIndexFS = fse.createReadStream(taskIndexFile);
      const rl = readline.createInterface(taskIndexFS);
      let lineNumber = 0;
      rl.on("line", line => {
        lineNumber ++;
        if (!line || !line.trim() || lineNumber === 1) { return; }
        const elements = line.split("\t");
        const originalTaskID = elements[0];
        const argumentString = elements[5];
        const exitCode = elements[14] == "null" || elements[14] == "" ? null : Number(elements[14]);
        this.taskLogs[originalTaskID] = {
          taskID: originalTaskID,
          taskName: elements[1],
          pid: elements[2] === "null" ? null : elements[2],
          image: elements[3] === "null" ? null : elements[3],
          exec: elements[4] === "null" ? null : elements[4],
          taskArgs: argumentString,
          command: elements[6],
          stdout: elements[7],
          stderr: elements[8],
          start: elements[9] == "null" ? null : new Date(elements[9]),
          end: elements[10] == "null" ? null : new Date(elements[10]),
          cpus: Number(elements[11]),
          mem: elements[12],
          nodes: elements[13],
          exitCode: this.forceRerun ? null : exitCode
        }
        if (exitCode != 0 && exitCode !== null) {
          this.taskLogs[originalTaskID].start = null;
          this.taskLogs[originalTaskID].end = null;
          this.taskLogs[originalTaskID].exitCode = null;
        }
      });
      rl.on("close", () => resolve());
    });
  }

  /**
   * @async
   * @function BdpTaskAdapter~_resumeRemoteTasks
   * @description This function trying to fetch remote jobs. 
   * This is perticularly useful when adapter process somehow closed and re-run the same adapter process.
   * Instead of directly re-creation of non-fnihsing jobs, adapter trys to fetch the job histroy.
   * Then, the adapter can see if there is a need to re-submit jobs.
   */
  async _resumeRemoteTasks() {
    const taskIDs = Object.keys(this.taskLogs);
    for (let i = 0; i < taskIDs.length; i ++) {
      const taskID = taskIDs[i];
      const taskObj = this.taskLogs[taskID];
      // Only resume the started but not yet finished tasks.
      if (taskObj.start !== null && taskObj.end == null && taskObj.option) {
        const stdoeMode = taskObj.option.stdoeMode;
        const stopOnError = taskObj.option.stopOnError;
        if (stdoeMode !== 'watch') {continue;}
        this.runningTasks[taskID] = {
          jobID: taskObj.pid, // The id that generted by the task executor (e.g. pid, the jobId from PBS, ...)
          stdoutStream: null, // The remote job does not use the pipe mode to stream the stdout/stderr stream.
          stderrStream: null,
          taskEmitter: new EventEmitter()
        };
        const runtimeStdoe = {stdout: taskObj.stdout, stderr: taskObj.stderr};
        const {stdoeWatcher, readFileSizes} = await __watchStdoeFiles(runtimeStdoe, taskObj.option.batch);
        const fileHandler = {stdoutFS: null, stderrFS: null
          , stdoeWatcher: stdoeWatcher, readFileSizes: readFileSizes};
        process.stderr.write(`[${new Date().toString()}] Resume watching the task ${taskObj.taskID}.` + "\n");
        process.stderr.write(`[command] ${taskObj.command}` + "\n");
        // ISSUE: For a very large tasks, using too many listerners will crash the adapter process.
        // TODO: dont push too many job Promise object here. Set the upper bound of the job number to monitor and then resolve this functions first.
        // Then, push the remaining job Promise object until the queue has hit the upper bound.

        this.queue.push(() => {
          return new Promise((resolve, reject) => {
            this.runningTasks[taskID].taskEmitter.on("finish", (exitCode, signal) => {
              (async () => {
                await this._processBeforeExitCallback(taskID, fileHandler);
                const exitObj = await this.processExitCallback(taskID, exitCode, signal);
                await this._processAfterExitCallback(taskID, exitObj, runtimeStdoe);
                return exitObj.exitCode == 0 ? resolve(0) : (stopOnError ? reject(exitObj.exitCode) : resolve(exitObj.exitCode));
              })().catch(err => reject(err));
            });
            this.runningTasks[taskID].taskEmitter.on("error", (exitCode, signal) => {
              (async () => {
                await this._processBeforeExitCallback(taskID, fileHandler);
                const exitObj = await this.processExitCallback(taskID, exitCode, signal);
                await this._processAfterExitCallback(taskID, exitObj, runtimeStdoe);
                return stopOnError ? reject(exitObj.exitCode) : resolve(exitObj.exitCode);
              })().catch(err =>reject(err));
            });
          });
        });
      }
    }
  }
  
  /**
   * @async
   * @function BdpTaskAdapter~_runTasks
   * @description The core function to initiate the queue to process tasks.
   */
  async _runTasks() {
    const taskLogs = this.taskLogs;
    const options = this.options;
    /**
     * The unfinished tasks are already executed by _resumeRemoteTasks (stdoeMode === 'watch'). These tasks are already in this.runningTasks
     * Only executed tasks that are not yet finished (exitCode == 0) and not in the runningTasks.
     */
    const taskIDs = Object.keys(taskLogs).filter(taskID => taskLogs[taskID].exitCode != 0 && !this.runningTasks[taskID] && this.taskLogs[taskID].option);
    if (taskIDs.length > 4 && options.concurrency > 4) {
      process.stderr.write("Force to the batch mode due to more than 3 concurrent tasks.\n");
      this.options.batch = true;
    }
    this.queue.concurrency = options.concurrency || 1;
    this.queue.autostart = false;
    for (const taskID of taskIDs) {
      this.queue.push(() => this._runProcess(taskID));
    }
    this._monitorTasks().catch(e => console.log(e));
    return new Promise((resolve, reject) => {
      this.queue.start(err => {
        this._stopMonitor();
        isStopping = true;
        if (err) {
          process.stderr.write(`[${new Date().toString()}] Task queue ends with error: ${err}.` + "\n");
          if (options.stopOnError || err === "SIGTERM") {
            (async () => {
              process.stderr.write(`[${new Date().toString()}] Begining task cleanups...` + "\n");
              await this.beforeExit();
              if (options.debug) {
                process.stderr.write(`[${new Date().toString()}] beforeExit() finished.` + "\n");
              }
              await sleep(3000);
              if (this.options.stdoeMode === 'watch') {
                process.stderr.write(`[${new Date().toString()}] Fetch job status...` + "\n");
                await this.detectJobStatus();
                const status = await this._waitingJobStatus(125000, true);
                if (status === 'timeout') {
                  process.stderr.write(`[${new Date().toString()}] Fetching timeout. ` + "\n");
                } else {
                  process.stderr.write(`[${new Date().toString()}] Fetching jobs fnished.` + "\n");
                }
              }
              /**
               * TODO: should wait until all jobs to be fully stopped!! Wait all processedAfterCallback to be finished?
               */
              process.stderr.write(`[${new Date().toString()}] Writing task logs...` + "\n");
              await this._writeTaskLogs();
            })().catch((e) => console.log(e)).finally(() => {
              process.stderr.write(`[${new Date().toString()}] Finished cleaning tasks ...` + "\n");
              reject(err);
            });
          } else {
            reject(err);
          }
        } else {
          const nonNormalExitNumber = Object.keys(taskLogs).filter(taskID => taskLogs[taskID].exitCode != 0 && this.taskLogs[taskID].option).length;
          if (nonNormalExitNumber > 0) {
            process.stderr.write(`[${new Date().toString()}] Task queue ends without error, but some tasks have exit code !== 0.` + "\n");
            (async () => {
              await this.beforeExit();
              process.stderr.write(`[${new Date().toString()}] Writing task logs...` + "\n");
              await this._writeTaskLogs();
              reject(`There are ${nonNormalExitNumber} job(s) failed (exitCode !== 0).`);
            })().catch((e) => console.log(e)).finally(() => reject(`There are ${nonNormalExitNumber} job(s) failed (exitCode !== 0).`));
          } else {
            process.stderr.write(`[${new Date().toString()}] Task queue ends without error, and all tasks have exit code 0.` + "\n");
            resolve(true);
          }
        }
      });
    });
  }

  /**
   * @async
   * @function BdpTaskAdapter~_waitingJobStatus
   * @param {*} timeout
   * @description This internal function waits until all taskObj are resolved (having their own exit codes).
   */
  async _waitingJobStatus(timeout, makeEnding) {
    let isTimeout = false, isAllResolved = false;
    const theTimeout = timeout || 120000;
    setTimeout(() => isTimeout = true, theTimeout);
    while(!isTimeout && !isAllResolved) {
      const taskIDs = Object.keys(this.taskLogs);
      isAllResolved = true;
      for (let i = 0; i < taskIDs.length; i ++) {
        const taskObj = this.taskLogs[taskIDs[i]];
        if (taskObj.exitCode === null || taskObj.exitCode === undefined) {
          isAllResolved = false;
        }
        break;
      }
      await sleep(1000);
    }
    if (isTimeout && makeEnding) {
      const taskIDs = Object.keys(this.taskLogs);
      for (let i = 0; i < taskIDs.length; i ++) {
        const taskObj = this.taskLogs[taskIDs[i]];
        if (taskObj.exitCode === null || taskObj.exitCode === undefined) {
          taskObj.end = new Date().valueOf();
          taskObj.exitCode = 4;
        }
      }
    }
    return isTimeout ? 'timeout' : 'resolved';
  }

  /**
   * @function BdpTaskAdapter#initialize
   * @returns {TaskAdapterInstance} An object that can be directly used by the developers. Used to run batch jobs.
   * @description This function must be called to get the Adapter object instance.
   */
  initialize() {
    if (globalReadlineInterface) {
      globalReadlineInterface.close();
    }
    process.nextTick(() => {
      globalReadlineInterface = readline.createInterface({
        input: process.stdin,
        output: process.stdout
      });
      globalReadlineInterface.on("line", line => {
        if (line === "bdp stop task") {
          this.queue.stop();
          isStopping = true;
          this._stopMonitor();
          process.stderr.write(`[${new Date().toString()}] bdp-task-adapter recieving the workaround stop signal.`+ "\n");
          process.stderr.write(`[${new Date().toString()}] Removing the task queue ...` + "\n");
          this.queue.end("SIGTERM");
          /*
          if (this.queue.length > 0) {
            this.queue.end(1);
          } else {
            this.beforeExit().catch(e => console.log(e)).finally(() => process.exit(1));
          }
          */
        }
      });
    });
    process.removeAllListeners("SIGINT");
    process.on("SIGINT", () => {
      // SIGINT no longer stop the running tasks.;
      this.queue.stop();
      isStopping = true;
      this._stopMonitor();
      this.exitSignalCount ++;
      process.stderr.write(`[${new Date().toString()}] bdp-task-adapter recieving the SIGINT signal.` +  "\n");
      if (this.exitSignalCount >= 5) {
        process.stderr.write('More than 5 SIGINT signals were recieved. Forcing this process to stop.\n');
        process.stderr.write('You may need to do some clean ups manually for the stopped jobs.\n');
        process.exit(1);
      }
      (async () => {
        if (this.options.stdoeMode === 'watch') {
          process.stderr.write(`[${new Date().toString()}] Fetch job status...` + "\n");
          await this.detectJobStatus();
          await sleep(10000);
          process.stderr.write(`[${new Date().toString()}] Writing task logs...` + "\n");
          await this._writeTaskLogs();
        } else {
          this.queue.end('SIGTERM');
        }
      })().catch((e) => console.log(e)).finally(() => {
        process.exit(1);
      });
    });
    process.removeAllListeners("SIGTERM");
    process.on("SIGTERM", () => {
      this.queue.stop();
      isStopping = true;
      this._stopMonitor();
      this.exitSignalCount ++;
      process.stderr.write(`[${new Date().toString()}] bdp-task-adapter recieving the SIGTERM signal.` + "\n");
      if (this.exitSignalCount >= 5) {
        process.stderr.write('More than 5 SIGINT signals were recieved. Forcing this process to stop.');
        process.stderr.write('You may need to do some clean ups for the stopped jobs.');
        process.exit(1);
      }
      process.stderr.write(`[${new Date().toString()}] Removing the task queue ...` + "\n");
      this.queue.end("SIGTERM");
    });

    return {
      getOptions: () => {
        return this.options;
      },
      run: tasks => {
        (async () => {
          try {
            if (Array.isArray(tasks)) {
              await this._addTasks(tasks);
            }
            const taskNames = Object.keys(this.taskLogs);
            if (taskNames.length <= 0) {
              throw "No task to proceed.\n";
            }
            isStopping = false;
            await this.beforeStart();
            await this._writeTaskLogs();
            await this._runTasks();
            await this._writeTaskLogs();
            await this.beforeExit();
            process.stderr.write(`[${new Date().toString()}] Finished.` + "\n");
            process.exit(0);
          } catch (e) {
            console.log(e);
            process.exit(1);
          }
        })();
      },
      addTasks: async (tasks) => {
        await this._addTasks(tasks);
      },
      execute: async tasks => {
        try {
          isStopping = false;
          // await this._loadProcessIndex();
          await this._loadProcessIndex();
          if (Array.isArray(tasks)) {await this._addTasks(tasks);}
          if (Object.keys(this.taskLogs).length <= 0) {throw "No task to proceed.\n";}
          await this._writeTaskLogs();
          await this._resumeRemoteTasks();
          await this.beforeStart();
          await this._writeTaskLogs();
          let success = false;
          let retry = 0;
          while (!success && retry <= this.options.retry) {
            try {
              if (retry > 0) {
                process.stderr.write(`[${new Date().toString()}] Retry failed tasks (${retry} time${retry > 1 ? 's' : ''}).` + "\n");
              }
              await this._runTasks();
              success = true;
            }catch(e) {
              retry ++;
              isStopping = false;
              if (e === 'SIGTERM') { throw e; }
              process.stderr.write(`[${new Date().toString()}] ${e}` + "\n");
              if (retry > this.options.retry) {
                process.stderr.write(`[${new Date().toString()}] Tasks failed after ${retry - 1} re-run(s).` + "\n");
                await this._writeTaskLogs();
                throw e;
              }
            }
          }
          await this._writeTaskLogs();
          await this.beforeExit();
          process.stderr.write(`[${new Date().toString()}] Task execution finished.` + "\n");
        } catch(e) {
          console.log(`Execute finish: ${e}`);
          throw e;
        }
      },
      stop: async () => {
        if (this.queue && this.queue.stop) {
          this.queue.end(1);
        }
        await this.beforeExit();
      }
    };
  }
}

module.exports = BdpTaskAdapter;
