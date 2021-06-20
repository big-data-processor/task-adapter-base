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
let cachedStatus;

/**
 * @typedef JobDefinition
 * @type {Object}
 * @description This the format that we store for each task item.
 * @property {string} taskName The job name in the task definitions
 * @property {string} jobId The auto-generated unique id which contains the taskName
 * @property {string} pid The jobId that is generated from the job running environment. (e.g. pid, pbs job ID, ...)
 * @property {string} image The image name in the task definitions
 * @property {string} exec The executable/command for the job in a container
 * @property {array} args The resolved arguments in the task definitions (These arguments were prepared for different run-time environments).
 * @property {string} command The executed command for display only.
 * @property {number} start The job start timestamp in milliseconds since midnight 01, Jan, 1970 UTC
 * @property {number} end The job start timestamp in milliseconds since midnight 01, Jan, 1970 UTC
 * @property {path} stdout The file path for standard output
 * @property {path} stderr The file path for standard error
 * @property {number} cpus Used for different runtime environments; It depends on the runtime environments; Might have different meanings.
 * @property {string} mem Used for different runtime environements; It depends on the runtime environments; Might have different meanings.
 * @property {number} nodes Used for different runtime environements; Might not be used if the adapter only send a task to one single computing node;
 * @property {number} exitCode The exit code after process exit;
 * @property {array} taskArgs The arguments before transforming into the runtime arguments. Different adapters will form taskArgs into different runtime arguments, e.g. the docker adapter will make the command begins with `docker run ...`, while the pbs adapter starts with `qsub ...`.
 * @property {AdapterOption} option Additional options that initiate the adapter.
 * @property {Proxy} proxy The proxy object (null if the proxy is not enabled)
 */

/**
 * @typedef Proxy
 * @type {Object}
 * @description The proxy information in a JobDefinition
 * @property {string} protocol The protocol of the proxy (currently, only the http protocol is supported)
 * @property {string} ip The ip address of the proxy
 * @property {number} port The port number of the proxy
 * @property {boolean} pathRewrite
 * @property {string} entryPath 
 * @property {number} containerPort The port number in the container
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
 * @property {string} stdoeMode Specifies the ways to monitor job status and recording stdout/stderr.
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
 * @property {'normal' | 'eager'} retryMode In the eager mode, failed task will be directly pushed back to the task queeu for rerun.
 * @property {*} ... Additional options can be provided for different requirements of adapters.
 */


/**
 * @typedef ProcessExit
 * @type {Object}
 * @property {number} exitCode The process exit code
 * @property {string} signal The SIGNAL to terminate the process; default is null.
 */

/**
 * @typedef RunningJob
 * @type {Object}
 * @property {string} runningJobId
 * @property {stream | number} stdoutStream Only used in the stdoeMode === 'pipe'; if in the stdoeMode of 'watch', it is used as an object storing the stream loading progress in bytes.
 * @property {stream | number} stderrStream Only used in the stdoeMode === 'pipe'; if in the stdoeMode of 'watch', it is used as an object storing the stream loading progress in bytes.
 * @property {eventemitter} jobEmitter This event emitter emits 'finish' or 'error' events when the process is finished or has errors, respectively.
 * @property {boolean} isRunning a flag to note if the job is queued or not running. If false, it may indicate that the job is sent to a remote resources but queued (not yet started). Used for the job counter display.
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
   * @funciton IAdapter#determineJobProxy
   * @param {JobDefinition} jobObj
   * @returns {Proxy | null}
   */
  async determineJobProxy(jobObj) {
    if (!jobObj.proxy) { return null; }
    return {
      protocol: jobObj.proxy.protocol,
      ip: jobObj.proxy.ip || '127.0.0.1',
      port: jobObj.proxy.port || undefined,
      pathRewrite: jobObj.proxy.pathRewrite.toString() === 'true' ? true : false,
      entryPath: jobObj.proxy.entryPath || '/',
      containerPort: jobObj.proxy.containerPort || undefined
    };
  }
  /**
   * @async
   * @function IAdapter#jobOverrides
   * @param {JobDefinition} jobObj
   * @param {string} argumentRecipe The recipe content to transform the JobDefinition.taskArgs into runtime arguments.
   * @return {JobDefinition}
   * @description implement this function to overrides the job definitions.
   * Each implemented adapter should override this function to formulate your command patterns for your computing resources
   * If you use an argument-recipe yaml file to formulate your runtime arguments, remember to call `await super._parseRecipe(jobObj, argumentRecipe);` inside the implemented version
   * Also, developers need to change the JobDefinition.option.mem to an accepted format. The JobDefinition.option.mem is memory size in bytes.
   * Use the humanizeMemory filter function (@big-data-processor/utilities.memHumanize) to convert bytes into other formats, e.g. 1000000 to 1MB (or 1m); 1024 to 1KiB.
   */
  async jobOverrides(jobObj, argumentRecipe) {
    await this.parseRecipe(jobObj, argumentRecipe);
    return jobObj;
  }

  /**
   * @async
   * @function IAdapter#jobDeploy
   * @param {JobDefinition} jobObj
   * @description Formuate the commands to spawn
   * @return {RunningJob} a runningJob object that at least contains the jobEmitter that emits 'finished' or 'error'
   */
  async jobDeploy(jobObj) {
    return {
      runningJobId: jobObj.pid, // The id that generted by the task executor (e.g. pid, the jobId from PBS, ...)
      stdoutStream: null,
      stderrStream: null,
      jobEmitter: new EventEmitter(), // emit 'finish' or 'error' events
      isRunning: true
    };
  }

  /**
   * @async
   * @method IAdapter#jobExitCallback
   * @param {JobDefinition} jobObj The job definition object
   * @param {number} exitCode
   * @param {string} signal The terminating signals such as SIGTERM, SIGINT if exists. For normal task execution, the signal will be `null`.
   * @returns {processExit} Process exit object, e.g. {exitCode: 0, signal: null}
   */
  async jobExitCallback(jobObj, exitCode, signal) {
    return { exitCode: exitCode, signal: signal };
  }

  
  /**
   * @async
   * @method IAdapter#detectJobStatus
   * @method 
   * @description Some Jobs may be sent to remote computing resources so the we cannot determine when does
   * the `this.runningJobs[jobId].jobEmitter` emit the finish or error event in the jobSpawn function.
   * This `detectJobStatus` function can be implemented to check if the job is finished or has errors. Returns nothing.
   */
  async detectJobStatus() {}

  /**
   * @async
   * @function IAdapter#stopAllJobs
   * @description This functions are required to stop all running jobs
   */
  async stopAllJobs() {}
  
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
 * Copyright (c) 2021 Chi Yang(楊崎) <chiyang1118@gmail.com>
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
  #jobStore;
  /**
   * @constructor
   * @param {AdapterOption} globalOption An option object. See the setOptions for more information.
   * @description The constructor recieves the option object which contains information about the runtime settings. This global option will be overridden by task options.
   * The default behavior of this constructor prints the adapter name and authors.
   * Use globalOption.simplifiedTitle = true to specify a simplied title graph.
   * Feel free to change your style.
   */
  constructor(globalOption) {
    super();
    this.options = this.setOptions(globalOption);
    plotTitle(this.options.adapterName || "Customized", this.options.adapterAuthor || "anonymous", !this.options.simplifiedTitle, this.options.titleFontName);
    /**
     * @member {Map<string, JobDefinition>} BdpTaskAdapter~taskLogs
     * @description This variable stores all the task objects.
     * @default {}
     */
    this.#jobStore = {};
    this.runningJobs = new Map();
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
   * @function BdpTaskAdapter~setOptions
   * @param {AdapterOption} options
   * @returns {AdapterOption}
   * @description You may using this in your adapter class, such as `super._setOptions(options)`.
   */
  setOptions(options) {
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
      retry: 0,
      retryMode: options.retryMode === 'eager' ? 'eager' : 'normal',
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
   * @function BdpTaskAdapter~#_printStatus
   * @param {BatchStatus} currentStatus The current status object with the fields {pending, queued, running, finishing, exit, done, total};
   * This function is used to print job progress in a batch task to stdout and stderr. You may customize this function for your prferred style.
   */

  #_printStatus({pending, queued, running, finishing, exit, done, total}) {
    if (cachedStatus) {
      if (
        cachedStatus.pending === pending &&
        cachedStatus.queued === queued &&
        cachedStatus.running === running &&
        cachedStatus.finishing === finishing &&
        cachedStatus.exit === exit &&
        cachedStatus.done === done &&
        cachedStatus.total === total
      ) {
        return;
      }
    }
    process.stdout.write(`[${new Date().toString()}] status change detected.` + "\n");
    const statusOutput = [
      "-------------------------------"
      , `Pending: ${pending}`
      , `Queued: ${queued}`
      , `Running: ${running}`
      , `Finishing: ${finishing}`
      , `Error: ${exit}`
      , `Done: ${done}`
      , `Total: ${total}`
      , "-------------------------------"
    ];
    cachedStatus = {pending, queued, running, finishing, exit, done, total};
    process.stdout.write(statusOutput.join("\n") + "\n");
  }

  /**
   * @member {boolean} BdpTaskAdapter~isStopping
   * @readonly
   * @description Using this property to understand whether the adapter is stopping.
   * You may use it to prevent extra waiting in while loops.
   */
    get isStopping() {
      return isStopping;
    }

  /**
   * @private
   * @function BdpTaskAdapter~#_sendMsgToBdpClient
   * @param {string} action The message action type. The current possible values are 'registerProxy' or 'unregisterProxy'.
   * @param {object} content The message content object of any types.
   * @description This private function sends messages to bdp-server with process.send.
   */
  #_sendMsgToBdpClient(action, content) {
    process.send({source: "bdp-adapter", action, content});
  }

  /**
   * @private
   * @function BdpTaskAdapter~#_handleProxy
   * @param {string} jobId The job ID
   * @param {Proxy} Proxy (Optional) The proxy object to register. When not giving this proxy, this function unregisters the proxy.
   * @description This private funciton handles the proxy registration and unregistration.
   */

  #_handleProxy(jobId, proxy) {
    if (!jobId) { return; }
    if (proxy) {
      const {ip, port, protocol, pathRewrite, entryPath} = proxy;
      this.#_sendMsgToBdpClient("registerProxy", {ip, port, protocol, jobId, pathRewrite, entryPath});
    } else {
      this.#_sendMsgToBdpClient("unregisterProxy", {jobId});
    }
  }

  /**
   * @async
   * @funciton BdpTaskAdapter#emitJobStatus
   * @param {*} jobId The job ID.
   * @param {*} code The job exit code 
   * @param {*} signal (optional) The job exit signal.
   * @description This function emits the 'finish' event when the job exit code is 0 or the 'error' event when else.
   * Note that calling this function does not stop the job process. This function only notify the adapter that the job is finished.
   */

  async emitJobStatus(jobId, code, signal) {
    const runningJob = this.runningJobs.get(jobId);
    if (!runningJob) { return false; }
    const jobEmitter = runningJob.jobEmitter;
    let resolved = false;
    while (!resolved) {
      await sleep(500);
      try {
        const state = code == 0 ? 'finish' : 'error';
        if (jobEmitter.listenerCount(state) >= 1) {
          jobEmitter.emit(state, code, signal);
          resolved = true;
        }
      } catch(e) {console.log(e)};
    }
    return true;
  }

  /**
   * @funciton BdpTaskAdapter#getJobById
   * @param {*} jobId The job id
   * @returns {JobDefinition} Returns the job definition object.
   * @memberof BdpTaskAdapter
   * @description This function returns the job definition object by giving the corresponding jobId.
   */
  getJobById(jobId) {
    return this.#jobStore[jobId];
  }

  /**
   * @funciton BdpTaskAdapter#getAllJobIds
   * @returns {string[]} Returns all job Ids.
   * @memberof BdpTaskAdapter
   * @description This function returns all jobIds.
   */
  getAllJobIds() {
    return Object.keys(this.#jobStore);
  }

  /**
   * @async
   * @private
   * @function BdpTaskAdapter~#_jobBeforeExitCallback
   * @param {string} jobId 
   * @param {FileHandler} fileHandler
   * @description This internal function will be called right after a job has exited (finished or errored) and before the `jobExitCallback` function.
   * The function gets the remaining stdout/stderr messages, cleans up the streaming resources and unregisters the proxy.
   */
  async #_jobBeforeExitCallback(jobId, fileHandler) {
    /**
     * TODO: 
     * For pipe mode: prepare the stdoutFS and stderrFS
     * For watchmode: prepare the stdoeWatcher, which can be generated by chokidar.watch()
     */
    if (this.runningJobs.has(jobId)) {
      this.runningJobs.get(jobId).jobEmitter.removeAllListeners("finish");
      this.runningJobs.get(jobId).jobEmitter.removeAllListeners("error");
    }
    const jobObj = this.getJobById(jobId);
    const runtimeStdOut = jobObj.stdout;
    const runtimeStdErr = jobObj.stderr;
    const taskOption = jobObj.option;
    const stdoeMode = this.options.stdoeMode;
    const currentDelay = this.concurrencyDelayCount;
    sleep(this.delayInterval*3).then(() => this.concurrencyDelayCount = currentDelay === this.concurrencyDelayCount ? 0 : this.concurrencyDelayCount);
    if (jobObj.proxy) {
      this.#_handleProxy(jobId);
    }
    if (fileHandler) {
      if (stdoeMode === "watch" && !taskOption.batch && fileHandler.stdoeWatcher) {
        fileHandler.stdoeWatcher.close();
        const stdoutExists = await fse.pathExists(runtimeStdOut);
        const stderrExists = await fse.pathExists(runtimeStdErr);
        if (stdoutExists) { await __getRemainingStdoe("stdout", runtimeStdOut, fileHandler.readFileSizes.stdout);}
        if (stderrExists) { await __getRemainingStdoe("stderr", runtimeStdErr, fileHandler.readFileSizes.stderr);}
      } else if (stdoeMode === "pipe") {
        if (fileHandler.stdoutFS) { await __endingWritableStreamAsync(fileHandler.stdoutFS); }
        if (fileHandler.stderrFS) { await __endingWritableStreamAsync(fileHandler.stderrFS); }
      }
    }
    if (this.options.debug) {
      process.stderr.write(`[${new Date().toString()}] ${jobId} #_processBeforeExitCall() finished.` + "\n");
    }
    return jobId;
  }

  async #_fetchJobStdoe(jobObj, runtimeStdoe, stdoe) {
    if (path.resolve(runtimeStdoe[stdoe]) === path.resolve(jobObj[stdoe])) { return; }
    try {
      await fse.ensureFile(jobObj[stdoe]);
      let trialNumber = 1, runtimeStdoeExists = await fse.pathExists(runtimeStdoe[stdoe]);
      while(!runtimeStdoeExists && trialNumber <= 300) {
        await sleep(2000);
        runtimeStdoeExists = await fse.pathExists(runtimeStdoe[stdoe]);
        if (runtimeStdoeExists) { break; }
        trialNumber ++;
      }
      if (runtimeStdoeExists) {
        await fse.move(runtimeStdoe[stdoe], jobObj[stdoe], { overwrite: true });
      } else if (trialNumber > 12) {
        process.stderr.write(`The job (${jobObj.jobId}) does not have the ${stdoe} file after ${trialNumber - 1} times to check the file.` + "\n");
      }
    } catch(err) {
      process.stderr.write(`We failed to get the ${stdoe} file for the job (${jobObj.jobId})` + '\n');
      console.log(err);
    }
  }

  /**
   * @async
   * @private
   * @function BdpTaskAdapter~#_jobAfterExitCallback
   * @param {string} jobId 
   * @param {FileHandler} fileHandler
   * @description This internal function is called after the `jobExitCallback` function.
   * This function stores the resulting stdout/stderr files and updates the status of all jobs.
   */
  async #_jobAfterExitCallback(jobId, exitObj, runtimeStdoe) {
    const jobObj = this.getJobById(jobId);
    let msg = "";
    if (exitObj.signal) { process.stderr.write(`[${new Date().toString()}] Recieving signal: ${exitObj.signal}` + ".\n"); }
    msg =  String(exitObj.exitCode) !== "0" ?
           `[${new Date().toString()}] ${jobObj.jobId} has aborted (exit code: ${exitObj.exitCode})` + ".\n" :
           `[${new Date().toString()}] ${jobObj.jobId} has finished.`+ "\n";
    process.stderr.write(msg);
    jobObj.exitCode = exitObj.exitCode;
    jobObj.end = new Date().valueOf();
    await (Promise.all(['stderr', 'stdout'].map(stdoe => this.#_fetchJobStdoe(jobObj, runtimeStdoe, stdoe))).catch(console.log));
    if (this.options.batch) {
      const status = await this.#_checkStatus();
      this.#_printStatus(status);
    }
    if (this.options.debug) {
      process.stderr.write(`[${new Date().toString()}] ${jobId} #_jobAfterExitCallback() finished.` + "\n");
    }
    this.runningJobs.delete(jobId);
    return;
  };
  /**
   * @async
   * @private
   * @function BdpTaskAdapter~#_runJob
   * @description An async function that used to run a single job.
   * @param {string} jobId The auto-generated and uniqe job ID
   * @return {promise} A promise object that will be resovled when the task is finished normally and will be rejected when the task has errors (exitCode !== 0) or leaves unexpectedly.
   */
  async #_runJob(jobId) {
    // To prevent the job submitted too frequently. This is controlled by the delayInterval option.
    this.concurrencyDelayCount ++;
    await sleep(this.delayInterval*this.concurrencyDelayCount);
    if (isStopping) { return false; }
    const startTime = new Date().valueOf();
    const jobObj = this.#jobStore[jobId];
    const jobOption = jobObj.option;
    if (!jobObj.option) {
      this.logWriterFS.write(`${jobId} has no option in its task object.` + "\n");
    }
    const stopOnError = this.options.stopOnError;
    if (jobObj.stdout) {
      const stdoutExists = await fse.pathExists(jobObj.stdout);
      if (stdoutExists) {await fse.remove(jobObj.stdout);}
    }
    if (jobObj.stderr) {
      const stderrExists = await fse.pathExists(jobObj.stderr);
      if (stderrExists) {await fse.remove(jobObj.stderr);}
    }
    jobObj.stdout = path.join(jobObj.option.taskLogFolder, jobObj.taskName, jobId + "-stdout.txt").replace(/\\/g, "/");
    jobObj.stderr = path.join(jobObj.option.taskLogFolder, jobObj.taskName, jobId + "-stderr.txt").replace(/\\/g, "/");
    await fse.ensureFile(jobObj.stdout);
    await fse.ensureFile(jobObj.stderr);
    const runningJob = await this.jobDeploy(jobObj);
    jobObj.start = startTime;
    jobObj.end = null;
    jobObj.pid = runningJob.runningJobId.toString();
    jobObj.exitCode = null;
    this.runningJobs.set(jobId, runningJob);
    const stdoeMode = this.options.stdoeMode;
    const fileHandler = {stdoutFS: null, stderrFS: null, stdoeWatcher: null, readFileSizes: {stdout: 0, stderr: 0}};
    const runtimeStdoe = {stdout: jobObj.stdout, stderr: jobObj.stderr};
    if (stdoeMode === "pipe") {
      // In this mode, we directly pipe the stdout/stderr to files (and process.stdout/stderr if not batch mode) 
      const {stdoutFS, stderrFS} = await __pipeStdoeFiles(runtimeStdoe, runningJob, jobObj.option.batch);
      fileHandler.stdoutFS = stdoutFS;
      fileHandler.stderrFS = stderrFS;
    } else if (stdoeMode === "watch" && !jobOption.batch) {
      // In the watch mode, we watch the runtimeStdOut/runtimeStdErr files (mainly for remote jobs)
      const {stdoeWatcher, readFileSizes} = await __watchStdoeFiles(runtimeStdoe, jobObj.option.batch);
      fileHandler.stdoeWatcher = stdoeWatcher;
      fileHandler.readFileSizes = readFileSizes;
    }
    let msg = `[${new Date(startTime).toString()}] Start the job ${jobObj.jobId}.` + "\n";
    process.stderr.write(msg);
    if (stdoeMode === "pipe") { fileHandler.stderrFS.write(msg); }
    msg = `[command] ${jobObj.command}` + "\n";
    process.stderr.write(msg);
    if (stdoeMode === "pipe") { fileHandler.stderrFS.write(msg); }
    if (this.options.batch) {
      const status = await this.#_checkStatus();
      this.#_printStatus(status);
    }
    (async () => {
      jobObj.proxy = await this.determineJobProxy(jobObj);
      jobObj.proxy = jobObj.proxy || null;
      this.#_handleProxy(jobId, jobObj.proxy);
      await this.#_writeJobLogs();
    })().catch(err => {
      console.log(err);
      process.stderr.write(`[Task Adapter] Failed to run the determineJobProxy function to get the proxy object. Unreigster the proxy.`);
      this.#_handleProxy(jobId);
      jobObj.proxy = null;
    });
    return new Promise((resolve, reject) => {
      this.runningJobs.get(jobId).jobEmitter.on("finish", (exitCode, signal) => {
        (async () => {
          await this.#_jobBeforeExitCallback(jobId, fileHandler);
          const exitObj = await this.jobExitCallback(jobObj, exitCode, signal);
          await this.#_jobAfterExitCallback(jobId, exitObj, runtimeStdoe);
          return exitObj.exitCode == "0" ? resolve(0) : (stopOnError ? reject(exitObj.exitCode) : resolve(exitObj.exitCode));
        })().catch(err => reject(err));
      });
      this.runningJobs.get(jobId).jobEmitter.on("error", (exitCode, signal) => {
        (async () => {
          await this.#_jobBeforeExitCallback(jobId, fileHandler);
          const exitObj = await this.jobExitCallback(jobObj, exitCode, signal);
          await this.#_jobAfterExitCallback(jobId, exitObj, runtimeStdoe);
          if (this.options.retryMode === 'eager') {
            if (!jobObj.retry) { jobObj.retry = 0; }
            jobObj.retry ++;
            if (jobObj.retry <= this.options.retry) {
              this.queue.push(() => this.#_runJob(jobId));
            }
          }
          return stopOnError ? reject(exitObj.exitCode) : resolve(exitObj.exitCode);
        })().catch(err => reject(err));
      });
    });
  }

  /**
   * @async
   * @function BdpTaskAdapter#parseRecipe
   * @param {JobDefinition} jobObj The job object
   * @param {string} argumentRecipe The recipe file path
   * @return {JobDefinition} 
   * @description This function can be called inside BdpTaskAdapter.jobOverrides when you implement the jobOverrides function.
   * This function basically parses the recipe file (in yaml format) and overrides the JobDefinition object. Then, it returns the updated job object.
   * (Note: the original job object will be mutated.)
   */
  async parseRecipe(jobObj, argumentRecipe) {
    if (!argumentRecipe) { return jobObj; }
    /**
     * TODO: in a near future, users can specify the formats of cpu and memory using the recipe yaml file.
     * The memHumanize funtion will be humanizeMemory filter funtion in the template
     * _setOption parse the memory in the unit of bytes. With the humanizeMemory filter function, developers can get a number and unit.
     * Both base-2 or base-10 units are supported, e.g. MB and MiB.
     */
    let templateObj;
    try {
      templateObj = yaml.load(argumentRecipe);
      /**
        * TODO: Parsing templateObj['option'] for cpus, memory and other(?)
      */
      if (templateObj && templateObj["arg-recipe"]) {
        const theJobObj = Object.assign({}, jobObj, {taskArgs: jobObj.taskArgs.map(arg => String(arg).replace(/\\/g, "\\\\"))});
        const resolvedYAMLString = await renderNunjucksTemplateAsync(
          templateObj["arg-recipe"],
          {
            option: theJobObj.option,
            jobObj: theJobObj,
            os: os,
            path: path,
            url: url
          }
        );
        const argArray = yaml.load(resolvedYAMLString);
        jobObj.args = Array.isArray(argArray) ? argArray.filter(arg => ((arg == "") | (arg == null) ? false : true)) : [];
      }
      return jobObj;
    } catch (e) {
      console.log("Failed to use the argument pattern file");
      console.log(e);
      process.exit(1);
    }
  }

  /**
   * @async
   * @private
   * @function BdpTaskAdapter~#_addJobs
   * @description An async function that construct each task object in the this.jobStore
   * @returns {JobDefinition[]} This process should fill the `this.taskLog` object with each process.
   */
  async #_addJobs(tasks) {
    const returnJobObjs = [];
    const loadedJobIds = Object.keys(this.#jobStore);
    for (let i = 0; i < tasks.length; i ++) {
      const eachTask = tasks[i];
      const jobId = uniqid(dashify(eachTask.name, { condense: true }) + "-");
      const taskOption = Object.assign({}, this.options, eachTask.option);
      const eachJobObj = {
        taskName: eachTask.name,
        jobId: jobId,
        pid: null,
        image: eachTask.image,
        exec: eachTask.exec,
        args: eachTask.args
              .filter(arg => arg !== null && arg !== "" && arg.trim && typeof arg === "string")
              .map(arg => `"${arg.trim()}"`),
        command: eachTask.exec + " " + eachTask.args.join(" ") + "\n",
        start: null,
        end: null,
        stdout: path.resolve(taskOption.taskLogFolder, eachTask.name, jobId + "-stdout.txt"),
        stderr: path.resolve(taskOption.taskLogFolder, eachTask.name, jobId + "-stderr.txt"),
        cpus: taskOption.cpus,
        mem: taskOption.mem,
        nodes: taskOption.nodes,
        exitCode: null,
        taskArgs: eachTask.args,
        option: taskOption,
        proxy: process.env.BDP_PROXY_ENABLED === 'true' && eachTask.proxy ? Object.assign({}, {
          ip: process.env.BDP_PROXY_IP,
          port: process.env.BDP_PROXY_PORT,
          protocol: process.env.BDP_PROXY_PROTOCOL,
          pathRewrite: process.env.BDP_PROXY_PATHREWRITE == 'true' ? true : false,
          entryPath: process.env.BDP_PROXY_ENTRYPATH,
        }, eachTask.proxy) : null
      };
      const taskArgString = eachTask.args
                            .filter(a => (a ? true : false))
                            .map(a => (a && a.replace ? a.replace(/\n/g, "  ") : ""))
                            .join(" ");
      let isLoaded = false;
      for (let j = 0; j < loadedJobIds.length; j ++) {
        const eachLoadedJobId = loadedJobIds[j];
        const loadedJobObj = this.getJobById(eachLoadedJobId);
        if (taskArgString === loadedJobObj.taskArgs &&
            eachTask.name === loadedJobObj.taskName &&
            eachTask.image == loadedJobObj.image &&
            eachTask.exec === loadedJobObj.exec) {
          eachJobObj.taskArgs = eachTask.args;
          eachJobObj.jobId = loadedJobObj.jobId;
          eachJobObj.option = taskOption;
          if (loadedJobObj.proxy && process.env.BDP_PROXY_ENABLED === 'true') {
            eachJobObj.proxy = loadedJobObj.proxy;
          } else if (!loadedJobObj.proxy && process.env.BDP_PROXY_ENABLED === 'true' && eachTask.proxy) {
            eachJobObj.proxy = Object.assign({}, {
              ip: process.env.BDP_PROXY_IP,
              port: process.env.BDP_PROXY_PORT,
              protocol: process.env.BDP_PROXY_PROTOCOL,
              pathRewrite: process.env.BDP_PROXY_PATHREWRITE == 'true' ? true : false,
              entryPath: process.env.BDP_PROXY_ENTRYPATH,
            }, eachTask.proxy);
          } else {
            eachJobObj.proxy = null;
          }
          const theUpdatedJobObj = await this.jobOverrides(eachJobObj);
          theUpdatedJobObj.pid = loadedJobObj.pid;
          theUpdatedJobObj.taskName = loadedJobObj.taskName;
          theUpdatedJobObj.exitCode = loadedJobObj.exitCode;
          theUpdatedJobObj.stdout = loadedJobObj.stdout;
          theUpdatedJobObj.stderr = loadedJobObj.stderr;
          theUpdatedJobObj.start = loadedJobObj.start;
          theUpdatedJobObj.end = loadedJobObj.end;
          theUpdatedJobObj.option = taskOption;
          if (loadedJobObj.exitCode == 0 || (loadedJobObj.start !== null && loadedJobObj.end == null )) {
            // For tasks that will not be executed again, use the original settings;
            theUpdatedJobObj.cpus = loadedJobObj.cpus;
            theUpdatedJobObj.mem = loadedJobObj.mem;
            theUpdatedJobObj.nodes = loadedJobObj.nodes;
          }
          this.#jobStore[eachLoadedJobId] = theUpdatedJobObj;
          loadedJobIds.splice(j, 1);
          returnJobObjs.push(this.#jobStore[eachLoadedJobId]);
          isLoaded = true;
          break;
        }
      }
      if (!isLoaded) {
        this.#jobStore[jobId] = await this.jobOverrides(eachJobObj);
        returnJobObjs.push(this.#jobStore[jobId]);
      }
    }
    return returnJobObjs;
  }

  /**
   * @async
   * @private
   * @function BdpTaskAdapter~#_checkStatus
   * @description
   * This function checks job status and return the latest status object.
   * If finished/exited jobs detected, emit finish/error event on the this.runningJobs.get(jobId).jobEmitter eventEmitter.
   * @return {BatchStatus} The updated batch status.
   */
  async #_checkStatus() {
    const jobIds = [...this.runningJobs.keys()];
    const allJobIDs = Object.keys(this.#jobStore);
    const currentStatus = {
      pending: 0,
      queued: 0,
      running: 0,
      finishing: 0,
      exit: 0,
      done: 0,
      others: [],
      total: 0
    };
    currentStatus.queued = jobIds.filter(jobId => !this.runningJobs.get(jobId)?.isRunning).length;
    currentStatus.running = jobIds.length - currentStatus.queued;
    currentStatus.total = allJobIDs.length - allJobIDs.filter(jobId => !this.#jobStore[jobId].option).length;
    for(let i = 0; i < allJobIDs.length; i ++) {
      const jobId = allJobIDs[i];
      const jobObj = this.#jobStore[jobId];
      if (!jobObj.option) { continue; }
      if (!jobObj.start) {
        currentStatus.pending++;
      } else if (!jobObj.end && !this.runningJobs.has(jobId)) {
        // the task began but haven't all finished
        // Has start timestamp and no end timestamp. Not running
        currentStatus.finishing++;
      } else if (jobObj.end) {
        if (jobObj.exitCode == 0) {
          currentStatus.done++;
        } else {
          currentStatus.exit++;
        }
      }
    }
    return currentStatus;
  }

  /**
   * @private
   * @function BdpTaskAdpater~#_stopMonitor
   * @description Calls clearTimeout to remove monitor timer. As the name suggests, this function stop monitoring jobs.
   */
  #_stopMonitor() {
    if (monitorTimer) {clearTimeout(monitorTimer); }
  }

  /**
   * @async
   * @private
   * @function BdpTaskAdapter~#_monitorJobs
   * @description This function is used to monitor job status and periodically checks job status.
   * @param {} queue The real queue object to deal the `this.runProcess` function of each job.
   */
  async #_monitorJobs() {
    this.#_stopMonitor();
    // let checkingCounts = 0;
    const monitor = async () => {
      if (this.runningJobs.size === 0 && this.queue.jobs.length === 0) { return; }
      try {
        await this.#_writeJobLogs();
        await this.detectJobStatus();
        const currentStatus = await this.#_checkStatus();
        for (const state of Object.keys(currentStatus)) {
          if (
            (state === "others" && currentStatus[state].length !== this.previousStatus[state].length) ||
            (state !== "others" && currentStatus[state] !== this.previousStatus[state])
          ) {
            if (this.options.batch) {
              this.#_printStatus(currentStatus);
            }
            break;
          }
        }
        this.previousStatus = currentStatus;
      } catch(e) {
        console.log(e);
      }
      await this.#_monitorJobs();
    };
    monitorTimer = setTimeout(() => monitor().catch(e => console.log(e)), this.options.updateInterval);
    return;
  }

  /**
   * @async
   * @private
   * @function BdpTaskAdapter~#_writeJobLogs
   * @description Writing task progress to the the `progress-index.txt` file
   */
  async #_writeJobLogs() {
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
            "start time\tend time\tcpus\tmemory\tnodes\texit code\tproxy\n");
        const jobIds = Object.keys(this.#jobStore);
        for (const jobId of jobIds) {
          const jobObj = this.#jobStore[jobId];
          const outputLogLineElems = [];
          outputLogLineElems.push.apply(outputLogLineElems, [
            jobId
            , jobObj.taskName
            , jobObj.pid == null ? "null" : jobObj.pid
            , jobObj.image || "null"
            , jobObj.exec || "null"
            , Array.isArray(jobObj.taskArgs) && jobObj.taskArgs ? jobObj.taskArgs.filter(a => a).map(a => (a && a.replace ? a.replace(/\n/g, "  ") : "")).join(" ") : jobObj.taskArgs
            , (jobObj.command ? jobObj.command.replace(/\n/g, "  ") : "")
            , jobObj.stdout || "null"
            , jobObj.stderr || "null"
            , jobObj.start ? new Date(jobObj.start).toString() : "null"
            , jobObj.end ? new Date(jobObj.end).toString() : "null"
            , jobObj.cpus
            , jobObj.mem
            , jobObj.nodes
            , jobObj.exitCode === null ? "null" : jobObj.exitCode
            , jobObj.proxy === null ? "null" : JSON.stringify(jobObj.proxy)
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
   * @private
   * @function BdpTaskAdapter~#_loadProgress
   * @description This function loads the index.txt file in the `options.taskLogFolder` to check if jobs are finished with the `exitCode` of 0 (normal exit).
   * If the job is finished, then it is skipped. This is used for pipeline resuming if the running pipelines stop unexpectedly.
   */
  async #_loadProgress() {
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
        const originalJobID = elements[0];
        const argumentString = elements[5];
        const exitCode = elements[14] == "null" || elements[14] == "" ? null : Number(elements[14]);
        this.#jobStore[originalJobID] = {
          jobId: originalJobID,
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
          exitCode: this.forceRerun ? null : exitCode,
          proxy: elements[15] == "null" | undefined ? null : JSON.parse(elements[15])
        }
        if (exitCode != 0 && exitCode !== null) {
          this.#jobStore[originalJobID].start = null;
          this.#jobStore[originalJobID].end = null;
          this.#jobStore[originalJobID].exitCode = null;
        }
      });
      rl.on("close", () => resolve());
    });
  }

  /**
   * @async
   * @private
   * @function BdpTaskAdapter~#_resumeJobs
   * @description This function trying to fetch remote jobs. 
   * This is perticularly useful when adapter process somehow closed and re-run the same adapter process.
   * Instead of directly re-creation of non-fnihsing jobs, adapter trys to fetch the job histroy.
   * Then, the adapter can see if there is a need to re-submit jobs.
   */
  async #_resumeJobs() {
    const jobIds = Object.keys(this.#jobStore);
    let batchCounter = 0;
    for (let i = 0; i < jobIds.length; i ++) {
      const jobId = jobIds[i];
      const jobObj = this.#jobStore[jobId];
      // Only resume the started but not yet finished tasks.
      if (jobObj.start !== null && jobObj.end == null && jobObj.option) {
        const stdoeMode = this.options.stdoeMode;
        const stopOnError = this.options.stopOnError;
        if (stdoeMode !== 'watch') {continue;}
        this.runningJobs.set(jobId, {
          jobId: jobObj.pid, // The id that generted by the task executor (e.g. pid, the jobId from PBS, ...)
          stdoutStream: null, // The remote job does not use the pipe mode to stream the stdout/stderr stream.
          stderrStream: null,
          jobEmitter: new EventEmitter(),
          isRunning: false
        });
        const runtimeStdoe = {stdout: jobObj.stdout, stderr: jobObj.stderr};
        const {stdoeWatcher, readFileSizes} = await __watchStdoeFiles(runtimeStdoe, jobObj.option.batch);
        const fileHandler = {stdoutFS: null, stderrFS: null, stdoeWatcher: stdoeWatcher, readFileSizes: readFileSizes};
        process.stderr.write(`[${new Date().toString()}] Resume watching the job ${jobObj.jobId}.` + "\n");
        process.stderr.write(`[command] ${jobObj.command}` + "\n");
        (async () => {
          jobObj.proxy = await this.determineJobProxy(jobObj);
          jobObj.proxy = jobObj.proxy || null;
          this.#_handleProxy(jobId, jobObj.proxy);
          await this.#_writeJobLogs();
        })().catch(err => {
          console.log(err);
          process.stderr.write(`[Task Adapter] Failed to run the determineJobProxy function to get the proxy object.`);
          this.#_handleProxy(jobId);
          jobObj.proxy = null;
        });
        // ISSUE: For a very large tasks, using too many listerners will crash the adapter process.
        // TODO: dont push too many job Promise object here. Set the upper bound of the job number to monitor and then resolve this functions first.
        // Then, push the remaining job Promise object until the queue has hit the upper bound.
        batchCounter ++;
        if (batchCounter > 100) {
          await sleep(500);
          batchCounter = 0;
        }
        this.queue.push(() => {
          return new Promise((resolve, reject) => {
            this.runningJobs.get(jobId).jobEmitter.on("finish", (exitCode, signal) => {
              (async () => {
                await this.#_jobBeforeExitCallback(jobId, fileHandler);
                const exitObj = await this.jobExitCallback(jobObj, exitCode, signal);
                await this.#_jobAfterExitCallback(jobId, exitObj, runtimeStdoe);
                return exitObj.exitCode == 0 ? resolve(0) : (stopOnError ? reject(exitObj.exitCode) : resolve(exitObj.exitCode));
              })().catch(err => reject(err));
            });
            this.runningJobs.get(jobId).jobEmitter.on("error", (exitCode, signal) => {
              (async () => {
                await this.#_jobBeforeExitCallback(jobId, fileHandler);
                const exitObj = await this.jobExitCallback(jobObj, exitCode, signal);
                await this.#_jobAfterExitCallback(jobId, exitObj, runtimeStdoe);
                if (this.options.retryMode === 'eager') {
                  if (!jobObj.retry) { jobObj.retry = 0; }
                  jobObj.retry ++;
                  if (jobObj.retry <= this.options.retry) {
                    this.queue.push(() => this.#_runJob(jobId));
                  }
                }
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
   * @private
   * @function BdpTaskAdapter~#_runTasks
   * @description The core function to initiate the queue to process tasks.
   */
  async #_runTasks() {
    const jobs = this.#jobStore;
    const options = this.options;
    /**
     * The unfinished tasks are already executed by #_resumeJobs (stdoeMode === 'watch'). These tasks are already in this.runningJobs
     * Only executed tasks that are not yet finished (exitCode == 0) and not in the runningJobs.
     */
    const jobIds = Object.keys(jobs).filter(jobId => jobs[jobId].exitCode != 0 && !this.runningJobs.has(jobId) && this.#jobStore[jobId].option);
    if (jobIds.length > 4 && options.concurrency > 4) {
      process.stderr.write("Force to the batch mode due to more than 3 concurrent tasks.\n");
      this.options.batch = true;
    }
    this.queue.concurrency = options.concurrency || 1;
    this.queue.autostart = false;
    for (const jobId of jobIds) {
      this.queue.push(() => this.#_runJob(jobId));
    }
    this.#_monitorJobs().catch(e => console.log(e));
    return new Promise((resolve, reject) => {
      this.queue.start(err => {
        this.#_stopMonitor();
        isStopping = true;
        if (err) {
          process.stderr.write(`[${new Date().toString()}] Task queue ends with ${err}.` + "\n");
          if (options.stopOnError || err === "SIGTERM" || err === "SIGINT") {
            (async () => {
              process.stderr.write(`[${new Date().toString()}] Begining task cleanups...` + "\n");
              if (err !== 'SIGINT' || this.options.stdoeMode !== 'watch') {
                try {
                  await this.stopAllJobs();
                  if (options.debug) {
                    process.stderr.write(`[${new Date().toString()}] stopAllJobs() finished.` + "\n");
                  }
                } catch(err) {
                  console.log(`Failed to stopAllJobs().`);
                  console.log(err);
                  reject(err);
                }
              }
              if (this.options.stdoeMode === 'watch') {
                process.stderr.write(`[${new Date().toString()}] Fetch job status...` + "\n");
                await this.detectJobStatus();
                if (err !== "SIGINT") {
                  const status = await this.#_waitingJobStatus(125000, true);
                  if (status === 'timeout') {
                    process.stderr.write(`[${new Date().toString()}] Fetching timeout. ` + "\n");
                  } else {
                    process.stderr.write(`[${new Date().toString()}] Fetching jobs fnished.` + "\n");
                  }
                }
              } else {
                while (this.runningJobs.size > 0) { await sleep(500); }
              }
              /**
               * TODO: should wait until all jobs to be fully stopped!! Wait all processedAfterCallback to be finished?
               */
              process.stderr.write(`[${new Date().toString()}] Writing job logs...` + "\n");
              await this.#_writeJobLogs();
            })().catch((e) => console.log(e)).finally(() => {
              const nonNormalExitNumber = Object.keys(jobs).filter(jobId => jobs[jobId].exitCode != 0 && this.#jobStore[jobId].option).length;
              if (nonNormalExitNumber > 0) {
                process.stderr.write(`[${new Date().toString()}] Task queue ends with ${err}, and some tasks have exit code !== 0.` + "\n");
                process.stderr.write(`[${new Date().toString()}] Finished cleaning tasks ...` + "\n");
                reject(err);
              } else {
                process.stderr.write(`[${new Date().toString()}] Task queue ends with ${err}, and all tasks have exit code == 0.` + "\n");
                process.stderr.write(`[${new Date().toString()}] Finished cleaning tasks ...` + "\n");
                resolve(err);
              }
            });
          } else {
            reject(err);
          }
        } else {
          const nonNormalExitNumber = Object.keys(jobs).filter(jobId => jobs[jobId].exitCode != 0 && this.#jobStore[jobId].option).length;
          if (nonNormalExitNumber > 0) {
            process.stderr.write(`[${new Date().toString()}] Task queue ends without error, but some tasks have exit code !== 0.` + "\n");
            (async () => {
              try {
                await this.stopAllJobs();
                if (options.debug) {
                  process.stderr.write(`[${new Date().toString()}] stopAllJobs() finished.` + "\n");
                }
              } catch(err) {
                console.log(`Failed to stopAllJobs().`);
                console.log(err);
                reject(err);
              }
              process.stderr.write(`[${new Date().toString()}] Writing task logs...` + "\n");
              await this.#_writeJobLogs();
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
   * @private
   * @function BdpTaskAdapter~#_waitingJobStatus
   * @param {*} timeout
   * @description This internal function waits until all job object are resolved (having their own exit codes).
   */
  async #_waitingJobStatus(timeout, makeEnding) {
    let isTimeout = false, isAllResolved = false;
    const theTimeout = timeout || 120000;
    setTimeout(() => isTimeout = true, theTimeout);
    while(!isTimeout && !isAllResolved) {
      const jobIds = Object.keys(this.#jobStore);
      isAllResolved = true;
      await this.detectJobStatus();
      for (let i = 0; i < jobIds.length; i ++) {
        const jobObj = this.#jobStore[jobIds[i]];
        if (jobObj.exitCode === null || jobObj.exitCode === undefined) {
          isAllResolved = false;
        }
        break;
      }
      await sleep(1000);
    }
    if (isTimeout && makeEnding) {
      const jobIds = Object.keys(this.#jobStore);
      for (let i = 0; i < jobIds.length; i ++) {
        const jobObj = this.#jobStore[jobIds[i]];
        if (jobObj.exitCode === null || jobObj.exitCode === undefined) {
          jobObj.end = new Date().valueOf();
          jobObj.exitCode = 4;
        }
      }
    }
    return isTimeout ? 'timeout' : 'resolved';
  }

  /**
   * @function BdpTaskAdapter#initialize
   * @returns {TaskAdapterInstance} An object that can be directly used by the developers. Used to run batch jobs.
   * @description This function initializes the whole adapter process and must be called to get the Adapter object instance.
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
          this.#_stopMonitor();
          process.stderr.write(`[${new Date().toString()}] bdp-task-adapter recieving the workaround stop signal.`+ "\n");
          process.stderr.write(`[${new Date().toString()}] Removing the job queue ...` + "\n");
          this.queue.end("SIGTERM");
        } else if (line === "bdp stop task with SIGINT") {
          this.queue.stop();
          isStopping = true;
          this.#_stopMonitor();
          process.stderr.write(`[${new Date().toString()}] bdp-task-adapter recieving the workaround stop signal with SIGINT.`+ "\n");
          process.stderr.write(`[${new Date().toString()}] Removing the job queue ...` + "\n");
          this.queue.end('SIGINT');
        }
      });
    });
    process.removeAllListeners("SIGINT");
    process.on("SIGINT", () => {
      // SIGINT no longer stop the running tasks.;
      this.queue.stop();
      isStopping = true;
      this.#_stopMonitor();
      this.exitSignalCount ++;
      process.stderr.write(`[${new Date().toString()}] bdp-task-adapter recieving the SIGINT signal.` +  "\n");
      if (this.exitSignalCount >= 5) {
        process.stderr.write('More than 5 SIGINT signals were recieved. Forcing this process to stop.\n');
        process.stderr.write('You may need to do some clean ups manually for the stopped jobs.\n');
        process.exit(1);
      }
      this.queue.end('SIGINT');
    });
    process.removeAllListeners("SIGTERM");
    process.on("SIGTERM", () => {
      this.queue.stop();
      isStopping = true;
      this.#_stopMonitor();
      this.exitSignalCount ++;
      process.stderr.write(`[${new Date().toString()}] bdp-task-adapter recieving the SIGTERM signal.` + "\n");
      if (this.exitSignalCount >= 5) {
        process.stderr.write('More than 5 SIGTERM signals were recieved. Forcing this process to stop.');
        process.stderr.write('You may need to do some clean ups for the stopped jobs.');
        process.exit(1);
      }
      process.stderr.write(`[${new Date().toString()}] Removing the job queue ...` + "\n");
      this.queue.end("SIGTERM");
    });
    process.on("message", msgObj => {
      if (!msgObj) { return; }
      if (msgObj.source !== 'bdp-server') { return; }
      (async () => {
        switch (msgObj.action) {
          case 'rejectProxy':
            break;
        }
      })().catch(console.log);
    });
    return {
      getOptions: () => {
        return this.options;
      },
      execute: async tasks => {
        try {
          isStopping = false;
          await this.#_loadProgress();
          if (Array.isArray(tasks)) {await this.#_addJobs(tasks);}
          if (Object.keys(this.#jobStore).length <= 0) {throw "No task to proceed.\n";}
          await this.#_writeJobLogs();
          await this.#_resumeJobs();
          await this.beforeStart();
          await this.#_writeJobLogs();
          let success = false;
          let retry = 0;
          while (!success && retry <= this.options.retry && this.options.retryMode === 'normal') {
            try {
              if (retry > 0) {
                process.stderr.write(`[${new Date().toString()}] Retry failed jobs (${retry} time${retry > 1 ? 's' : ''}).` + "\n");
                // await this.beforeStart();
              }
              await this.#_runTasks();
              success = true;
            }catch(e) {
              retry ++;
              isStopping = false;
              if (e === 'SIGTERM' || e === 'SIGINT') { throw e; }
              process.stderr.write(`[${new Date().toString()}] ${e}` + "\n");
              if (retry > this.options.retry) {
                process.stderr.write(`[${new Date().toString()}] Task failed after ${retry - 1} re-run(s).` + "\n");
                await this.#_writeJobLogs();
                throw e;
              }
            }
          }
          await this.#_writeJobLogs();
          await this.beforeExit(); // This is used to clean the resources after finishing all jobs
          process.stderr.write(`[${new Date().toString()}] Task execution finished.` + "\n");
        } catch(e) {
          if (e === 'SIGTERM') {
            try {
              await this.beforeExit();
            } catch(err) {
              console.log(`Failed to run beforeExit().`);
              console.log(err);
              console.log(`Execute finish: ${e}`);
            }
          }
          process.stderr.write(`[${new Date().toString()}] Task execution finished with ${e}.` + "\n");
          throw e;
        }
      }
    };
  }
}

module.exports = BdpTaskAdapter;
