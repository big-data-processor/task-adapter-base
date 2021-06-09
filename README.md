# A base class of Task Adapter for BD-Processor.
---
This is a base class for developers to extend and implment for different computing resources.

([Click to view the jsDoc on Github Pages](https://big-data-processor.github.io/task-adapter-base/)).

## Task Adapter
The Task Adapter is a core component in the BD-Processor.
Task Adpater processes are spawned when a Task is executed on BD-Processor.
As shown in the following figure, adapter processes mange to deploy and monitor jobs on local or remote computing resources.

<div align='center'>
    <img src="https://raw.githubusercontent.com/big-data-processor/assets/master/images/System%20Architecture.png" width="600"><br>
    <b>System Architecture of BD-Processor</b>
</div>


## What does a Task Adapter do?
An adapter process does the following things.

1. Organizing task arguments and form job commands
   
    Different runtime environments have different command patterns. For example, if you use task-adapter-docker which execute the job with the `docker run ...` pattern.
    If you use a pbs scheduling system, the job submission pattern may be looked like `qsub ...`.

2. Scheduling jobs 

    The adapter has a queue to schedule multiple jobs. There is a concurrency option to specify the concurrent job numbers and the adapter will try to maintiain the concurrency.

3. Monitoring job states

    There are two modes for an adapter to monitor job status and it depends on the runtime environments. It specifies the ways to monitor job status and recording stdout/stderr.
    For the `pipe` stdoeMode, the job process is spawned locally and the message is directly pipe to the adpater process. The job status is directly known by the adapter process.
    For the `watch` stdoeMode, the job is executed remotely and the job status needs to be fetched. The adapter process retrieves the job status periodically. The interval is set by the `updateInterval` option.

4. Recording job messages such as the stdout and stderr

    In order to allow viewing job messages in real-time, the adapter reports the latest job messages to BD-Processor.An adapter process captures job messages (stdout/stderr) based on the above mentioned modes. All jobs are organized well by the adapter.
    Developers can customize different ways to captures the job messages or logs for different runtime environments.

5. Resuming job executions

    An adapter process logs the progres of each job. If one of the jobs failed, the adapter process fails in the end.
    If developers fixes their scripts or fixes task settings, it is very easy to rerun the adapter process on BD-Processor.
    The adapter process will try to resume unfinished jobs.


Overall, the workflow procedure of an adapter is shown in the following figure.
<div align='center'>
    <img src="https://raw.githubusercontent.com/big-data-processor/assets/master/images/Adapter-Flowchart.png" width="600"><br>
    <b>The procedure of an Adapter process</b>
</div>

# Instructions to extend this base class and implement for various computing resources
(comming soon!)

# Examples
(comming soon!)

# License

Licensed under the MIT license (see the <a href="https://github.com/big-data-processor/task-adapter-base/blob/master/LICENSE" target=_blank>license</a>)