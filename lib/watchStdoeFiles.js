const chokidar = require("chokidar")
  , path = require("path")
  , fse = require("@big-data-processor/utilities").fse;
module.exports = async function(runtimeStdoe, isBatch) {
  // Only watch the latest part, not the whole file. 
  const watchFolder = path.dirname(runtimeStdoe.stdout);
  const stdoutBasename = path.basename(runtimeStdoe.stdout);
  const stderrBasename = path.basename(runtimeStdoe.stderr);
  const stdoutExists = await fse.pathExists(runtimeStdoe.stdout);
  const stderrExists = await fse.pathExists(runtimeStdoe.stderr);
  const readFileSizes = { stdout: 0, stderr: 0 };
  const stdoeWatcher = chokidar.watch(watchFolder, {
    usePolling: true,
    ignoreInitial: true,
    followSymlinks: false,
    cwd: watchFolder,
    depth: 0,
    alwaysStat: true
  });
  if (stdoutExists) {
    const stdoutStat = await fse.stat(runtimeStdoe.stdout);
    readFileSizes.stdout = stdoutStat.size;
  }
  if (stderrExists) {
    const stderrStat = await fse.stat(runtimeStdoe.stderr);
    readFileSizes.stderr = stderrStat.size;
  }
  stdoeWatcher.on("add", (thePath, stats) => {
    const stdoe = thePath === stdoutBasename ? "stdout" : thePath === stderrBasename ? "stderr" : false;
    if (!stdoe) {return;}
    if (!isBatch) {
      fse.createReadStream(path.resolve(watchFolder, thePath), {
        encoding: "utf8",
        start: readFileSizes[stdoe],
        end: stats.size
      }).pipe(process[stdoe]);
    }
    readFileSizes[stdoe] = stats.size;
  });
  stdoeWatcher.on("change", (thePath, stats) => {
    const stdoe = thePath === stdoutBasename ? "stdout" : thePath === stderrBasename ? "stderr" : false;
    if (!stdoe) {return;}
    if (!isBatch) {
      fse.createReadStream(path.resolve(watchFolder, thePath), {
        encoding: "utf8",
        start: readFileSizes[stdoe],
        end: stats.size
      }).pipe(process[stdoe]);
    }
    readFileSizes[stdoe] = stats.size;
  });
  return {stdoeWatcher: stdoeWatcher, readFileSizes: readFileSizes};
}