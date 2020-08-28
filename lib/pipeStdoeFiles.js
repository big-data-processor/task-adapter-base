const fse = require("@big-data-processor/utilities").fse;
module.exports = async function(runtimeStdoe, runningTaskObj, isBatch) {
  await fse.ensureFile(runtimeStdoe.stdout);
  await fse.ensureFile(runtimeStdoe.stderr);
  const stdoutFS = fse.createWriteStream(runtimeStdoe.stdout);
  const stderrFS = fse.createWriteStream(runtimeStdoe.stderr);
  ['stdout', 'stderr'].forEach((stdoe) => {
    runningTaskObj[stdoe + "Stream"].on("readable", () => {
      const data = runningTaskObj[stdoe + "Stream"].read();
      if(!data) { return;}
      if (!isBatch) { process[stdoe].write(data); }
      stdoe === 'stdout' ? stdoutFS.write(data) : stderrFS.write(data);
    });
  });
  return {stdoutFs: stdoutFS, stderrFS: stderrFS};
};
