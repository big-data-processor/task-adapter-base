const fse = require("@big-data-processor/utilities").fse;
const path = require('path');
module.exports = async function(runtimeStdoe, runningTaskObj, isBatch) {
  try {
    await fse.ensureDir(path.dirname(runtimeStdoe.stdout));
  } catch(err) {
    console.log(err);
  }
  try {
    const stderrFS = fse.createWriteStream(runtimeStdoe.stderr);
    const stdoutFS = fse.createWriteStream(runtimeStdoe.stdout);
    ['stdout', 'stderr'].forEach((stdoe) => {
      runningTaskObj[stdoe + "Stream"].on("readable", () => {
        const data = runningTaskObj[stdoe + "Stream"].read();
        if(!data) { return;}
        if (!isBatch) { process[stdoe].write(data); }
        try {
          stdoe === 'stdout' ? stdoutFS.write(data) : stderrFS.write(data);
        } catch(err) {}
      });
    });
    return {stdoutFS, stderrFS};
  } catch(err) {
    console.log(err);
  }
};
