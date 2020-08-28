const fse = require("@big-data-processor/utilities").fse;
module.exports = function(stdoe, filePath, start) {
  return new Promise(resolve => {
    const stdoeFS = fse.createReadStream(filePath, {encoding: "utf8", start: start});
    stdoeFS.on("close", () => resolve());
    stdoeFS.on("error", err => {
      console.log(err);
      stdoeFS.close();
    });
    stdoeFS.pipe(process[stdoe]);
  });
};
