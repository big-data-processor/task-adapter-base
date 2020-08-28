const nunjucks = require("nunjucks")
  , defaultFilters = require("@big-data-processor/default-filters");

module.exports = (templateString, templateData) => {
  const nunjuckEnv = new nunjucks.Environment([], {autoescape: false, trimBlocks: true, lstripBlocks: true});
  defaultFilters(nunjuckEnv);
  return new Promise((resolve, reject) => {
    try {
      const template = new nunjucks.Template(templateString, nunjuckEnv, true);
      const renderedString = template.render(templateData);
      resolve(renderedString);
    }catch(e) {
      reject(e);
    }
  });
};
