const figlet = require('figlet');

const figletFontListAsync = function() {
  return new Promise((resolve, reject) => figlet.fonts((err, fonts) => err ? reject(err) : resolve(fonts)));
}
const figletAsync = function(textInput, fontName) {
  return new Promise((resolve, reject) => {
    figlet.text(textInput, {
      font: fontName
    }, (err, data) => err ? reject(err) : resolve(data))
  });
};
module.exports = function(adapterName, author, drawAsciiArt, fontName) {
  (async () => {
    let outputText = '', asciiArt = '';
    if (drawAsciiArt) {
      const fontList = await figletFontListAsync();
      if (!fontName || fontList.indexOf(fontName) < 0) {
        const randomIndex = Math.floor(Math.random() * fontList.length);
        fontName = fontList[randomIndex];
      }
      asciiArt = await figletAsync("Big Data\n    Processor", fontName);
    }
    if (drawAsciiArt) {
      outputText += '=====================================================================================================================\n';
      outputText += 'The ' + fontName + ' font\n\n'
      outputText += asciiArt + '\n';
    }
    outputText += '\nHello, you are executing job(s) through the ' + adapterName + ' adapter.\n';
    outputText += 'Adapter author: ' + (author || 'noname') + '\n';
    process.stdout.write(outputText);
    process.stderr.write(outputText);
    if (drawAsciiArt) {
      process.stdout.write('===============================================↓↓ stdout start here ↓↓===============================================\n');
      process.stderr.write('===============================================↓↓ stderr start here ↓↓===============================================\n');
    }
  })().catch(e => console.log(e));
}
