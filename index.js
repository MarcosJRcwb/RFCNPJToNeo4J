const fs = require('fs');
const path = require('path');
const os = require('os');
const colors = require('colors');
const readlineSync = require('readline-sync');
const loaders = require('./loaders');
const ReceitaFederalFactory = require('./factories/ReceitaFederalFactory');

loaders.init();

(async () => {
  let pathArquivo = readlineSync.question('Caminho do arquivo de CNPJs da Receita Federal do Brasil:');
  if (pathArquivo == '' || pathArquivo == null) {
    pathArquivo = path.join(os.homedir(), '/Documentos/dadosAbertos/receitaFederal/F.K032001K.D90308');
  }
  const receitaRederal = ReceitaFederalFactory({ pathArquivo });
  receitaRederal.leia({ funcaoCallbackRegistro: callbackRegistro });
  console.log(receitaRederal.dataGravacao);
})();

function callbackRegistro(registro) {
  console.log(registro);
  // process.exit();
}
