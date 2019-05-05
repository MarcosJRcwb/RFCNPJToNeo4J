const fs = require('fs');
const path = require('path');
const os = require('os');
const colors = require('colors');
const readlineSync = require('readline-sync');
const loaders = require('./loaders');
const ReceitaFederalFactory = require('./factories/ReceitaFederalFactory');
const Neo4JDAOFactory = require('./factories/Neo4JDAOFactory');

loaders.init();

const neo4J = Neo4JDAOFactory({ uri: process.env.NEO4J_URI, login: process.env.NEO4J_USERNAME, senha: process.env.NEO4J_PASSWORD });

(async () => {
  // Interacao usuario
  let pathArquivo = readlineSync.question('Caminho do arquivo de CNPJs da Receita Federal do Brasil:');
  const filtroUF = readlineSync.question('Filtrar UF (deixe em branco para nao filtrar):');
  const filtroCidade = readlineSync.question('Filtrar Cidade (deixe em branco para nao filtrar):');
  const filtroBairro = readlineSync.question('Filtrar Bairro (deixe em branco para nao filtrar):');
  const filtroIncluirBaixadas = readlineSync.question('Incluir empresas ja baixadas (s/N):');
  if (pathArquivo == '' || pathArquivo == null) {
    pathArquivo = path.join(os.homedir(), '/Documentos/dadosAbertos/receitaFederal/F.K032001K.D90308');
  }
  const receitaRederal = ReceitaFederalFactory({
    pathArquivo,
  });

  receitaRederal.leia({
    filtroUF,
    filtroCidade,
    filtroBairro,
    filtroIncluirBaixadas: filtroIncluirBaixadas.toUpperCase() === 'S',
    funcaoCallbackRegistro: callbackRegistro,
  });
})();

function callbackRegistro(registro) {
  neo4J.salvaPJ({ pj: registro });
  // console.log(registro);
  // process.exit();
}
