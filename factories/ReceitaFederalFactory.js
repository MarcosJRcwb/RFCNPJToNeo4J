const colors = require('colors');
const fs = require('fs');
const path = require('path');
const es = require('event-stream');

/**
 * Retornará uma instância imutável de um objeto com funcionalidades para
 * leitura do arquivo de CNPJs da Receita Federal
 *
 * @param {string} pathArquivo Path para o arquivo a ser lido
 * @returns {object} Objeto com utilidades para leitura de dados do arquivo da Receita Federal
 */
module.exports = function ReceitaFederalFactory({ pathArquivo }) {
  let nomeArquivo = path.basename(pathArquivo);
  let dataGravacao = null;

  const wait = ms => new Promise((r, j) => setTimeout(r, ms));

  return Object.freeze({
    leia,
    nomeArquivo,
    dataGravacao,
  });

  /**
   * Percorre o arquivo linha a linha enquanto repassada cada linha para a funcao {funcaoLeituraLinha}
   * @param {string} filtroUF Se setado, retorna apenas registros com a UF igual ao passado
   * @param {string} filtroCidade Se setado, retorna apenas registros com a nome da cidade igual ao passado
   * @param {string} filtroBairro Se setado, retorna apenas registros com o nome do bairro igual ao passado
   * @param {boolean} filtroIncluirBaixadas Se FALSE, exclui do retorno as empresas que ja deram baixa
   * @param {function} funcaoCallbackRegistro Funcao que recebera o registro contido em cada uma das linhas do arquivo
   * @param {function} funcaoCallbackFimArquivo Funcao que sera chamada quando se chegar ao fim do arquivo
   */
  function leia({
    filtroUF, filtroCidade, filtroBairro, filtroIncluirBaixadas, funcaoCallbackRegistro, funcaoCallbackFimArquivo,
  }) {
    let cabecalhoLido = false;
    fs.createReadStream(pathArquivo)
      .pipe(es.split())
      .pipe(
        es.mapSync((linha) => {
          if (!cabecalhoLido && linha.substring(0, 1) === '0') {
            nomeArquivo = linha.substr(17, 11);
            dataGravacao = linha.substr(28, 8);
            cabecalhoLido = true;
          } else if (linha.substring(0, 1) === '1') {
            if (
              (filtroUF == null || filtroUF.trim() === '' || linha.substr(682, 2) === filtroUF.toUpperCase())
              && (filtroCidade == null || filtroCidade.trim() === '' || linha.substr(688, 50) === filtroCidade.toUpperCase())
              && (filtroBairro == null || filtroBairro.trim() === '' || linha.substr(624, 50) === filtroBairro.toUpperCase())
              && (filtroIncluirBaixadas || linha.substr(223, 2) !== '08')
            ) {
              const cnpj = {};
              add(cnpj, 'cnpj', linha.substr(3, 14), true);
              add(cnpj, 'razaoSocial', linha.substr(18, 150), true);
              add(cnpj, 'nomeFantasia', linha.substr(168, 55), true);
              add(cnpj, 'codigoSituacaoCadastral', linha.substr(223, 2), true);
              add(cnpj, 'situacaoCadastral', obtemSituacaoCadastral(linha.substr(223, 2)), true);
              add(cnpj, 'dataSituacaoCadastral', linha.substr(225, 8), true);
              add(cnpj, 'codigoPais', linha.substr(290, 3));
              add(cnpj, 'nomePais', linha.substr(293, 70));
              add(cnpj, 'nomeCidadeExterior', linha.substr(235, 55));
              add(cnpj, 'codigoNaturezaJuridica', linha.substr(363, 4), true);
              add(cnpj, 'dataInicioAtividade', linha.substr(367, 8), true);
              add(cnpj, 'cnaePrincipal', linha.substr(375, 7), true);
              const endereco = {};
              add(endereco, 'tipoLogradouro', linha.substr(382, 20), true);
              add(endereco, 'logradouro', linha.substr(402, 60), true);
              add(endereco, 'numero', linha.substr(462, 6), true);
              add(endereco, 'complemento', linha.substr(468, 156), true);
              add(endereco, 'bairro', linha.substr(624, 50), true);
              add(endereco, 'cep', linha.substr(674, 8), true);
              add(endereco, 'uf', linha.substr(682, 2), true);
              add(endereco, 'codigoMunicipio', linha.substr(684, 4), true);
              add(endereco, 'municipio', linha.substr(688, 50), true);
              add(cnpj, 'endereco', endereco);
              cnpj.telefones = [];
              const telefone1 = {};
              add(telefone1, 'ddd', linha.substr(738, 4));
              add(telefone1, 'telefone', linha.substr(742, 8));
              if (Object.keys(telefone1).length > 0) {
                cnpj.telefones.push(telefone1);
              }
              const telefone2 = {};
              add(telefone2, 'ddd', linha.substr(750, 4));
              add(telefone2, 'telefone', linha.substr(755, 8));
              if (Object.keys(telefone2).length > 0) {
                cnpj.telefones.push(telefone2);
              }
              const fax = {};
              add(fax, 'ddd', linha.substr(762, 4));
              add(fax, 'fax', linha.substr(766, 8));
              if (Object.keys(fax).length > 0) {
                cnpj.telefones.push(fax);
              }
              add(cnpj, 'email', linha.substr(774, 115));
              add(cnpj, 'codigoPorteEmpresa', linha.substr(905, 2));
              add(cnpj, 'porteEmpresa', obtemPorteEmpresa(linha.substr(905, 2)));
              add(cnpj, 'optanteMEI', linha.substr(924, 1));
              add(cnpj, 'situacaoEspecial', linha.substr(925, 23));
              funcaoCallbackRegistro(cnpj);
              // Se tem mais de 300 linhas "de saldo" para gravar, espera pois o banco pode estar sobrecarregado
              // Espera-se 10 segundos para cada 1000 em espera
              console.log('Escritas em espera', colors.red(global.escritasEmEspera));
              while (global.escritasEmEspera > 300) {
                 setTimeout(()=>{
		 console.log(`Aguardando gravação de registros no banco de dados para continuar leitura do arquivo: ${global.escritasEmEspera}`);
		 },global.escritasEmEspera*20);
              }
            }
          } else if (linha.substring(0, 1) === '9') {
            funcaoCallbackFimArquivo();
          }
        }),
      );
  }
};

/**
 * Adiciona ao objeto {objetoTarget} uma propridade de nome {propriedade} caso
 * o {valor} seja diferente de null e, em caso de ser string, nao seja vazio
 *
 * Caso o parametro {adicionaMesmoNULL} seja TRUE, entao adiciona assim mesmo
 *
 * @param {object} objetoTarget
 * @param {string} propriedade
 * @param {*} valor
 * @param {boolean} adicionaMesmoNULLOuVazio Se TRUE, adiciona a propriedade mesmo que esteja NULL ou string vazia
 */
function add(objetoTarget, propriedade, valor, adicionaMesmoNULLOuVazio = false) {
  if (valor != null || adicionaMesmoNULLOuVazio) {
    if (typeof valor === 'string' && (valor.trim().length > 0 || adicionaMesmoNULLOuVazio)) {
      objetoTarget[propriedade] = valor.trim();
    } else if (typeof valor === 'object' && Object.keys(valor).length > 0) {
      objetoTarget[propriedade] = valor;
    } else if (typeof valor !== 'string' && typeof valor !== 'object') {
      objetoTarget[propriedade] = valor;
    }
  }
}

function obtemSituacaoCadastral(codigo) {
  switch (codigo) {
    case '01': {
      return 'NULA';
    }
    case '02': {
      return 'ATIVA';
    }
    case '03': {
      return 'SUSPENSA';
    }
    case '04': {
      return 'INAPTA';
    }
    case '08': {
      return 'BAIXADA';
    }
    default: {
      return `<CODIGO NAO IDENTIFICADO: ${codigo}>`;
    }
  }
}

function obtemPorteEmpresa(codigo) {
  switch (codigo) {
    case '00': {
      return 'NAO INFORMADO';
    }
    case '01': {
      return 'MICRO EMPRESA';
    }
    case '03': {
      return 'EMPRESA DE PEQUENO PORTE';
    }
    case '05': {
      return 'DEMAIS';
    }
    default: {
      return `<CODIGO NAO IDENTIFICADO: ${codigo}>`;
    }
  }
}
