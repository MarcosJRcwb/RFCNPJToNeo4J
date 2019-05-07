const neo4j = require('neo4j-driver').v1;
const colors = require('colors');

/**
 * Retornará uma instância imutável de um objeto com funcionalidades para
 * acesso e escrita de dados em uma base de grafos Neo4J
 *
 * @param {string} uri URI da base de dados Neo4J
 * @param {string} login Nome de usuario para se autenticar na base Neo4J
 * @param {string} senha Senha para se autenticar na base de dados Neo4J
 * @returns {object} Objeto com utilidades para interacao com base Neo4J
 */
module.exports = function Neo4JDAOFactory({ uri, login, senha }) {
  let linhasRecebidas = 0;
  let linhasInseridas = 0;
  const driver = neo4j.driver(uri, neo4j.auth.basic(login, senha), {
    encrypted: 'ENCRYPTION_OFF', // workaround por conta da excecao 'Client network socket disconnected before secure TLS connection was established'
  });
  const session = driver.session();

  return Object.freeze({
    salvaPJ,
    encerraConexao,
  });

  /**
   * Persiste  na base de dados os dados de uma pessoa juridica
   *
   * @param {object} pj Dados da PJ a ser persistido
   */
  async function salvaPJ({ pj }) {
    linhasRecebidas++;
    try {
      session.run('MATCH (p:PessoaJuridica {cnpj: $cnpj}) RETURN p.cnpj', pj).subscribe({
        onNext(result) {
          pj.endereco.cnpj = pj.cnpj;
          // Se o CNPJ nao esta na base ainda, entao insere
          //console.log(colors.yellow(pj.cnpj), JSON.stringify(result));
          if (result.length == 0) {
            criaPessoa({
              pj,
              callBack: (result) => {
                linhasInseridas++;
                console.log(`${linhasInseridas}/${linhasRecebidas} PJs importadas`);
                buscaOuCriaEndereco({
                  endereco: pj.endereco,
                  callBack: (result) => {
                    criaRelacaoEnderecoCNPJ({ endereco, callBack: (result) => {} });
                  },
                });
              },
            });
          } else {
            console.log(colors.green(`${pj.cnpj} ja presente na base de dados`));
            buscaOuCriaEndereco({
              endereco: pj.endereco,
              callBack: (result) => {
                buscaOuCriaRelacaoEnderecoCNPJ({ endereco: pj.endereco, callBack: (result) => {} });
              },
            });
          }
        },
      });
      // console.log(pj.razaoSocial, pj.endereco.logradouro, pj.endereco.municipio, pj.endereco.uf);
    } catch (e) {
      console.error(e.message);
      global.log.error(e);
      return false;
    }
    return true;
  }

  function criaPessoa({ pj, callBack }) {
    session
      .run(
        `CREATE (a:PessoaJuridica { cnpj : $cnpj, 
      razaoSocial : $razaoSocial, 
      nomeFantasia : $nomeFantasia, 
      codigoSituacaoCadastral : $codigoSituacaoCadastral, 
      situacaoCadastral : $situacaoCadastral,
      dataSituacaoCadastral : $dataSituacaoCadastral,
      codigoNaturezaJuridica : $codigoNaturezaJuridica,
      dataInicioAtividade : $dataInicioAtividade,
      cnaePrincipal : $cnaePrincipal }) RETURN a`,
        pj,
      )
      .subscribe({
        onNext: callBack,
      });

    // Busca uma Pessoa Juridica pelo CNPJ informado, se nao existir, cria
    /* await session.run(
      `MERGE (a:PessoaJuridica {cnpj: $cnpj})
    ON CREATE SET
      a.cnpj = $cnpj,
      a.razaoSocial = $razaoSocial,
      a.nomeFantasia = $nomeFantasia,
      a.codigoSituacaoCadastral = $codigoSituacaoCadastral,
      a.situacaoCadastral = $situacaoCadastral,
      a.dataSituacaoCadastral = $dataSituacaoCadastral,
      a.codigoNaturezaJuridica = $codigoNaturezaJuridica,
      a.dataInicioAtividade = $dataInicioAtividade,
      a.cnaePrincipal = $cnaePrincipal RETURN a`,
      pj,
    ); */
  }

  function buscaOuCriaEndereco({ endereco, callBack }) {
    buscaEndereco({
      endereco,
      callBack: (result) => {
        if (result == null || result.length == 0) {
          criaEndereco({ endereco, callBack });
        } else {
          console.log(
            colors.green(`Endereco ${endereco.tipoLogradouro} ${endereco.logradouro} ${endereco.municipio}-${endereco.uf} ja presente na base de dados`),
          );
          callBack(result);
        }
      },
    });
  }

  function buscaEndereco({ endereco, callBack }) {
    session
      .run(
        `MATCH (e:Endereco {codigoMunicipio: $codigoMunicipio, bairro: $bairro, 
complemento: $complemento, numero: $numero, logradouro: $logradouro, tipoLogradouro: $tipoLogradouro}) RETURN e`,
        endereco,
      )
      .subscribe({
        onNext: callBack,
      });
  }

  function criaEndereco({ endereco, callBack }) {
    session
      .run(
        `CREATE (e:Endereco { tipoLogradouro : $tipoLogradouro, 
        logradouro : $logradouro, 
        numero : $numero, 
        complemento : $complemento, 
        bairro : $bairro,
        cep : $cep,
        uf : $uf,
        codigoMunicipio : $codigoMunicipio,
        municipio : $municipio })
         RETURN e `,
        endereco,
      )
      .subscribe({
        onNext: callBack,
      });
  }

  function buscaOuCriaRelacaoEnderecoCNPJ({ endereco, callBack }) {
    buscaRelacaoEnderecoCNPJ({
      endereco,
      callBack: (result) => {
        if (result == null || result.length == 0) {
          criaRelacaoEnderecoCNPJ({ endereco, callBack });
        } else {
          console.log(
            colors.green(
              `CNPJ ${endereco.cnpj} SEDIADA_EM  ${endereco.tipoLogradouro} ${endereco.logradouro} ${endereco.municipio}-${
                endereco.uf
              } ja presente na base de dados`,
            ),
          );
          callBack(result);
        }
      },
    });
  }

  function buscaRelacaoEnderecoCNPJ({ endereco, callBack }) {
    session
      .run(
        ` MATCH (:PessoaJuridica {cnpj: $cnpj})-[r:SEDIADA_EM]->(Endereco {codigoMunicipio: $codigoMunicipio, bairro: $bairro, 
complemento: $complemento, numero: $numero, logradouro: $logradouro, tipoLogradouro: $tipoLogradouro}) RETURN r`,
        endereco,
      )
      .subscribe({
        onNext: callBack,
      });
  }

  function criaRelacaoEnderecoCNPJ({ endereco, callBack }) {
    session
      .run(
        `MATCH (p:PessoaJuridica {cnpj: $cnpj})
         MATCH (e:Endereco {codigoMunicipio: $codigoMunicipio, bairro: $bairro, 
complemento: $complemento, numero: $numero, logradouro: $logradouro, tipoLogradouro: $tipoLogradouro}) 
         CREATE (p)-[r:SEDIADA_EM]->(e)
         RETURN r })`,
        endereco,
      )
      .subscribe({
        onNext: callBack,
      });
  }

  /**
   * Encerra conexao com base dados Neo4J
   */
  function encerraConexao() {
    // on application exit:
    session.close();
    driver.close();
  }
};
