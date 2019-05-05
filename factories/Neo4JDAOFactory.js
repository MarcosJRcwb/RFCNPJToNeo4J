const neo4j = require('neo4j-driver').v1;

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
  const driver = neo4j.driver(uri, neo4j.auth.basic(login, senha), {
    encrypted: 'ENCRYPTION_OFF', // workaround por conta da excecao 'Client network socket disconnected before secure TLS connection was established'
  });

  return Object.freeze({
    salvaPJ,
    encerrarConexao,
  });

  /**
   * Persiste  na base de dados os dados de uma pessoa juridica
   *
   * @param {object} pj Dados da PJ a ser persistido
   */
  async function salvaPJ({ pj }) {
    const session = driver.session();
    try {
      // Busca uma Pessoa Juridica pelo CNPJ informado, se nao existir, cria
      const pjPersistida = await session.run(
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
      );
      const singleRecord = pjPersistida.records[0];
      const node = singleRecord.get(0);
      console.log(node.properties);
      pj.endereco.cnpj = pj.cnpj;
      // Busca um endereco informado, se nao existir, cria
      const enderecoPersistido = await session.run(
        `MERGE (e:Endereco {codigoMunicipio: $codigoMunicipio, bairro: $bairro, 
          complemento: $complemento, numero: $numero, logradouro: $logradouro, tipoLogradouro: $tipoLogradouro})
       ON CREATE SET
        e.tipoLogradouro = $tipoLogradouro, 
        e.logradouro = $logradouro, 
        e.numero = $numero, 
        e.complemento = $complemento, 
        e.bairro = $bairro,
        e.cep = $cep,
        e.uf = $uf,
        e.codigoMunicipio = $codigoMunicipio,
        e.municipio = $municipio 

        MERGE (p:PessoaJuridica {cnpj: $cnpj})
        CREATE (p)-[r:SEDIADA_EM]->(e)  
        RETURN e`,
        pj.endereco,
      );
      const enderecoRecord = enderecoPersistido.records[0];
      const nodeEndereco = enderecoRecord.get(0);
      console.log(nodeEndereco.properties);
    } catch (e) {
      console.error(e.message);
      global.log.error(e);
    } finally {
      session.close();
    }
  }

  /**
   * Encerra conexao com base dados Neo4J
   */
  function encerrarConexao() {
    // on application exit:
    driver.close();
  }
};
