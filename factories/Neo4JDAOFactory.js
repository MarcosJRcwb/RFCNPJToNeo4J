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
  const driver = neo4j.driver(uri, neo4j.auth.basic(login, senha));

  return Object.freeze({
    salvaPJ,
    encerrarConexao,
  });

  /**
   * Persiste  na base de dados os dados de uma pessoa juridica
   *
   * @param {object} pj Dados da PJ a ser persistido
   */
  function salvaPJ({ pj }) {
    const session = driver.session();
    const resultPromise = session.run(
      `CREATE (a:PJ {cnpj: $cnpj, 
        razaoSocial: $razaoSocial, 
        nomeFantasia: $nomeFantasia, 
        codigoSituacaoCadastral: $codigoSituacaoCadastral, 
        situacaoCadastral: $situacaoCadastral,
        dataSituacaoCadastral: $dataSituacaoCadastral,
        codigoNaturezaJuridica: $codigoNaturezaJuridica,
        dataInicioAtividade: $dataInicioAtividade,
        cnaePrincipal: $cnaePrincipal}) RETURN a`,
      {
        pj,
      },
    );
    resultPromise.then((result) => {
      session.close();

      const singleRecord = result.records[0];
      const node = singleRecord.get(0);

      console.log(node.properties.name);
    });
  }

  /**
   * Encerra conexao com base dados Neo4J
   */
  function encerrarConexao() {
    // on application exit:
    driver.close();
  }
};
