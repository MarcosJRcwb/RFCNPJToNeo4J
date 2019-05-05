const logging = require('./logging');

module.exports.init = () => {
  // variaveis de ambiente
  require('dotenv').config();
  // logging
  logging.setupLogging({ logName: 'dadosabertosBRL-RFCNPJToNeo4J' });
  process.on('uncaughtException', (err) => {
    global.log.error(err);
    console.error(`Caught exception: ${err}\n`);
    console.error(err.stack);
  });
};
