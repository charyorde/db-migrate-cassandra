var util = require('util');
var assert = require('assert')
var vows = require('vows')
var driver = require('../index');
var config = require('./db-config.json').cassandra;
var log = require('./log')
var internals = {};

internals.migrationTable = 'migrations';
internals.mod = internals.mod || {};
internals.mod.log = log;
log.silence(true);

var dataType = {
  ASCII: 'ascii',
  STRING: 'text',
  CHAR: 'varchar',
  TEXT: 'text',
  VARCHAR: 'varchar',
  INTEGER: 'int',
  BIG_INTEGER: 'bigint',
  COUNTER: 'counter',
  DOUBLE: 'double',
  UUID: 'uuid',
  DATE_TIME: 'timestamp',
  TIME: 'timestamp',
  BLOB: 'blob',
  TIMESTAMP: 'timestamp',
  BINARY: 'binary',
  BOOLEAN: 'boolean',
  DECIMAL: 'decimal',
  FLOAT: 'float',
  INET: 'inet',
  LIST: 'list',
  MAP: 'map',
  SET: 'set',
  TIMEUUID: 'timeuuid',
  VARINT: 'varint' // Arbitrary-precision integer
};

internals.mod.type = dataType;

driver.connect(config, internals, function(err, db) {
  vows.describe('cql').addBatch({
    'createTable': {
      topic: function() {
        db.createTable('event', {
          id: { type: dataType.INTEGER, primaryKey: true},
          str: { type: dataType.STRING },
          txt: { type: dataType.TEXT },
          chr: dataType.CHAR,
          intg: dataType.INTEGER,
          dti: dataType.DATE_TIME,
          bl: dataType.BOOLEAN
        }, this.callback.bind(this))
      }
    },

    'containing the event table': function(err, tables) {
      console.log("tables", tables)
      assert.isNotNull(tables);
    }
  }).export(module)
})

