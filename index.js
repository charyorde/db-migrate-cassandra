var util = require('util');
var cassandra = require('cassandra-driver');
var Base = require('db-migrate-base');
var type;
var log;
var Promise = require('bluebird');

var CassandraDriver = Base.extend({

  init: function(connection, schema, intern) {
    this._escapeString = '\''; 
    this._super(intern);
    this.internals = intern;
    this.connection = connection;
    this.schema = schema;
    this.connection.connect();
  },

  /**
   * Alias for createKeyspace
   */ 
  createDatabase: function(dbName, options, callback) {
    this.createKeyspace(dbName, options, callback)
  },

  createKeyspace: function(name, options, callback) {
 
    var ifNotExistsSql = '';

    if(typeof(options) === 'function')
      callback = options;
    else {
      ifNotExistsSql = (options.ifNotExists) ? "IF NOT EXISTS" : '';
    }

    this.runSql(util.format('CREATE KEYSPACE %s %s', ifNotExistsSql, this.escapeDDL(name)), callback)
  },

  dropDatabase: function(dbName, options, callback) {
    
    var ifExists = '';

    if(typeof(options) === 'function')
      callback = options;
    else
    {
      ifExists = (options.ifExists === true) ? 'IF EXISTS' : '';
    }

    this.runSql(util.format('DROP KEYSPACE %s %s', ifExists, this.escapeDDL(dbName)), callback);
  },

  createMigrationsTable = function(callback) {

    var options = {
      columns: {
        'id': { type: type.INTEGER, primaryKey: true },
        'name': { type: type.STRING, primaryKey: true}, // clustering key
        'run_on': { type: type.TIMESTAMP, primaryKey: true} // clustering key
      },
      ifNotExists: true
    }

    return this.createTable(this.internals.migrationTable, options).nodeify(callback)
  },

  /**
   * Overridden from Base
   */ 
  createTable: function(tableName, options, callback) {
 
    log.verbose('creating table:', tableName);
    var columnSpecs = options;
    var tableOptions = {};

    if (options.columns !== undefined) {
      columnSpecs = options.columns;
      delete options.columns;
      tableOptions = options;
    }

    var ifNotExistsSql = "";
    if(tableOptions.ifNotExists) {
      ifNotExistsSql = "IF NOT EXISTS";
    }

    for (var columnName in columnSpecs) {
      var columnSpec = this.normalizeColumnSpec(columnSpecs[columnName]);
      columnSpecs[columnName] = columnSpec;
      if (columnSpec.primaryKey) {
        primaryKeyColumns.push(columnName);
      }
    }

    // Add support for compound primary keys
    // @todo Add support for Cassandra clustering keys
    var pkSql = '';
    if (primaryKeyColumns.length > 1) {
      pkSql = util.format(', PRIMARY KEY (%s)',
        this.quoteDDLArr(primaryKeyColumns).join(', '));
      
    } else {
      columnDefOptions.emitPrimaryKey = true;
    }

    var columnDefs = [];
    var foreignKeys = []; 

    for (var columnName in columnSpecs) {
      var columnSpec = columnSpecs[columnName];
      var constraint = this.createColumnDef(columnName, columnSpec, columnDefOptions, tableName);

      columnDefs.push(constraint.constraints);
      if (constraint.foreignKey)
        foreignKeys.push(constraint.foreignKey);
    }

    var sql = util.format('CREATE TABLE %s %s (%s%s)', ifNotExistsSql, this.escapeDDL(tableName), columnDefs.join(', '), pkSql);

    return this.runSql(sql).bind(this).nodeify(callback)
  },

  createColumnConstraint: function(spec, options, tableName, columnName){
    var constraint = [],
        cb;

    if (spec.primaryKey && options.emitPrimaryKey) {
      constraint.push('PRIMARY KEY');
    }

    if (constraint) {
      ret = { foreignKey: cb, constraints: constraint.join(' ') };
    }
    else {
      ret = { foreignKey: cb }
    }

    return ret
  },

  addColumn: function(tableName, columnName, columnSpec, callback) {

    var def = this.createColumnDef(columnName,
      this.normalizeColumnSpec(columnSpec), {}, tableName);
    var sql = util.format('ALTER TABLE %s ADD %s',
      this.escapeDDL(tableName), def.constraints);

    return this.runSql(sql)
    .then(function()
    {
      return Promise.resolve();
    }).nodeify(callback);
  },

  removeColumn: function(tableName, columnName, callback) {
    var sql = util.format('ALTER TABLE "%s" DROP "%s"', tableName, columnName);
    return this.runSql(sql).nodeify(callback);
  },

  renameColumn: function(tableName, oldColumnName, newColumnName, callback) {
    var sql = util.format('ALTER TABLE "%s" RENAME "%s" TO "%s"', tableName, oldColumnName, newColumnName);
    return this.runSql(sql).nodeify(callback);
  },

  insert: function() {

    var index = 1;

    if( arguments.length > 3 ) {

      index = 2;
    }

    arguments[index] = arguments[index].map(function(value) {
      return 'string' === typeof value ? value : JSON.stringify(value);
    });

    return this._super.apply(this, arguments);
  },

  runSql: function() {
    var callback,
        minLength = 1;

    if(typeof(arguments[arguments.length - 1]) === 'function')
    {
      minLength = 2;
      callback = arguments[arguments.length - 1];
    }

    params = arguments;

    log.sql.apply(null, params);
    if(this.internals.dryRun) {
      return Promise.resolve().nodeify(callback);
    }

    return new Promise(function(resolve, reject) {
      var prCB = function(err, data) {
        return (err ? reject(err) : resolve(data));
      };

      if( minLength === 2 )
        params[params.length - 1] = prCB;
      else
        params[params.length++] = prCB;

      this.connection.query.apply(this.connection, params);
    }.bind(this)).nodeify(callback);
  },

  all: function() {
    params = arguments;

    log.sql.apply(null, params);

    return new Promise(function(resolve, reject) {
      var prCB = function(err, data) {
        return (err ? reject(err) : resolve(data));
      };

      this.connection.query.apply(this.connection, [params[0], function(err, result){
        prCB(err, (result) ? result.rows : result);
      }]);

    }.bind(this)).nodeify(params[1]);
  },

  close: function(callback) {
    this.connection.end();
    if( typeof(callback) === 'function' )
      return Promise.resolve().nodeify(callback);
    else
      return Promise.resolve();
  }
})

Promise.promisifyAll(CassandraDriver);

exports.connect = function(config, intern, callback) {

  internals = intern

  log = intern.mod.log
  type = intern.mod.type

  var db = new cassandra.Client(config)
  callback(null, new CassandraDriver(db, config.schema, intern));
};
