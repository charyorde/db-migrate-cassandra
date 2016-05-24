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
    this.internals.notransactions = true;
    this.connection = connection;
    this.schema = schema;
    this.connection.connect(function(err) {
      if(err) {
        throw err
      }
    });

    this.connection.on('connected', function() {
      log.verbose("Connected to Cassandra")
    })
  },

  endMigration: function(cb) {
    return Promise.resolve(null).nodeify(cb);
  },

  mapDataType: function(str) {
    switch(str) {
      case type.INTEGER:
        return 'int'
      case type.STRING:
        return 'varchar'
      case type.TIMESTAMP:
        return 'timestamp'
      case type.TEXT:
        return 'text'
      case 'map':
        return 'map'
      case 'set':
        return 'set'
      case 'list':
        return 'list'
      case 'uuid':
        return 'uuid'
    }
    return this._super(str);
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

  createMigrationsTable: function(callback) {

    var options = {
      columns: {
        'id': { type: type.TEXT, primaryKey: true },
        'run_on': { type: type.TIMESTAMP, clusteringKey: {order: 'DESC'} }, // clustering key
        'name': { type: type.STRING, clusteringKey: {order: 'DESC'} } // clustering key
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

    var primaryKeyColumns = [];
    var clusteringKeyColumns = []
      , clusteringKeyOrder = []
    var clusteringKey;
    var withClustering = false
    var columnDefOptions = {
      emitPrimaryKey: false
    }

    for (var columnName in columnSpecs) {
      var columnSpec = this.normalizeColumnSpec(columnSpecs[columnName]);
      columnSpecs[columnName] = columnSpec;
      if (columnSpec.primaryKey) {
        primaryKeyColumns.push(columnName);
      }
      if (columnSpec.clusteringKey) {
        clusteringKeyColumns.push(columnName)
      }
    }

    // Add support for compound primary keys
    var pkSql = '';
    if (primaryKeyColumns.length > 1 && !clusteringKeyColumns) {
      pkSql = util.format(', PRIMARY KEY (%s)',
        this.quoteDDLArr(primaryKeyColumns).join(', '));
  
     } else if ((primaryKeyColumns.length > 1) && clusteringKeyColumns) {
      // get the second item in pkSql and make it the clustering key
      withClustering = true
      pkSql = util.format(', PRIMARY KEY ((%s),%s)',
        this.quoteDDLArr(primaryKeyColumns).join(', '), this.quoteDDLArr(clusteringKeyColumns).join(', '))

    } else if (primaryKeyColumns.length == 1 && clusteringKeyColumns) {
      withClustering = true
      pkSql = util.format(', PRIMARY KEY ((%s),%s)',
        this.quoteDDLArr(primaryKeyColumns).join(', '), this.quoteDDLArr(clusteringKeyColumns).join(', '))
        
    }
    else {
      columnDefOptions.emitPrimaryKey = true;
    }

    var columnDefs = [];
    var foreignKeys = []; 
    var cdefs

    for (var columnName in columnSpecs) {
      var columnSpec = columnSpecs[columnName];
      var constraint = this.createColumnDef(columnName, columnSpec, columnDefOptions, tableName);
      
      columnDefs.push(constraint.constraints);
    }

    var sql = util.format('CREATE TABLE %s %s (%s%s)', ifNotExistsSql, this.escapeDDL(tableName), columnDefs.join(', '), pkSql);
    if (withClustering) {
      clusteringKeyColumns.forEach(function(name) {
        clusteringKeyOrder.push(name + " DESC")
      })
      sql = sql + " WITH CLUSTERING ORDER BY " + util.format('(%s)', clusteringKeyOrder.join(', '))
    }

    return this.runSql(sql).bind(this).nodeify(callback)
  },

  createColumnDef: function(name, spec, options) {
    name = this._escapeDDL + name + this._escapeDDL;
    var type       = this.mapDataType(spec.type);
    var len        = spec.length ? util.format('(%s)', spec.length) : '';
    var constraint = this.createColumnConstraint(spec, options);

    return {
      constraints: [name, type, constraint].join(' ')
    }
  },

  createColumnConstraint: function(spec, options, tableName, columnName){
    var constraint = [],
        cb;

    if (spec.primaryKey && options.emitPrimaryKey) {
      constraint.push('PRIMARY KEY');
    }

    return (constraint) ? constraint.join(' ') : ''
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

  addMigrationRecord: function (name, callback) {
    var Uuid = cassandra.types.Uuid;
    var id = Uuid.random().toString();
    this.runSql('INSERT INTO ' + this.escapeDDL(this.internals.migrationTable) +
      ' (' + this.escapeDDL('id') + ', ' + this.escapeDDL('name') + ', ' + this.escapeDDL('run_on') +
      ') VALUES (?, ?, ?)', [id, name, new Date()], { prepare: true}, callback);
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

      this.connection.execute.apply(this.connection, params);
    }.bind(this)).nodeify(callback);
  },

  allLoadedMigrations: function(callback) {
    // Cassandra would order by default with the WITH CLUSTERING clause
    var sql = 'SELECT * FROM ' + this._escapeDDL + this.internals.migrationTable + this._escapeDDL;
    return this.all(sql, callback);
  },

  all: function() {
    params = arguments;

    log.sql.apply(null, params);

    return new Promise(function(resolve, reject) {
      var prCB = function(err, data) {
        return (err ? reject(err) : resolve(data));
      };

      this.connection.execute.apply(this.connection, [params[0], function(err, result){
        prCB(err, (result) ? result.rows : result);
      }]);

    }.bind(this)).nodeify(params[1]);
  },

  close: function(callback) {
    return new Promise( function(resolve, reject) {
    
      this.connection.shutdown(function(err) {
      
        if (err) {
          reject(err)
        }
        log.verbose("Cassandra connection closed")
        resolve()
      })
    }.bind(this) )
    .nodeify(callback)
  }
})

Promise.promisifyAll(CassandraDriver);

exports.connect = function(config, intern, callback) {

  internals = intern

  log = intern.mod.log
  type = intern.mod.type

  // Make sure the database is defined
  if(config.keyspace === undefined) {
    throw new Error('keyspace must be defined in database.json');
  }

  config.host = util.isArray(config.hosts) ? config.hosts : [config.host];
  
  var db = new cassandra.Client({
    contactPoints: config.host,
    keyspace: config.keyspace,
    authProvider: new cassandra.auth.PlainTextAuthProvider(config.user, config.password)
  })
  callback(null, new CassandraDriver(db, config.keyspace, intern));
};
