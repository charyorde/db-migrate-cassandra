var vows = require('vows');
var assert = require('assert');
var fs = require('fs');
var path = require('path');
var cp = require('child_process');
var rmdir = require('rimraf');

var toArray = function(obj) {
  var arr = [];
  for (var prop in obj) {
      arr[prop] = obj[prop];
    }
  return arr;
};

function wipeMigrations(callback) {
  var dir = path.join(__dirname, 'migrations');
  rmdir(dir, callback);
}

function dbMigrate() {
  var args = toArray(arguments);
  var dbm = path.join(__dirname, 'bin', 'db-migrate');
  args.unshift(dbm);
  return cp.spawn('node', args, { cwd: __dirname });
}

vows.describe('create').addBatch({
  'with sql-file option set to true from config file' : {
    topic: function() {
      var configOption = path.join("--config=", __dirname, 'database.json');
      wipeMigrations(function(err) {
        assert.isNull(err);
        var dbm = path.join(__dirname, 'bin', 'db-migrate');
        args = [dbm, 'create', 'cql migration', configOption]
        dbmigrate = cp.spawn('node', args, { cwd: __dirname });
        dbmigrate.stderr.on('data', function (data) {
          console.log('stderr: ' + data);
        });
        dbmigrate.on('exit', function (code) {
          console.log("child process exit code", code)
        })

      }.bind(this));
    },

    'does not cause an error': function(code) {
      assert.isNull(code);
    },

    'will create a new migration': function(code) {
      var files = fs.readdirSync(path.join(__dirname, 'migrations'));

      for (var i = 0; i<files.length; i++) {
        var file = files[i];
        var stats = fs.statSync(path.join(__dirname, 'migrations', file));
        if (stats.isFile()) assert.match(file, /cql-migration\.js$/);
      }
    },

    'will create a new migration/sqls directory': function(code) {
      var stats = fs.statSync(path.join(__dirname, 'migrations/sqls'));
      assert.isTrue(stats.isDirectory());
    },

    'will create a new migration sql up file': function(code) {
      var files = fs.readdirSync(path.join(__dirname, 'migrations/sqls'));
      assert.equal(files.length, 2);
      var file = files[1];
      assert.match(file, /cql-migration-up\.sql$/);
    }
  }
}).export(module)
