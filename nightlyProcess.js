/**
 * Created by wpatterson on 1/13/2016.
 * Simple process to run our 4 steps
 */
/*jshint strict:false */
/*jshint node:true */
var async = require('async'),
    spawn = require('child_process').spawn;

function runScript(step, cb) {
  var op = spawn('node', [step]);

  op.stdout.setEncoding('utf8');

  op.stdout.on('data', function (data) {
    console.log(data);
  });

  op.on('exit', function (code) {
    if (code > 0) {
      cb('Oh no, there seems to be an error: ' + code);
    } else {
      cb();
    }
  });
}

// run steps in a series
async.series([
  function(cb) {
    runScript('step1', cb);
  },
  function(cb) {
    runScript('step2', cb);
  },
  function(cb) {
    runScript('step3', cb);
  },
  function(cb) {
    runScript('step4', cb);
  }
], function (err) {
  if(err) {
    process.exit(err);
  } else {
    process.exit();
  }
});
