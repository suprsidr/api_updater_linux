/**
 * @author wpatterson
 * simple file operations
 */
var fs = require('fs');

function File() {

  function save(path, val, cb) {
    fs.writeFile(path, val, function(err) {
      if(err) {
        cb(err);
      } else {
        cb();
      }
    });
  }

  function copy(source, target, cb) {
    var cbCalled = false;

    var rd = fs.createReadStream(source);
    rd.on("error", function(err) {
      _done(err);
    });
    var wr = fs.createWriteStream(target);
    wr.on("error", function(err) {
      _done(err);
    });
    wr.on("close", function(ex) {
      _done();
    });
    rd.pipe(wr);

    function _done(err) {
      if (!cbCalled) {
        cb(err);
        cbCalled = true;
      }
    }
  }

  this.save = save;
  this.copy = copy;
}

module.exports = new File;
