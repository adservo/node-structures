'use strict';

// This is basically a single db request, which will add the results to the "target" area of the structure
var Branch = function Branch(query, target, options) {
  this.query = query;
  this.target = target;
  this.options = options || {};
}

var p = Branch.prototype;

p.load = function() {
  // Load the data into the target structure area
  log('target');
  log(target);

  var self = this;
  pg_pool.query(this.options.source().query())
    .on('error', console.error)
    .on('row', function(row) { self.row_received(row, this.target) })
    .on('end', function() { self.emit('end', self.data); });
}

p.row_received = function(row, target) {
  // 'this' is the target array for the row
  log('row :');
  log(row);
  log('this :');
  log(this);
  target.push(row);
}

module.exports = Branch;
