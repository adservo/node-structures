/*
 * structures
 * 
 *
 * Copyright (c) 2013 Mark Selby
 * Licensed under the MIT license.
 */

'use strict';

var EventEmitter = require('events').EventEmitter;
var Util = require('util');

var Structure = function (structure, parent, params, name) {
  EventEmitter.call(this);

  // Structure to fulfil. Contains :
  // source: method to call that returns a query
  // parent_key: the parent field name for attaching related children
  // key: the field name in our rows to match to the parent rows
  // children: optional substructures for retrieving related sub-data
  this.structure = structure;

  // Parent is set if we're from a children substructure
  this.parent = parent;

  // Store the params to pass on to queries
  this._params = params;

  // Child rows take their name from the structure declaration
  this.name = name;

  // console.log(structure);

  this.prepare();
};

Util.inherits(Structure, EventEmitter);

var p = Structure.prototype;

p.init = function () {
  // Execute the source query for this part of the structure
  this.structure.source(this.params())
    .on('row', Array.isArray(this._rows) ? this.rootRow.bind(this) : this.childRow.bind(this))
    .on('error', this.error.bind(this))
    .on('end', this.end.bind(this))
    .execute();
};

p.prepare = function () {
  // This is the root, so we just place the result rows into an array
  if (!this.parent) {
    this._rows = [];
    return;
  }
  if (!this.structure.parent_key) {
    throw 'No parent key defined for : ' + Util.inspect(this.structure);
  }
  if (!this.structure.key) {
    throw 'No key defined for : ' + Util.inspect(this.structure);
  }
  if (!this.parentRows()) {
    throw 'No rows defined for parent : ' + Util.inspect(this.parent);
  }
  // console.log('Parent rows : ' + Util.inspect(this.parent.rows()[0]));
  // if (!this.parent._rows[0][this.structure.parent_key]) {
  if (!this.parentRows()[0][this.structure.parent_key]) {
    throw 'Parent result is missing parent_key field : ' + Util.inspect(this.structure);
  }

  // Create a placholder index into child rows
  this._rows = {};
  // Iterate the parent rows
  this.parentRows().forEach(function (row) {
    // Ignore parent rows where parent_key field is null-ish
    if (!row[this.structure.parent_key]) { return; }
    // Create results placeholder on each parent row
    row[this.name] = [];
    // Create the indexed by parent key pointer into the parent rows
    this._rows[row[this.structure.parent_key]] = row[this.name];
  }.bind(this));
};

// If it's the root query then we just stuff the result into the array from prepare()
p.rootRow = function (row) {
  this._rows.push(row);
};

// Child rows need to honour the keys
p.childRow = function (row) {
  this._rows[row[this.structure.key]].push(row);
};

p.params = function () {
  // return Array.isArray(this._rows) ? this._params : { id: Object.keys(this._rows) };
  return this.parent ? { id: Object.keys(this._rows) } : this._params;
};

// Return the rows as an array, whether array (root) or hash (child data)
p.parentRows = function () {
  if (!this._parentRows) {
    this._parentRows = Array.isArray(this.parent._rows) ? this.parent._rows : [].concat.apply([], Object.keys(this.parent._rows).map(function (k) { return this.parent._rows[k]; }.bind(this)));
  }
  return this._parentRows;
};

// This particular part of the structure has completed
p.end = function () {
  // Only if we have rows should we bother trying to fetch any child rows
  // if (this._rows.length && this.structure.children) {
  if (this.structure.children) {
    this.pending = 0;
    this.keys = {};
    Object.keys(this.structure.children).forEach(function (name) {
      this.pending++;
      new Structure(this.structure.children[name], this, this.params(), name)
        .on('done', this.childDone.bind(this))
        .on('error', this.error.bind(this))
        .init();
    }.bind(this));
  } else {
    this.emit('done', this.target);
  }
};

p.error = function (err) {
  throw 'Structure error : ' + err + ' processing ' + Util.inspect(this.structure);
};

p.childDone = function () {
  this.pending--;
  if (!this.pending) {
    this.emit('done', this._rows);
  }
};

module.exports = Structure;
