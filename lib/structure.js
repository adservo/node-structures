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
  // source: method to call to invoke the query
  // parent_key: the parent field name for attaching related children
  // key: the field name in our rows to match to the parent rows
  // children: optional substructures for retrieving related sub-data
  this.structure = structure;

  // Parent is set if we're from a children substructure
  this.parent = parent;

  // Store the params to pass on to queries
  this.params = params;

  this.name = name;
  this.prepare();
};

Util.inherits(Structure, EventEmitter);

var p = Structure.prototype;

p.init = function () {
  // Execute the source query for this part of the structure
  this.structure.source(this.params)
    .on('row', Array.isArray(this.rows) ? this.rootRow.bind(this) : this.childRow.bind(this))
    .on('end', this.end.bind(this))
    .execute();
};

p.prepare = function () {
  // This is the root, so we just place the result rows into an array
  if (!this.parent) {
    this.rows = [];
    return;
  }
  if (!this.structure.parent_key) {
    throw 'No parent key defined for : ' + Util.inspect(this.structure);
  }
  if (!this.structure.key) {
    throw 'No key defined for : ' + Util.inspect(this.structure);
  }
  if (!this.parent.rows) {
    throw 'No rows defined for parent : ' + Util.inspect(this.parent);
  }
  if (!this.parent.rows[0][this.structure.parent_key]) {
    throw 'Parent result is missing parent_key field : ' + Util.inspect(this.structure);
  }

  // Create a placholder index into child rows
  this.rows = {};
  // Iterate the parent rows
  this.parent.rows.forEach(function (row) {
    // Ignore parent rows where parent_key field is null-ish
    if (!row[this.structure.parent_key]) { return; }
    // Create results placeholder on each parent row
    row[this.name] = [];
    // Create the indexed by parent key pointer into the parent rows
    this.rows[row[this.structure.parent_key]] = row[this.name];
  }.bind(this));
};

// If it's the root query then we can just stuff the result into the array
p.rootRow = function (row) {
  this.rows.push(row);
};

// Child rows need to honour the keys
p.childRow = function (row) {
  this.rows[row[this.structure.key]].push(row);
};

// This particular part of the structure has completed
p.end = function () {
  // Only if we have rows should we bother trying to fetch any child rows
  if (this.rows.length && this.structure.children) {
    this.pending = 0;
    this.keys = {};
    Object.keys(this.structure.children).forEach(function (name) {
      this.pending++;
      new Structure(this.structure.children[name], this, this.params, name)
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
    this.emit('done', this.rows);
  }
};

module.exports = Structure;
