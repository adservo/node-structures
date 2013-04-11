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

var Structure = function(structure, parent, params) {
  EventEmitter.call(this);
  console.log('received structure : ');
  console.log(structure);

  // Structure to fulfil. Contains :
  // source: method to call to invoke the query
  // key: the key field for attaching related data (from children)
  // children: substructures for retrieving related data
  this.structure = structure;

  // Parent is set if we're from a children substructure
  this.parent = parent;

  // Store the params to pass on to queries
  this.params = params;

  // this.source = structure.source;
  // this.key = structure.key;
  // this.children = structure.children;

  if(!this.parent) {
    this.target = [];
  }
}

Util.inherits(Structure, EventEmitter);

var p = Structure.prototype;

p.init = function() {
  // Execute the source query for this part of the structure
  this.structure.source(this.params)
    .on('row', Array.isArray(this.target) ? this.rootRow.bind(this) : this.childRow.bind(this))
    .on('end', this.end.bind(this))
    .execute();
}

// If it's the root query then we can just stuff the result into the array
p.rootRow = function(row) {
  console.log('Root row');
  this.target.push(row)
};

// Child rows need to honour the keys
p.childRow = function(row) {
  console.log('Child row');
  this.target.push(row)
};

p.end = function() {
  if(this.structure.children) {
    this.keys = {};
    console.log('All done');
  }
  // else {
    this.emit('done', this.target)
  // }
};

// p.attach_relations = function(relations) {
//   this.relations = relations;
//   // Make an array of the names of the relations we'll now asynchronously load in parallel, so that we can remove
//   // them one by one when done and know all calls are completed when the array is empty
//   this.relation_names = Object.keys(relations);
//   this.ids = {};
//   this.data.forEach(function(row) {
//     // Build a pointer hash into the rows by id for fast reference
//     this.ids[row.id] = row;
//     // Add an array to each row to contain the child relations
//     this.relation_names.forEach(function(rel) { row[rel] = [] });
//   }, this);
//   this.match_ids = Object.keys(this.ids);
//   this.relation_names.forEach(function(rel) {
//     var q = this.relations[rel][0]({ ids: this.match_ids }).query();
//     pg_pool.query(q.text, q.values)
//       .on('error', console.error)
//       .on('end', this.child_completed.bind({ obj:this, target: rel }))
//       .on('row', this.child_row.bind({ ids: this.ids, target: rel, options: this.relations[rel][1] }));
//   }.bind(this));
// }

// p.child_row = function(row) {
//   this.ids[row[this.options.key || (this.target + '_id')]][this.target].push(row);
// }

// p.child_completed = function() {
//   this.obj.relation_names.splice(this.obj.relation_names.indexOf(this.target), 1);
//   if(this.obj.relation_names.length == 0) { this.obj.send(); }
// }

// p.send = function() {
//   this.behaviour.success(new metadata(this.req, this.data).wrap());
// }

module.exports = Structure;
