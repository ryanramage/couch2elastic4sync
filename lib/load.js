// Loads elastic search for the very fisrt time
var ndjson = require('ndjson')
var through2 = require('through2')
var jsonfilter = require('jsonfilter')
var hyperquest = require('hyperquest')
var to_couch = require('ndjson-to-couchdb')

module.exports = function (couchdb, eleasticsearch, mapper) {
  var to_elastic_config = {
    url: eleasticsearch
  }
  return hyperquest(couchdb + '/_all_docs?include_docs=true')
    .pipe(jsonfilter('rows.*'))
    .pipe(ndjson.parse())
    .pipe(through2.obj(function (row, enc, cb) {
      var doc = row.doc
      if (mapper) doc = mapper(row.doc)
      cb(null, doc)
    }))
    //
    .pipe(to_couch(to_elastic_config)) // this is actually piping to elasticsearch
    .pipe(ndjson.stringify())
}
