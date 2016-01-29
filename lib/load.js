// Loads elastic search for the very fisrt time
var ndjson = require('ndjson')
var through2 = require('through2')
var jsonfilter = require('jsonfilter')
var hyperquest = require('hyperquest')
var to_couch = require('ndjson-to-couchdb')

module.exports = function (couchdb, eleasticsearch, mapper, addRaw, log) {
  var to_elastic_config = {
    url: eleasticsearch
  }
  console.log(to_elastic_config)
  return hyperquest(couchdb + '/_all_docs?include_docs=true')
    .pipe(jsonfilter('rows.*'))
    .pipe(ndjson.parse())
    .pipe(through2.obj(function (row, enc, cb) {
      var result = row.doc
      if (mapper) {
        result = mapper(row.doc)
        if (result && addRaw) result.raw = row.doc
      }
      cb(null, result)
    }))
    //
    .pipe(to_couch(to_elastic_config)) // this is actually piping to elasticsearch
    .on('error', function (err) {
      log.error('document error', err)
    })
    .pipe(ndjson.stringify())
}
