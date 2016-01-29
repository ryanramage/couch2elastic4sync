// Loads elastic search for the very fisrt time
var ndjson = require('ndjson')
var through2 = require('through2')
var jsonfilter = require('jsonfilter')
var hyperquest = require('hyperquest')
var to_couch = require('ndjson-to-couchdb')
var remove_meta = require('./remove-meta')

module.exports = function (config, log) {
  var to_elastic_config = config.load
  to_elastic_config.url = config.elasticsearch

  return hyperquest(config.database + '/_all_docs?include_docs=true')
    .pipe(jsonfilter('rows.*'))
    .pipe(ndjson.parse())
    .pipe(through2.obj(function (row, enc, cb) {
      var result = row.doc
      if (config.mapper) {
        result = config.mapper(row.doc)
        if (result && config.addRaw) result[config.rawField] = row.doc
      }
      if (config.removeMeta) {
        result = remove_meta(result)
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
