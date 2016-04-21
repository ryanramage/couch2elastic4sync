// Loads elastic search for the very fisrt time
var ndjson = require('ndjson')
var through2 = require('through2')
var jsonfilter = require('jsonfilter')
var jsonist = require('jsonist')
var hyperquest = require('hyperquest')
var to_es = require('ndjson-to-elasticsearch')

module.exports = function (config, log, done) {
  var to_elastic_config = config.load
  to_elastic_config.url = config.elasticsearch
  to_elastic_config.urlTemplate = config.urlTemplate
  to_elastic_config.removeMeta = config.removeMeta
  to_elastic_config.key = config.key || '_id'

  return hyperquest(config.database + '/_all_docs?include_docs=true')
    .pipe(jsonfilter('rows.*'))
    .pipe(ndjson.parse())
    .pipe(through2.obj(function (row, enc, cb) {
      // ignore design docs
      if (row.id.indexOf('_design') === 0) return cb()
      var result = row.doc
      if (config.mapper) {
        result = config.mapper(row.doc)
        if (result && config.addRaw) result[config.rawField] = row.doc
        result._id = row.id // we need this to work correct
      }
      cb(null, result)
    }))
    //
    .pipe(to_es(to_elastic_config))
    .on('error', function (err) {
      log.error('document error', err)
    })
    .pipe(ndjson.stringify())
    .once('end', function () {
      // save the seq
      jsonist.get(config.database, function (err, about) {
        if (err) return done(err)
        jsonist.put(config.seq_url, {_meta: {seq: about.update_seq }}, function (err, resp) {
          return done(err)
        })
      })
    })
}
