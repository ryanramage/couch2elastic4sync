// Loads elastic search for the very fisrt time
var ndjson = require('ndjson')
var through2 = require('through2')
var hyperquest = require('hyperquest')
var to_es = require('ndjson-to-elasticsearch')

module.exports = function (config, log, id, done) {
  var to_elastic_config = config.load
  to_elastic_config.url = config.elasticsearch
  to_elastic_config.urlTemplate = config.urlTemplate
  to_elastic_config.removeMeta = config.removeMeta
  to_elastic_config.key = config.key || '_id'
  console.log(config.database + '/' + id)

  return hyperquest(config.database + '/' + id)
    .pipe(ndjson.parse())
    .pipe(through2.obj(function (doc, enc, cb) {
      let result = doc

      if (config.mapper) {
        try {
          result = config.mapper(doc)
          if (!result) return cb()
          if (result && config.addRaw) result[config.rawField] = doc
          result._id = doc._id // we need this to work correct
        } catch (e) {
          log.error(e)
          return cb()
        }
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
      console.log('done')
    })
}
