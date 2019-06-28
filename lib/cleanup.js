var async = require('async')
var jsonist = require('jsonist')
var request = require('request')

module.exports = function (config, log, done) {
  var es_doc_url = config.elasticsearch + '/_search'
  var body = {
    query: {
      match_all: {}
    },
    fields: [],
    size: config.limit || 100000
  }
  jsonist.post(es_doc_url, body, (err, resp) => {
    if (err) return done(err)
    let ids = resp.hits.hits.map(_row => _row._id)
    async.eachLimit(ids, config.concurrency, (id, cb) => {
      let url = config.database + '/' + id
      let opts = { url, method: 'HEAD'}
      request(opts, (err, resp) => {
        if (err) return cb(err)
        if (resp.statusCode !== 404) return cb()
        let es_doc_url = config.elasticsearch + '/' + id
        opts.url = es_doc_url
        opts.method = 'DELETE'
        log.info('removing', es_doc_url)
        request(opts, cb)
      })
    }, done)
  })
}
