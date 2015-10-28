var follow = require('follow')
var jsonist = require('jsonist')

module.exports = function (couchdb, eleasticsearch, mapper, addRaw, log, since) {
  var config = {
    db: couchdb,
    include_docs: true,
    since: since || 'now'
  }
  follow(config, function (err, change) {
    if (err) return log.error(err)

    var doc = change.doc
    var es_doc_url = eleasticsearch + '/' + doc._id

    if (doc._deleted) {
      // delete the doc from es
      return jsonist.delete(es_doc_url, onDone.bind(null, log, doc._id, null, 'delete', change.seq))
    }
    var _rev = doc._rev
    if (mapper) {
      var mapped = mapper(change.doc)
      if (mapped && addRaw) mapped.raw = change.doc
      doc = mapped
    }
    jsonist.put(es_doc_url, doc, onDone.bind(null, log, change.doc._id, _rev, 'update', change.seq))

  })
}

function onDone (log, _id, _rev, type, seq, err, resp) {
  if (err) return log.error('error occured', type, _id, _rev, err)

  log.info({ change: seq }, 'success. ', type, _id, _rev, err)
}
