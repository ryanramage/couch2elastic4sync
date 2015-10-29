var follow = require('follow')
var jsonist = require('jsonist')

module.exports = function (couchdb, eleasticsearch, mapper, addRaw, log, since, end_on_catchup) {
  var config = {
    db: couchdb,
    include_docs: true,
    since: since || 'now'
  }

  var pending = {}


  var shutdown = function () {
    if (Object.keys(pending).length ) return
    process.exit(0)
  }

  var onDone = function  (log, _id, _rev, type, seq, err, resp) {
    if (err) return log.error('error occured', type, _id, _rev, err)
    log.info({ change: seq }, 'success. ', type, _id, _rev, err)
    delete pending[seq]
  }

  var feed = follow(config, function (err, change) {
    if (err) return log.error(err)

    pending[change.seq] = true

    var doc = change.doc
    var es_doc_url = eleasticsearch + '/' + doc._id

    if (doc._deleted) {
      // delete the doc from es
      return jsonist.delete(es_doc_url, onDone.bind(null, log, doc._id, null, 'delete', change.seq))
    }
    var _rev = doc._rev
    if (mapper) {
      try {
        var mapped = mapper(change.doc)
        if (mapped && addRaw) mapped.raw = change.doc
        doc = mapped
      } catch (e) {
        delete pending[change.seq]
        return log.error({ change: feed.original_db_seq }, change.doc._id, _rev, 'An error occured in the mapping', e)
      }
    }
    if (!doc) {
      delete pending[change.seq]
      return log.error({ change: feed.original_db_seq }, change.doc._id, _rev, 'No document came back from the mapping')
    }
    jsonist.put(es_doc_url, doc, onDone.bind(null, log, change.doc._id, _rev, 'update', change.seq))
  })

  if (end_on_catchup) {
    feed.on('catchup', function() {
      feed.pause()
      setInterval(shutdown, 400)
    })
  }

  feed.on('confirm', function(info) {
    log.info({ change: feed.original_db_seq }, 'started')
  })

}


