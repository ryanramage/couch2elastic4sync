var follow = require('follow')
var jsonist = require('jsonist')

module.exports = function (couchdb, eleasticsearch, mapper, since) {
  var config = {
    db: couchdb,
    include_docs: true,
    since: since || 'now'
  }
  follow(config, function (err, change) {
    if (err) return console.log('Error:', err)
    var doc = change.doc
    var es_doc_url = eleasticsearch + '/' + doc._id

    if (doc._deleted) {
      // delete the doc from es
      return jsonist.delete(es_doc_url, onDone.bind(null, doc._id, null, 'delete'))
    }
    var _rev = doc._rev
    if (mapper) {
      doc = mapper(change.doc)
    }
    jsonist.put(es_doc_url, doc, onDone.bind(null, doc._id, _rev, 'update'))

  })
}

function onDone (_id, _rev, type, err, resp) {
  var msg = 'success. '
  if (err) msg = 'ERROR: '
  console.log(msg, type, _id, _rev, err)
}
