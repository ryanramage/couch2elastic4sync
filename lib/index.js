var async = require('async')
var follow = require('follow')
var request = require('request')
var jsonist = require('jsonist')
var remove_meta = require('./remove-meta')
var template = require('lodash.template')
var once = require('lodash.once')

module.exports = function (config, log, since) {
  var _retry = async.retry.bind(null, config.retry)
  var follow_config = {
    db: config.database,
    include_docs: true,
    since: since || 'now'
  }
  var checkpoint_counter = 0
  var updating_checkpoint_counter = false
  var last_seen_seq
  var pending = {}
  var _shutdown_attempts = 0
  var _shutdown_last_pending

  var shutdown = function () {
    log.info('shutdown called')
    var _remain = Object.keys(pending).length
    if (_remain > 0) {
      if (!_shutdown_last_pending) _shutdown_last_pending = _remain
      if (_remain < _shutdown_last_pending) {
        _shutdown_last_pending = _remain
        log.info({pending: _remain}, 'still processing')
        return setTimeout(shutdown, config.shutdown.interval);
      }


      if (_shutdown_attempts > config.shutdown.times) {
        log.error('shutdown wait time exceeded. shutting down anyway.')

      } else {
        log.info({pending: _remain, attempts: _shutdown_attempts++}, 'there are pending keys, sleeping')
        return setTimeout(shutdown, config.shutdown.interval);
      }
    }

    if (!last_seen_seq) return process.exit(0)
    jsonist.put(config.seq_url, {_meta: {seq: last_seen_seq }}, function (err, resp) {
      if (err) log.error('Could not record sequence in elasticsearch', last_seen_seq, err)
      else log.info({change: last_seen_seq}, 'stored in elasticsearch. ')
      process.exit(0)
    })

  }

  var onDone = function (log, _id, _rev, type, seq, err, resp) {
    if (seq > last_seen_seq) last_seen_seq = seq
    delete pending[seq]
    if (err) return log.error('error occured', type, _id, _rev, err)
    log.info({change: seq}, 'success. ', type, _id, _rev, err)
    checkpoint_counter++
    if (!updating_checkpoint_counter && checkpoint_counter > config.checkpointSize) {
      updating_checkpoint_counter = true
      checkpoint_counter = 0
      // store the thing change seq
      jsonist.put(config.seq_url, {_meta: {seq: seq }}, function (err, resp) {
        updating_checkpoint_counter = false
        if (err) return log.error('Could not record sequence in elasticsearch', seq, err)
        log.info({change: seq}, 'stored in elasticsearch. ')
      })
    }

  }

  var feed = follow(follow_config, function (err, change) {
    if (err) return log.error(err)
    if (change.id.indexOf('_design') === 0) return

    pending[change.seq] = true

    var doc = change.doc
    var es_doc_url = config.elasticsearch + '/' + change.id
    var compiled
    if (config.urlTemplate) {
      compiled = template(es_doc_url)
      es_doc_url = compiled(doc)
    }
    var _onDone

    if (doc._deleted) {
      // delete the doc from es
      if (config.urlTemplate) {
        // we need the prev doc. hack attack
        request({
          url: config.database + '/' + change.id + '?revs=true&open_revs=all'
        }, function (err, resp, body) {
          // total hacky
          var _json = JSON.parse(body.split('\n')[3])
          var _rev = (_json._revisions.start - 1) + '-' + _json._revisions.ids[1]
          request({
            url: config.database + '/' + change.id,
            qs: {
              rev: _rev
            },
            json: true
          }, function (err, resp, _prev_doc) {
            try {
              es_doc_url = compiled(_prev_doc)
              _retry(jsonist.delete.bind(null, es_doc_url), onDone.bind(null, log, doc._id, null, 'delete', change.seq))
            } catch (e) {}
          })
        })
        return
        // end of finding prev doc for delete of a urlTemplate
      } else {
        return _retry(jsonist.delete.bind(null, es_doc_url), onDone.bind(null, log, doc._id, null, 'delete', change.seq))
      }
    }

    var _rev = doc._rev
    if (config.mapper) {
      try {
        var mapped = config.mapper(change.doc)
        if (mapped && config.addRaw) mapped[config.rawField] = change.doc
        doc = mapped
      } catch (e) {
        delete pending[change.seq]
        return log.error({change: feed.original_db_seq}, change.doc._id, _rev, 'An error occured in the mapping', e)
      }
    }
    if (!doc) {
      delete pending[change.seq]
      return log.error({change: feed.original_db_seq}, change.doc._id, _rev, 'No document came back from the mapping')
    }
    if (config.removeMeta) {
      doc = remove_meta(doc)
    }
    _retry(jsonist.put.bind(null, es_doc_url, doc),  onDone.bind(null, log, change.doc._id, _rev, 'update', change.seq))
  })

  var _shutdown = once(shutdown)
  if (config.endOnCatchup) {
    feed.on('catchup', function () {
      feed.pause()
      setInterval(_shutdown, 400)
    })
  }

  feed.on('confirm', function (info) {
    log.info({change: feed.original_db_seq}, 'started')
  })

}
