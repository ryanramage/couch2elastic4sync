var async = require('async')
var follow = require('follow')
var recover = require('recover-couch-doc')
var jsonist = require('jsonist')
var remove_meta = require('./remove-meta')
var template = require('lodash').template
var once = require('lodash').once

module.exports = function (config, log, since) {
  var _retry = async.retry.bind(null, config.retry)
  var follow_config = {
    db: config.database,
    include_docs: true,
    since: since || 'now'
  }
  var checkpoint_counter = 0
  var updating_checkpoint_counter = false
  var last_seen_seq = 0

  var shutdown = function () {
    log.info('shutdown called')
    if (!last_seen_seq) return process.exit(0)
    jsonist.put(config.seq_url, {_meta: {seq: last_seen_seq }}, function (err, resp) {
      if (err) log.error('Could not record sequence in elasticsearch', last_seen_seq, err)
      else log.info({type: 'checkpoint', seq: last_seen_seq}, 'stored in elasticsearch')
      process.exit(0)
    })

  }

  var onDone = function (endTask, log, _id, _rev, type, seq, err, resp, _prev_es_doc) {
    if (seq > last_seen_seq) last_seen_seq = seq
    if (err) {
      log.error('error occured', type, _id, _rev, err)
      return endTask()
    }
    var logged = {type: 'change', seq: seq, change: type, id: _id, rev: _rev, err: err}
    if (_prev_es_doc) logged.doc = _prev_es_doc
    log.info(logged, 'success')
    checkpoint_counter++
    if (!updating_checkpoint_counter && checkpoint_counter > config.checkpointSize) {
      updating_checkpoint_counter = true
      checkpoint_counter = 0
      // store the thing change seq
      jsonist.put(config.seq_url, {_meta: {seq: seq }}, function (err, resp) {
        updating_checkpoint_counter = false
        if (err) log.error('Could not record sequence in elasticsearch', seq, err)
        else log.info({type: 'checkpoint', seq: last_seen_seq}, 'stored in elasticsearch')
        return endTask()
      })
    }
    else return endTask()
  }

  var q = async.queue(function (change, endTask) {
    if (change.id.indexOf('_design') === 0) return endTask()

    var doc = change.doc
    var es_doc_url = config.elasticsearch + '/' + change.id
    var compiled
    if (config.urlTemplate) es_doc_url = run_compile(es_doc_url, doc, log)
    if (doc._deleted) return handle_delete(config, es_doc_url, change, log, onDone, endTask)

    var _rev = doc._rev
    if (config.mapper) {
      try {
        var mapped = config.mapper(change.doc)
        if (mapped && config.addRaw) mapped[config.rawField] = change.doc
        doc = mapped
      } catch (e) {
        log.error(e)
        return log.error({change: feed.original_db_seq}, change.doc._id, _rev, 'An error occured in the mapping', e)
      }
    }
    if (!doc) {
      return log.error({change: feed.original_db_seq}, change.doc._id, _rev, 'No document came back from the mapping')
    }
    if (config.removeMeta) {
      doc = remove_meta(doc)
    }
    return _retry(jsonist.put.bind(null, es_doc_url, doc), onDone.bind(null, endTask, log, change.doc._id, _rev, 'update', change.seq))

  }, config.concurrency)

  var _caughtUp = false
  var _drained = false
  var _shutdown = once(shutdown)

  q.drain = function () {
    _drained = true
    log.info({_caughtUp: _caughtUp}, 'drain called')
    if (_caughtUp) setTimeout(_shutdown, 400)
  }

  var feed = follow(follow_config, function (err, change) {
    if (err) return log.error(err)
    q.push(change)
  })

  if (config.endOnCatchup) {
    feed.on('catchup', function () {
      log.info('catchup event received. Pausing feed')
      _caughtUp = true
      feed.pause()
      var remain = q.length()
      if (remain === 0) {
        log.info('no tasks. shutdown')
        return _shutdown()
      }
      var ensureComplete = function () {
        var now_remain = q.length()
        log.info(now_remain + ' tasks remain')
        if (now_remain === remain) {
          log.info('no queue change. shutdown')
          return _shutdown()
        }
        remain = now_remain
        setTimeout(ensureComplete, 4000)
      }
      setTimeout(ensureComplete, 4000)
    })
  }

  feed.on('confirm', function (info) {
    log.info({type: 'start', seq: feed.original_db_seq}, 'started')
  })

}


function handle_delete(config, es_doc_url, change, log, onDone, endTask) {
  var tasks = []
  var _prev_es_doc
  var _err
  var _es_doc_url = es_doc_url

  if (config.urlTemplate) {
    tasks.push(function (cb) {
      recover(config.database, change.id, function (err, _prev_couch_doc) {
        if (err) _err = err
        _es_doc_url = run_compile(es_doc_url, _prev_doc, log)
        cb()
      })
    })
  }
  if (config.logDeleted) {
    tasks.push(function (cb) {
      jsonist.get(_es_doc_url, function(err, prev_es_doc) {
        if (err) _err = err
        _prev_es_doc = prev_es_doc
        cb()
      })
    })
  }
  tasks.push(function (cb) {
    jsonist.delete(_es_doc_url, cb)
  })

  async.series(tasks, function (err) {
    onDone(endTask, log, change.id, null, 'delete', change.seq, _err, null, _prev_es_doc)
  })
}

function run_compile(es_doc_url, doc, log) {
  compiled = template(es_doc_url)
  try {
    compiled(doc)
  } catch (e) {
    log.error(e)
    return null
  }
}


