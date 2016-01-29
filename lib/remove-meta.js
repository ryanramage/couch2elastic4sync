module.exports = function (doc) {
  if (!doc) return
  delete doc._id
  delete doc._all
  delete doc._parent
  delete doc._routing
  delete doc._timestamp
  delete doc._ttl
  return doc
}
