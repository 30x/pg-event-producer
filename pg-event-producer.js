'use strict'
const http = require('http')
const util = require('util')
const rLib = require('@apigee/response-helper-functions')
const lib = require('@apigee/http-helper-functions')
const keepAliveAgent = new http.Agent({ keepAlive: true });

var SPEEDUP = process.env.SPEEDUP || 1
var ONEMINUTE = 60*1000/SPEEDUP
var TWOMINUTES = 2*ONEMINUTE
var TENMINUTES = 10*ONEMINUTE
var ONEHOUR = 60*ONEMINUTE
var ONEDAY = 24*ONEHOUR
var ONEYEAR = 365*ONEDAY

var INTERNAL_SCHEME = process.env.INTERNAL_SCHEME || 'http'

const MAX_RECORD_SIZE = process.env.MAX_RECORD_SIZE || 1e5

function eventProducer(pool, tableName, idColumnName) {
  this.pool = pool
  this.consumers = []
  this.tableName = tableName
  this.idColumnName = idColumnName
}

 eventProducer.prototype.log = function(method, err) {
  console.log((new Date).toISOString(), this.componentName, method, util.inspect(err, false, null))
}

eventProducer.prototype.init = function(callback) {
  var self = this
  this.createTablesThen(function () {
    self.getListeners(self, function () {
      self.getListenerTimer = setInterval(self.getListeners, ONEMINUTE, self)
      self.discardListenerTimer = setInterval(self.discardListenersOlderThan, TWOMINUTES, TENMINUTES, self)
      self.discardEventTimer = setInterval(self.discardEventsOlderThan, ONEDAY, ONEYEAR, self)
      callback()
    })
  })  
}

eventProducer.prototype.finalize = function() {
  console.log('pg-event-producer finalizing')
  this.getListenerTimer.unref()
  this.discardListenerTimer.unref()
  this.discardEventTimer.unref()
}

eventProducer.prototype.discardListenersOlderThan = function(interval, self) {
  var time = Date.now() - interval
  var pool = self.pool
  pool.query(`DELETE FROM consumers WHERE registrationtime < ${time}`, function (err, pgResult) {
    if (err) 
      console.log('discardListenersOlderThan:', `unable to delete old consumers ${err}`)
    else
      console.log('discardListenersOlderThan:', `trimmed consumers older than ${time}`)
  })
}

eventProducer.prototype.getListeners = function(self, callback) {
  var query = 'SELECT ipaddress FROM consumers'
  var pool = self.pool
  pool.query(query, function (err, pgResult) {
    if (err)
      console.log(`unable to retrieve ipaddresses from consumers`)
    else
      self.setConsumers(pgResult.rows.map(row => row.ipaddress))
    if (callback) 
      callback()
  })
}

eventProducer.prototype.discardEventsOlderThan = function(interval, self) {
  var time = Date.now() - interval
  var pool = self.pool
  pool.query(`DELETE FROM events WHERE eventtime < ${time} RETURNING index`, function (err, pgResult) {
    if (err)
      console.log('discardEventsOlderThan:', `unable to delete old events ${err}`)
    else
      console.log(`discardEventsOlderThan: ${interval} found: ${pgResult.rows.map(row => row.index)}`)
  })
}

eventProducer.prototype.createTablesThen = function(callback) {
  var query = 'CREATE TABLE IF NOT EXISTS events (index bigserial, topic text, eventtime bigint, data jsonb)'
  var pool = this.pool
  pool.query(query, function(err, pgResult) {
    if (err && err.code != 23505)
      console.error('error creating events table', err)
    else {
      query = 'CREATE TABLE IF NOT EXISTS consumers (ipaddress text primary key, registrationtime bigint)'
      pool.query(query, function(err, pgResult) {
        if(err && err.code != 23505)
          console.error('error creating consumers table', err)
        else
          callback()
      })
    }
  })
}

eventProducer.prototype.setConsumers = function(consumers) {
  console.log('setConsumers:', 'consumers:', consumers)
  this.consumers = consumers
}

eventProducer.prototype.tellConsumers = function(req, event, callback) {
  var count = 0
  var total = this.consumers.length
  if (total > 0) {
    var responded = false
    for (var i = 0; i < total; i++) {
      let listener = this.consumers[i]
      sendEventThen(req, event, listener, function(err) {
        if (err)
          console.log(`component: ${process.env.COMPONENT_NAME}, failed to send event ${event.index} to ${listener} err: ${err}`)
        else
          console.log(`component: ${process.env.COMPONENT_NAME}, sent event ${event.index} to ${listener} index: ${event.index}`)
        if (++count == total && !responded) {
          responded = true
          callback()
        }
      })
    }
    setTimeout(function() { // don't wait more than 50ms for listeners 
      if (!responded) {
        responded = true 
        callback()
      }
    }, 50)
  } else
    callback()
}

function sendEventThen(serverReq, event, host, callback) {
  var postData = JSON.stringify(event)
  var headers = {
    'Accept': 'application/json',
    'content-type': 'application/json',
    'content-length': Buffer.byteLength(postData)
  }
  if (serverReq.headers.authorization)
    headers.authorization = serverReq.headers.authorization 
  var hostParts = host.split(':')
  var options = {
    protocol: `${INTERNAL_SCHEME}:`,
    hostname: hostParts[0],
    path: '/az-events',
    method: 'POST',
    headers: headers,
    agent: keepAliveAgent
  }
  if (hostParts.length > 1)
    options.port = hostParts[1]
  var client_req = http.request(options, function (client_res) {
    client_res.setEncoding('utf8')
    var body = ''
    client_res.on('data', chunk => body += chunk)
    client_res.on('end', function() {
      if (client_res.statusCode == 200)  
        callback(null)
      else 
        callback(`unable to send event to: ${host} statusCode: ${client_res.statusCode} body: ${body}`)
    })
  })
  client_req.on('error', function (err) {
    callback(err)
  })
  client_req.setTimeout(2000, function() {
    client_req.abort()
  })
  client_req.write(postData)
  client_req.end()
}

 eventProducer.prototype.beginTransaction = function(errorHandler, callback) {
  let pool = this.pool
  pool.connect(function(err, client, release) {
    if (err)
      rLib.internalError(errorHandler, {msg: `unable to get postgres connection`, err: err, time: time})
    else
      client.query('BEGIN', function(err) {
        if(err) {
          client.query('ROLLBACK', release)
          rLib.internalError(errorHandler, {msg: `unable to beginTransaction postgres transaction`, err: err})
        } else 
          callback(client, release)
      })
  })
}

 eventProducer.prototype.commitTransaction = function(client, release, callback) {
  client.query('COMMIT', (err) => {
    if (err) {
      this.log(`error on transaction COMMIT: ${JSON.stringify(err)}`)
      release(err)
      callback(err)
    } else {
      release()
      callback()
    }
  })
}

 eventProducer.prototype.rollbackTransaction = function(client, release, callback) {
  client.query('ROLLBACK',  (err) => {
    if (err) {
      this.log(`error on transaction ROLLBACK: ${JSON.stringify(err)}`)
      release(err)
      callback(err)
    } else {
      release()
      callback()
    }
  })
  callback()
}

 eventProducer.prototype.executeInTransaction = function(errorHandler, client, release, query, params, callback) {
  if (typeof params == 'function')
    [params, callback] = [undefined, params]
  client.query(query, params, (err, pgResult) => {
    if(err)
      this.rollbackTransaction(client, release, () => {
        if (err.code == 23505) {
          var errMsg = `duplicate key value violates unique constraint ${err.constraint}`
          this.log(errMsg)
          rLib.duplicate(errorHandler, {msg: errMsg})
        } else { 
          var errRslt = {msg: `unable to execute write query`, query: query, err: err, params: params}
          this.log('pg-storage:executeWriteQuery', errRslt)
          rLib.internalError(errorHandler, errRslt)
        }
      })
    else
      callback(pgResult)
  })
}

eventProducer.prototype.selectForUpdateDo = function(errorHandler, client, release, id, ifMatch, callback) {
  var query, params
  if (ifMatch) {
    query = `SELECT data, "${this.idColumnName}" as id FROM "${this.tableName}" WHERE "${this.idColumnName}" = $1 AND etag = $2 FOR UPDATE`  
    params = [id, ifMatch]
  } else {
    query = `SELECT data FROM "${this.tableName}" WHERE id = $1 FOR UPDATE`
    params = [id]
  }
  this.executeInTransaction(errorHandler, client, release, query, params, pgResult => {
    if (pgResult.rowCount == 1)
      callback(pgResult.rows[0].data, pgResult.rows[0].id, pgResult.rows[0].etag)
    else
      this.rollbackTransaction(client, release, () => {
        rLib.notFound(errorHandler, {msg: `resource with id ${id} and etag ${ifMatch} does not exist`})
      })
  })
}

eventProducer.prototype.updateRecord = function(errorHandler, client, release, id, record, etag, ifMatch, callback) {
  var query
  var recordString = JSON.stringify(record)
  if (recordString.length > MAX_RECORD_SIZE) 
    rLib.badRequest(errorHandler, {msg: `size of patched resource may not exceed ${MAX_RECORD_SIZE}`})
  else
    query = `UPDATE "${this.tableName}" SET (etag, data) = ($1, $2) WHERE "${this.idColumnName}" = $3 AND etag = $4`
    this.executeInTransaction(errorHandler, client, release, query, [etag, recordString, id, ifMatch], pgResult => {
      if (pgResult.rowCount == 1)
        callback(record.etag)    
      else
        this.rollbackTransaction(client, release, function() {
          rLib.notFound(errorHandler, {msg: `resource with id ${id} and etag ${ifMatch} does not exist`})
        })
    })
}

eventProducer.prototype.insertAuditEvent = function(errorHandler, client, release, eventTopic, changeEvent, callback) {
  changeEvent.time = Date.now()
  let changeEventString = JSON.stringify(changeEvent)
  var query = `INSERT INTO events (topic, eventtime, data) values ($1, $2, $3) RETURNING *`
  this.executeInTransaction(errorHandler, client, release, query, [eventTopic, changeEvent.time, changeEventString], (pgResult) => {
    if (pgResult.rowCount == 1) {
      callback(pgResult.rows[0])    
    } else
      this.rollbackTransaction(client, release, function() {
        rLib.notFound(errorHandler, {msg: 'could not insert event record'})
      })
  })
}

eventProducer.prototype.updateResourceThen = function(req, errorHandler, subject, resource, ifMatch, previous, eventTopic, changeEvent, callback) {
  this.beginTransaction(errorHandler, (client, release) => {
    changeEvent.time = new Date().toISOString()
    resource.modifier = lib.getUserFromReq(req)
    resource.modified = changeEvent.time
    this.insertAuditEvent(errorHandler, client, release, eventTopic, changeEvent, (eventRecord) => {
      let eventSequenceNumber = eventRecord.index
      // The following line is a critical line. It ensures that the resource stored on disk includes the
      // event index of the corresponding audit event. This allows clients to know reliably whether
      // one version of the resource is more or less recent than any other version.
      resource.eventSequenceNumber = eventSequenceNumber
      resource.etag = `e${(++eventSequenceNumber).toString()}`
      this.updateRecord(errorHandler, client, release, subject, resource, resource.etag, ifMatch, () => {
        this.commitTransaction(client, release, (err) => {
          this.tellConsumers(req, eventRecord, function(){
            callback(eventRecord)
          })
        })
      })
    })
  })
}

eventProducer.prototype.insrtResource = function(errorHandler, client, release, id, record, etag, callback) {
  var query = `INSERT INTO "${this.tableName}" (${this.idColumnName}, etag, data) values ($1, $2, $3)`
  let recordString = JSON.stringify(record)
  if (recordString.length > MAX_RECORD_SIZE) 
    rLib.badRequest(errorHandler, {msg: `size of inserted resource may not exceed ${MAX_RECORD_SIZE}`})
  else
    this.executeInTransaction(errorHandler, client, release, query, [id, etag, recordString], pgResult => {
      if (pgResult.rowCount == 1)
        callback()    
      else
        this.rollbackTransaction(client, release, function() {
          rLib.notFound(errorHandler, {msg: `could not create resource with id ${id}`})
        })
    })
}

eventProducer.prototype.createResourceThen = function(req, errorHandler, resourceID, resource, eventTopic, changeEvent, callback) {
  this.beginTransaction(errorHandler, (client, release) => {
    changeEvent.time = new Date().toISOString()
    resource.creator = lib.getUserFromReq(req)
    resource.created = changeEvent.time
    this.insertAuditEvent(errorHandler, client, release, eventTopic, changeEvent, (eventRecord) => {
      let eventSequenceNumber = eventRecord.index
      // The following line is a critical line. It ensures that the resource stored on disk includes the
      // event index of the corresponding audit event. This allows clients to know reliably whether
      // one version of the resource is more or less recent than any other version.
      resource.eventSequenceNumber = changeEvent.sequenceNumber
      resource.etag = `e${(++eventSequenceNumber).toString()}`
      this.insrtResource(errorHandler, client, release, resourceID, resource, resource.etag, () => {
        this.commitTransaction(client, release, (err) => {
          this.tellConsumers(req, eventRecord, function(){
            callback(eventRecord)
          })
        })
      })
    })
  })
}

eventProducer.prototype.deleteRecord = function(errorHandler, client, release, id, callback) {
  var query = `DELETE FROM "${this.tableName}" WHERE "${this.idColumnName}" = $1 RETURNING *`
  this.executeInTransaction(errorHandler, client, release, query, [id], function (pgResult) {
    if (pgResult.rowCount == 1)
      callback(pgResult.rows[0])
    else {
      client.query('ROLLBACK', release)
      rLib.notFound(errorHandler, {msg: `resource with id ${id} does not exist`})
    }     
  })
}

eventProducer.prototype.deleteResourceThen = function(req, errorHandler, id, eventTopic, changeEvent, callback) {
  this.beginTransaction(errorHandler, (client, release) => {
    this.deleteRecord(errorHandler, client, release, id, (resourceRecord) => {
      changeEvent.resource = resourceRecord.data
      this.insertAuditEvent(errorHandler, client, release, eventTopic, changeEvent, (eventRecord) => {
        changeEvent.resource.eventSequenceNumber = changeEvent.sequenceNumber
        this.commitTransaction(client, release, (err) => {
          this.tellConsumers(req, eventRecord, function(){
            callback(resourceRecord, eventRecord)
          })
        })
      })
    })
  })
}

eventProducer.prototype.queryAndStoreEvent = function(req, query, queryArgs, eventTopic, eventData, callback) {
  // We use a transaction here, since its PG and we can. In fact it would be OK to create the event record first and then do the update.
  // If the update failed we would have created an unnecessary event record, which is not ideal, but probably harmless.
  // The converse—creating an update without an event record—could be harmful.
  var pool = this.pool
  var self = this
  pool.connect(function(err, client, release) {
    if (err)
      callback(err)
    else
      client.query('BEGIN', function(err) {
        if(err) {
          client.query('ROLLBACK', release)
          callback(err)
        } else
          client.query(query, queryArgs, function(err, pgResult) {
            if(err) {
              client.query('ROLLBACK', (rollback_err) => {
                if (err) {
                  console.log(`component: ${process.env.COMPONENT_NAME}, error on transaction COMMIT: ${JSON.stringify(err)}`)
                  release(err)
                  callback(err)
                } else {
                  release()
                  callback('no event created')
                }
              })
              if (err.code == 23505)
                callback(409)
              else { 
                console.log(err)
                callback(err)
              }
            } else {
              var time = Date.now()
              var event = eventData(pgResult)
              if (event) {
                var equery = `INSERT INTO events (topic, eventtime, data) 
                              values ($1, $2, $3)
                              RETURNING *`
                client.query(equery, [eventTopic, time, JSON.stringify(event)], function(err, pgEventResult) {
                  if(err) {
                    client.query('ROLLBACK', (rollbackEerror) => {
                      if (rollbackEerror) {
                        console.log(`component: ${process.env.COMPONENT_NAME}, error on transaction ROLLBACK: ${JSON.stringify(err)}`)
                        release(rollbackEerror)
                        callback(err)
                      } else {
                        release()
                        callback(err)
                      }
                    })
                  } else
                    client.query('COMMIT', (err) => {
                      if (err) {
                        console.log(`component: ${process.env.COMPONENT_NAME}, error on transaction COMMIT: ${JSON.stringify(err)}`)
                        release(err)
                        callback(err)
                      } else {
                        release()
                        self.tellConsumers(req, pgEventResult.rows[0], function(){
                          callback(null, pgResult, pgEventResult)
                        })
                      }
                    })
                })
              } else
                client.query('COMMIT', (err) => {
                  if (err) {
                    console.log(`component: ${process.env.COMPONENT_NAME}, error on transaction COMMIT: ${JSON.stringify(err)}`)
                    release(err)
                    callback(err)
                  } else {
                    release()
                    self.tellConsumers(req, pgEventResult.rows[0], function(){
                      callback(null, pgResult)
                    })
                  }
                })
            }
          })
      })
  })
}

exports.eventProducer=eventProducer