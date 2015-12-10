# Description:
#   Stores the brain in Postgres
#
# Configuration:
#   DATABASE_URL
#
# Notes:
#   Run the following SQL to setup the table and column for storage.
#
#   CREATE TABLE brain (
#     key TEXT unique,
#     value JSON default '{}'::json,
#     CONSTRAINT brain_pkey PRIMARY KEY (key)
#   )
#
# Author:
#   Yannick Schutz

_        = require 'lodash'
Postgres = require 'pg'

# sets up hooks to persist the brain into postgres.
module.exports = (robot) ->

  database_url = process.env.DATABASE_URL

  throw new Error('pg-brain requires a DATABASE_URL to be set.') unless database_url?

  client = new Postgres.Client(database_url)
  client.connect()
  robot.logger.debug "pg-brain connected to #{database_url}."

  query = client.query("SELECT key, value FROM brain")
  query.on 'row', (row) ->
    data = {}
    data[row.key] = row.value
    robot.brain.mergeData data
    robot.logger.debug "pg-brain loaded. #{row.key}"

  client.on "error", (error) ->
    robot.logger.error error

  robot.brain.on 'save', (data) ->
    keys = []

    robot.logger.debug 'save'

    client.query "SELECT key FROM brain", (error, result) ->
      return robot.logger.debug(error) if error

      keys = _.pluck(result.rows, 'key')

      for key of data
        sql = if key in keys
          "UPDATE brain SET value = $2 WHERE key = $1"
        else
          "INSERT INTO brain(key, value) VALUES ($1, $2)"

        query = client.query(sql, [key, data[key]])

        robot.logger.debug "pg-brain saved. #{key}"

  robot.brain.on 'close', ->
    client.end()
