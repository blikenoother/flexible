'use strict';

/**
 * Flexible Web-Crawler Module
 * (https://github.com/eckardto/flexible.git)
 *
 * This file is part of Flexible.
 *
 * Flexible is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Flexible is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Flexible.  If not, see <http://www.gnu.org/licenses/>.
 */

var pg = require('pg');
var crc32 =require('buffer-crc32');

module.exports = function (options) {
    if (typeof options === 'string') {
        options = {uri: options};
    } else if (options.url) {
        options.uri = options.url;
    }

    return function (crawler) {
        crawler.queue = new Queue(options);

        crawler.on('complete', function () {
            crawler.queue._client.end();
        });
    };
};

function Queue(options) {
    this._domain = options.domain || '';
    this._rate_limit = options.rate_limit || 0;
    this._get_interval = options.get_interval || 1000;
    this._max_get_attempts = options.max_get_attempts || 10;
    this._client = new pg.Client(options.uri);
    this._client.connect();
}

/**
 * Setup the database.
 */
Queue.prototype._setup = function (callback) {
    var query = 'CREATE TABLE IF NOT EXISTS rate_limit (domain varchar(250) NOT NULL, limit_in_sec integer NOT NULL ' +
        'DEFAULT 0, CONSTRAINT pk_domain PRIMARY KEY (domain)); ';
    query += 'CREATE TABLE IF NOT EXISTS crawl_time (domain varchar(250) UNIQUE NOT NULL, last_crawl_time TIMESTAMP ' +
        'NOT NULL DEFAULT CURRENT_TIMESTAMP, CONSTRAINT fk_crawl_time_domain FOREIGN KEY (domain) REFERENCES ' +
        'rate_limit(domain) MATCH SIMPLE ON UPDATE CASCADE ON DELETE CASCADE);';
    query += 'CREATE TABLE IF NOT EXISTS queue (id varchar(250) NOT NULL, url text NOT NULL,  domain varchar(250) ' +
        'NOT NULL, status smallint NOT NULL DEFAULT 0, CONSTRAINT pk_url PRIMARY KEY (id), CONSTRAINT fk_queue_domain ' +
        'FOREIGN KEY (domain) REFERENCES rate_limit(domain) MATCH SIMPLE ON UPDATE CASCADE ON DELETE CASCADE);';
    this._client.query(query, function (error) {
        callback(error);
    });
};

/**
 * Add a document to the queue.
 */
Queue.prototype.add = function (loc, callback) {
    var doc = {
        queue: this,
        url: loc
    };

    var self = this;
    var insertRateLimit = 'INSERT INTO rate_limit VALUES ($1, $2)';
    self._client.query(insertRateLimit, [self._domain, self._rate_limit], function(error, data) {
        if (error && error.code && error.code == '42P01') {
            self._setup(function (error) {
                if (error) {callback(error);}
                else {self.add(loc, callback);}
            });
        } else {
            var insertCrawlTime = 'INSERT INTO crawl_time VALUES ($1)';
            self._client.query(insertCrawlTime, [self._domain], function () {
                var insertQueue = 'INSERT INTO queue VALUES ($1, $2, $3)';
                self._client.query(insertQueue, [crc32.unsigned(doc.url), doc.url, self._domain], function () {
                    callback(null, doc);
                });
            });
        }
    });
};

/**
 * Get a document to process.
 */
Queue.prototype.get = function (callback) {
    var updateQueue = 'UPDATE queue SET status = 1 WHERE url in (SELECT url FROM queue WHERE status = 0 AND domain in' +
        ' (SELECT ct.domain from crawl_time ct, rate_limit rl WHERE ct.domain = rl.domain AND ' +
        '(ROUND(EXTRACT(EPOCH FROM current_timestamp - ct.last_crawl_time))) > rl.limit_in_sec) limit 1) RETURNING url';
    var attempts = 0, self = this;
    (function get() {
        self._client.query(updateQueue, function (error, results) {

            if (error && error.code && error.code == '42P01') {
                self._setup(callback);
            } else if (results && results.rows && !results.rows[0]) {
                if (attempts < self._max_get_attempts) {
                    ++attempts;
                    setTimeout(get, self._get_interval);
                } else {callback(null, null);}
            } else {
                var _data = null;
                try { _data = {url : results.rows[0].url}; }
                catch(er) {}

                var updateRateLimit = 'UPDATE crawl_time SET last_crawl_time = CURRENT_TIMESTAMP WHERE domain = $1';
                self._client.query(updateRateLimit, [self._domain], function (error) {
                    callback(null, _data);
                });
            }
        });
    })();
};

/**
 * End processing of a document.
 */
Queue.prototype.end = function (doc, callback) {
    var updateQueue = 'UPDATE queue SET status = 3 WHERE url = $1';
    this._client.query(updateQueue, [doc.url], function (error) {
        callback(error, doc);
    });
};