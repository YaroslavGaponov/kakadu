/*
 kakadu - search index engine
 copyright (c) 2014 Yaroslav Gaponov <yaroslav.gaponov@gmail.com>
*/
(function () {

        var ROOT = {};


        var Settings = ROOT.Settings = {

            DEFAULT_TERM_OPERATION: 'AND',

            DEFAULT_OFFSET: 0,
            DEFAULT_LIMIT: 10,

            DEFAULT_HIGHLIGHT_TAGS: ['<b>', '</b>'],

            DEFAULT_DOCUMENT_BOOST: 0.5,
            DEFAULT_FIELD_BOOST: 0.1,

            DEFAULT_PREFIX_DOC: 'DOC#',
            DEFAULT_PREFIX_TERM: 'TERM#',
            DEFAULT_PREFIX_TOKEN: 'TOKEN#',
            DEFAULT_PREFIX_BOOST: 'BOOST#',
            DEFAULT_KEY_DELIMITER: ':'

        }

        var UTIL = {

            isEmpty: function (o) {
                var bool = !!o;
                if (bool) {
                    if (typeof o === 'object') {
                        if (Array.isArray(o)) {
                            bool = o.length === 0;
                        } else {
                            bool = Object.keys(o).length === 0;
                        }
                    }
                }
                return bool;
            },

            inherit: function (child, parent) {
                var o = function () {};
                o.prototype = new parent();
                child.prototype = new o();
            },

            invoke: function (callback) {
                if (callback && typeof callback === "function") {
                    var args = Array.prototype.slice.call(arguments);
                    args.shift();
                    callback.apply(null, args);
                }
            },

            stopper: function (counter) {
                return function (done) {
                    return function () {
                        if (--counter === 0) {
			  if (done && typeof done === 'function') {
			    done.apply(null, arguments);
			  }
                        }
                    }
                }
            },

            insert: function (str, text, index) {
                if (str.length < index) {
                    return str + text;
                } else if (index < 0) {
                    return text + str;
                } else {
                    return str.substr(0, index) + text + str.substr(index);
                }
            }
        }

        var WAY = {

            parallel: function (handlers, done) {
                var end = UTIL.stopper(handlers.length);
                handlers.forEach(function (handler) {
                    handler(end(done));
                });
            },

            chain: function (handlers, done) {
                var indx = 0;
                var next = function () {
                    if (indx < handlers.length) {
                        handlers[indx++](next);
                    } else {
                        UTIL.invoke(done);
                    }
                }
                next();
            },

            packet: function (handler, args, done) {
                var end = UTIL.stopper(args.length);
                args.forEach(function (arg) {
                    if (!Array.isArray(arg)) {
                        arg = [arg];
                    }
                    arg.push(end(done));
                    handler.apply(null, arg);
                })
            },

            pipe: function (handlers) {
                return function (argument, iterator, done) {
                    var counter = 0;

                    var _next = function (argument, idx) {

                        idx = idx || 0;

                        var _iterator = function (argument) {
                            if (idx < handlers.length - 1) {
                                _next(argument, idx + 1);
                            } else {
                                iterator(argument);
                            }
                        }

                        var _done = function () {
                            if (--counter === 0) {
                                UTIL.invoke(done);
                            }
                        }

                        counter++;
                        handlers[idx](argument, _iterator, _done);
                    }

                    _next(argument);
                }
            }
        }


        var LogLevel = ROOT.LogLevel = {
            INFO: 1 << 0,
            DEBUG: 1 << 2,
            TRACE: 1 << 3,
            WARN: 1 << 4,
            ERROR: 1 << 5,	  
            _level: 0,
            setLevel: function (level) {
                return LogLevel._level = level;
            },
            getLevel: function () {
                return LogLevel._level;
            },
            getName: function (level) {
                switch (level) {
                case LogLevel.INFO:
                    return 'INFO';
                case LogLevel.DEBUG:
                    return 'DEBUG';
                case LogLevel.TRACE:
                    return 'TRACE';
                case LogLevel.WARN:
                    return 'WARN';
                case LogLevel.ERROR:
                    return 'ERROR';
                }
            }
        }

        var Logger = ROOT.Logger = function () {
            if (!(this instanceof Logger)) {
                return new Logger();
            }
        }

        Logger.prototype.print = function (level, message) {
            if ((LogLevel.getLevel() & level) == level) {
                console.log(Date() + '\t' + LogLevel.getName(level) + ':\t' + message);
            }
        }

        Logger.prototype.info = function (message) {
            this.print(LogLevel.INFO, message);
        }

        Logger.prototype.debug = function (message) {
            this.print(LogLevel.DEBUG, message);
        }

        Logger.prototype.trace = function (message) {
            this.print(LogLevel.TRACE, message);
        }

        Logger.prototype.warn = function (message) {
            this.print(LogLevel.WARN, message);
        }

        Logger.prototype.error = function (message) {
            this.print(LogLevel.ERROR, message);
        }

        /*
        @private
        @description bitset stucture implementation
        */
        var BitSet = function (data) {
            this.data = data || {};
        }

        BitSet.prototype.add = function (id) {
            var idx = id >>> 5;
            var bit = 1 << (id & 0x1f);
            if (!this.data[idx]) {
                this.data[idx] = bit;
            } else {
                this.data[idx] |= bit;
            }
        }

        BitSet.prototype.remove = function (id) {
            var idx = id >>> 5;
            var bit = 1 << (id & 0x1f);
            if (this.data[idx]) {
                this.data[idx] &= ~bit;
            }
            if (!this.data[idx]) {
                delete this.data[idx];
            }
        }

        BitSet.prototype.length = function () {
            var _bits = function (n) {
                var table = [0, 1, 1, 2, 1, 2, 2, 3, 1, 2, 2, 3, 2, 3, 3, 4, 1, 2, 2, 3, 2, 3, 3, 4, 2, 3, 3, 4, 3, 4, 4, 5, 1, 2, 2, 3, 2, 3, 3, 4, 2, 3, 3, 4, 3, 4, 4, 5, 2, 3, 3, 4, 3, 4, 4, 5, 3, 4, 4, 5, 4, 5, 5, 6, 1, 2, 2, 3, 2, 3, 3, 4, 2, 3, 3, 4, 3, 4, 4, 5, 2, 3, 3, 4, 3, 4, 4, 5, 3, 4, 4, 5, 4, 5, 5, 6, 2, 3, 3, 4, 3, 4, 4, 5, 3, 4, 4, 5, 4, 5, 5, 6, 3, 4, 4, 5, 4, 5, 5, 6, 4, 5, 5, 6, 5, 6, 6, 7, 1, 2, 2, 3, 2, 3, 3, 4, 2, 3, 3, 4, 3, 4, 4, 5, 2, 3, 3, 4, 3, 4, 4, 5, 3, 4, 4, 5, 4, 5, 5, 6, 2, 3, 3, 4, 3, 4, 4, 5, 3, 4, 4, 5, 4, 5, 5, 6, 3, 4, 4, 5, 4, 5, 5, 6, 4, 5, 5, 6, 5, 6, 6, 7, 2, 3, 3, 4, 3, 4, 4, 5, 3, 4, 4, 5, 4, 5, 5, 6, 3, 4, 4, 5, 4, 5, 5, 6, 4, 5, 5, 6, 5, 6, 6, 7, 3, 4, 4, 5, 4, 5, 5, 6, 4, 5, 5, 6, 5, 6, 6, 7, 4, 5, 5, 6, 5, 6, 6, 7, 5, 6, 6, 7, 6, 7, 7, 8];
                return table[n & 0xff] + table[(n >> 8) & 0xff] + table[(n >> 16) & 0xff] + table[(n >> 24) & 0xff];
            }

            var length = 0;
            for (var idx in this.data) {
                length += _bits(this.data[idx]);
            }
            return length;

        }

        BitSet.prototype.forEach = function (iterator) {
            for (var idx in this.data) {
                for (var n = 0; n < 32; n++) {
                    var bit = 1 << n;
                    if ((this.data[idx] & bit) == bit) {
                        iterator((idx << 5) | n);
                    }
                }
            }
        }

        BitSet.prototype.OR = function (bitSet) {
            for (var idx in bitSet.data) {
                if (this.data[idx]) {
                    this.data[idx] |= bitSet.data[idx];
                } else {
                    this.data[idx] = bitSet.data[idx];
                }
            }
            return this;
        }

        BitSet.prototype.AND = function (bitSet) {
            for (var idx in bitSet.data) {
                if (this.data[idx]) {
                    this.data[idx] &= bitSet.data[idx];
                }
                if (!this.data[idx]) {
                    delete this.data[idx];
                }
            }
            return this;
        }

        BitSet.prototype.NOT = function (bitSet) {
            for (var idx in bitSet.data) {
                if (this.data[idx]) {
                    this.data[idx] &= ~bitSet.data[idx];
                }
                if (!this.data[idx]) {
                    delete this.data[idx];
                }
            }
            return this;
        }


        BitSet.prototype.toArray = function () {
            var array = [];
            for (var idx in this.data) {
                array.push(+idx);
                array.push(+this.data[idx]);
            }
            return array;
        }

        BitSet.prototype.fromArray = function (array) {
            for (var i = 0; i < array.length; i += 2) {
                this.data[array[i]] = array[i + 1];
            }
        }


        /*
        @public
        @description storage interface
        */
        var KeyValueStorage = ROOT.KeyValueStorage = function () {}

        KeyValueStorage.prototype.open = function (done) {
            UTIL.invoke(done, new Error('Not implemented.'));
        }

        KeyValueStorage.prototype.close = function (done) {
            UTIL.invoke(done, new Error('Not implemented.'));
        }

        KeyValueStorage.prototype.set = function (key, value, done) {
            UTIL.invoke(done, new Error('Not implemented.'));
        }

        KeyValueStorage.prototype.get = function (key, done) {
            UTIL.invoke(done, new Error('Not implemented.'));
        }

        KeyValueStorage.prototype.remove = function (key, done) {
            UTIL.invoke(done, new Error('Not implemented.'));
        }

        KeyValueStorage.prototype.clear = function (done) {
            UTIL.invoke(done, new Error('Not implemented.'));
        }

        KeyValueStorage.prototype.forEach = function (filter, iterator, done) {
            done();
        }

        /*
        @public
        @descrition key-value storage in memory implementation
        */
        var MemoryKeyValue = ROOT.MemoryKeyValue = function (asych) {
            this.asych = !!asych;
            this.data = {};
        }

        UTIL.inherit(MemoryKeyValue, KeyValueStorage);

        MemoryKeyValue.open = function (done) {
            var _open = function () {
                var err = null;
                UTIL.invoke(done, err);
            }
            if (this.asych) {
                setTimeout(_open, 0);
            } else {
                _open();
            }
        }

        MemoryKeyValue.close = function (done) {
            var _close = function () {
                var err = null;
                UTIL.invoke(done, err);
            }
            if (this.asych) {
                setTimeout(_close, 0);
            } else {
                _close();
            }
        }

        MemoryKeyValue.prototype.clear = function (done) {
            var self = this;
            var _clear = function () {
                var err = null;
                self.data = {};
                UTIL.invoke(done, err);
            }
            if (this.asych) {
                setTimeout(_clear, 0);
            } else {
                _clear();
            }
        }

        MemoryKeyValue.prototype.set = function (key, value, done) {
            var self = this;
            var _set = function () {
                var err = null;
                var res = {
                    key: key,
                    value: self.data[key] = value
                }
                UTIL.invoke(done, err, res);
            }
            if (this.asych) {
                setTimeout(_set, 0);
            } else {
                _set();
            }
        }

        MemoryKeyValue.prototype.get = function (key, done) {
            var self = this;
            var _get = function () {
                var res = {
                    key: key,
                    value: self.data[key]
                }
                var err = null;
                done(err, res);
            }
            if (this.asych) {
                setTimeout(_get, 0);
            } else {
                _get();
            }
        }

        MemoryKeyValue.prototype.remove = function (key, done) {
            var self = this;
            var _remove = function () {
                var err = null;
                delete self.data[key];
                UTIL.invoke(done, err);
            }
            if (this.asych) {
                setTimeout(_remove, 0);
            } else {
                _remove();
            }
        }

        MemoryKeyValue.prototype.forEach = function (filter, iterator, done) {
            var self = this;
            var _forEach = function () {
                for (var key in self.data) {
                    if (filter(key)) {
                        iterator(key, self.data[key]);
                    }
                }
                done();
            }
            if (this.asych) {
                setTimeout(_forEach, 0);
            } else {
                _forEach();
            }
        }

        /*
        @description get unique id atomic
        */
        MemoryKeyValue.prototype.getID = function (done) {
            var self = this;
            var id = parseInt(self.data['LASTID']);
            if (isNaN(id)) {
                id = 0;
            } else {
                id++;
            }
            self.data['LASTID'] = id;
            done(id);
        }

        /*
        @private
        @description storage for documents
        */
        var Documents = function (kv, done) {
            this.kv = kv;
            UTIL.invoke(done);
        }

        Documents.prototype.save = function (doc, done) {
            var self = this;
            self.kv.getID(function (docID) {
                self.kv.set(Settings.DEFAULT_PREFIX_DOC + docID, JSON.stringify(doc), function (err) {
                    return done(err, docID);
                });
            });
        }

        Documents.prototype.load = function (docID, done) {
            var self = this;
            self.kv.get(Settings.DEFAULT_PREFIX_DOC + docID, function (err, res) {
                done(err, JSON.parse(res.value));
            });
        }

        Documents.prototype.remove = function (docID, done) {
            this.kv.remove(Settings.DEFAULT_PREFIX_DOC + docID, function (err) {
                done(err, docID);
            });
        }


        var RIndex = function (kv, done) {
            this.kv = kv;
            UTIL.invoke(done);
        }

        RIndex.prototype.add = function (term, docID, done) {
            var self = this;

            var _addTerm = function (key) {
                return function (done) {
                    self.kv.get(key, function (err, res) {
                        var bs = new BitSet();
                        if (res && res.value) {
                            bs.fromArray(JSON.parse(res.value));
                        }
                        bs.add(docID);
                        self.kv.set(key, JSON.stringify(bs.toArray()), done);
                    });
                }
            }

            var _addToken = function (key) {
                return function (done) {
                    self.kv.set(key, term.getToken().toString(), done);
                }
            }

            WAY.parallel(
                [
                    _addTerm(Settings.DEFAULT_PREFIX_TERM),
                    _addTerm(Settings.DEFAULT_PREFIX_TERM + term.getFieldName()),
                    _addTerm(Settings.DEFAULT_PREFIX_TERM + term.toString()),
                    _addToken(Settings.DEFAULT_PREFIX_TOKEN + docID + Settings.DEFAULT_KEY_DELIMITER + term.toString())
                ],
                done
            );
        }


        RIndex.prototype.get = function (key, done) {
	    var self = this;
            self.kv.get(Settings.DEFAULT_PREFIX_TERM + key.toString(), function (err, res) {
                if (err) {
                    done(err);
                } else {
                    var array = JSON.parse(res.value);
                    var bs = new BitSet();
                    bs.fromArray(array)
                    done(null, bs);
                }
            });
        }


        RIndex.prototype.getToken = function (docID, fieldName, token, done) {
            var self = this;
            self.kv.get(Settings.DEFAULT_PREFIX_TOKEN + docID + Settings.DEFAULT_KEY_DELIMITER + fieldName + Settings.DEFAULT_KEY_DELIMITER + token, function (err, res) {
                if (err || !res || !res.value) {
                    done(err);
                } else {
                    var o = JSON.parse(res.value);
                    var token = new Token();
                    token.fromJSON(o)
                    done(null, token);
                }
            });
        }


        RIndex.prototype.remove = function (term, docID, done) {
            var self = this;

            var _removeTerm = function (key) {
                return function (done) {
                    self.kv.get(key, function (err, res) {
                        var bs = new BitSet();
                        if (res && res.value) {
                            bs.fromArray(JSON.parse(res.value));
                            bs.remove(docID);
                        }
                        self.kv.set(key, JSON.stringify(bs.toArray()), done);
                    });
                }
            }

            var _removeToken = function (key) {
                return function (done) {
                    self.kv.remove(key, done);
                }
            }


            WAY.parallel(
                [
                    _removeTerm(Settings.DEFAULT_PREFIX_TERM),
                    _removeTerm(Settings.DEFAULT_PREFIX_TERM + term.getFieldName()),
                    _removeTerm(Settings.DEFAULT_PREFIX_TERM + term.toString()),
                    _removeToken(Settings.DEFAULT_PREFIX_TOKEN + docID + Settings.DEFAULT_KEY_DELIMITER + term.toString())
                ],
                done
            );
        }

        RIndex.prototype.removeALL = function (docID, done) {
            var self = this;

            var _filter = function (key) {
                return (key.indexOf(Settings.DEFAULT_PREFIX_TERM) === 0);
            }

            var _iterator = function (key, value) {
                self.kv.get(key, function (err, res) {
                    var bs = new BitSet();
                    if (res && res.value) {
                        bs.fromArray(JSON.parse(res.value));
                        bs.remove(docID);
                        self.kv.set(key, JSON.stringify(bs.toArray()));
                    }
                });
            };

            var _done = function () {
                UTIL.invoke(done);
            }

            this.kv.forEach(_filter, _iterator, _done);
        }

        RIndex.prototype.forEach = function (filter, iterator, done) {
            var self = this;

            var _filter = function (key) {
                return (key.indexOf(Settings.DEFAULT_PREFIX_TERM) === 0) && filter(key);
            }

            var _iterator = function (key, data) {
                var bs = new BitSet();
                if (data) {
                    bs.fromArray(JSON.parse(data));
                }
                iterator(key, bs);
            }

            this.kv.forEach(_filter, _iterator, done);
        }

        RIndex.prototype.ALL = function (done) {
            var self = this;
            self.kv.get(Settings.DEFAULT_PREFIX_TERM, function (err, res) {
                var acc = new BitSet();
                if (!err && res) {
                    acc.fromArray(JSON.parse(res.value));
                }
                done(err, acc);
            });
        }

        RIndex.prototype.NOT = function (bs, done) {
            var self = this;
            self.ALL(function (err, acc) {
                acc.NOT(bs)
                done(err, acc);
            });
        }

        RIndex.prototype.setDocumentBoost = function (docID, boost, done) {
            this.kv.set(Settings.DEFAULT_PREFIX_BOOST + docID, JSON.stringify(boost), done);
        }

        RIndex.prototype.getDocumentBoost = function (docID, done) {
            this.kv.get(Settings.DEFAULT_PREFIX_BOOST + docID, function (err, res) {
                if (err) {
                    done(err);
                } else {
                    done(null, parseFloat(res.value));
                }
            });
        }


        var Token = function (text, start, stop) {
            this.text = text ? text.toString() : '';
            this.start = start || 0;
            this.stop = stop || (this.start + this.text.length);
        }


        Token.prototype.getText = function () {
            return this.text;
        }

        Token.prototype.getStart = function () {
            return this.start;
        }

        Token.prototype.getStop = function () {
            return this.stop;
        }

        Token.prototype.toJSON = function () {
            var json = {};
            json.text = this.getText();
            json.start = this.getStart();
            json.stop = this.getStop();
            return json;
        }

        Token.prototype.fromJSON = function (json) {
            this.text = json.text;
            this.start = json.start;
            this.stop = json.stop;
        }

        Token.prototype.toString = function () {
            return JSON.stringify(this.toJSON());
        }

        var Term = function (fieldName, token) {
            this.fieldName = fieldName;
            this.token = token;
        }


        Term.prototype.getFieldName = function () {
            return this.fieldName;
        }

        Term.prototype.getToken = function () {
            return this.token;
        }

        Term.prototype.toString = function () {
            return this.getFieldName() + Settings.DEFAULT_KEY_DELIMITER + this.getToken().getText();
        }

        var SkipFilter = function () {
            return function (token, iterator, done) {
                done();
            }
        }

        var KeywordTokenizer = ROOT.KeywordTokenizer = function () {
            return function (token, iterator, done) {
                if (token) {
                    iterator(token);
                }
                done();
            }
        }

        var NumberFilter = ROOT.NumberFilter = function () {
            return function (token, iterator, done) {
                if (token) {
                    var number = new Number(token.getText())
                    iterator(new Token(number.toString(), token.getStart(), token.getStop()));
                }
                done();
            }
        }



        var BooleanFilter = ROOT.BooleanFilter = function () {
            return function (token, iterator, done) {
                if (token) {
                    var bool = token.getText() === 'true';
                    iterator(new Token((bool.toString()).toString(), token.getStart(), token.getStop()));
                }
                done();
            }
        }

        var DateFilter = ROOT.DateFilter = function () {
            return function (token, iterator, done) {
                if (token) {
                    var dt = Date.parse(token.getText());
                    if (dt) {
                        iterator(new Token(dt.toString(), token.getStart(), token.getStop()));
                    }
                }
                done();
            }
        }


        var StandardTokenizerDelimeter = ROOT.StandardTokenizerDelimeter = function (delimeters) {
            delimeters =
                delimeters ?
                delimeters : [' ', '\t', '.', ',', '!', '?', '(', ')', '{', '}'];
            return function (token, iterator, done) {
                if (token) {
                    text = token.getText();
                    for (var start = stop = i = 0; i < text.length; i++) {
                        if (delimeters.indexOf(text[i]) === -1) {
                            stop++;
                        } else {
                            if (start < stop) {
                                iterator(new Token(text.substring(start, stop), token.getStart() + start, token.getStart() + stop));
                            }
                            start = ++stop;
                        }
                    }
                    if (start < stop) {
                        iterator(new Token(text.substring(start, stop), token.getStart() + start, token.getStart() + stop));
                    }
                }
                done();
            }
        }

        var StandardTokenizerAlphabet = ROOT.StandardTokenizerAlphabet = function (alphabet) {
            alphabet =
                alphabet ?
                alphabet : /[a-z]+/i;
            return function (token, iterator, done) {
                if (token) {
                    text = token.getText();
                    for (var start = stop = i = 0; i < text.length; i++) {
                        if (alphabet.test(text[i])) {
                            stop++;
                        } else {
                            if (start < stop) {
                                iterator(new Token(text.substring(start, stop), token.getStart() + start, token.getStart() + stop));
                            }
                            start = ++stop;
                        }
                    }
                    if (start < stop) {
                        iterator(new Token(text.substring(start, stop), token.getStart() + start, token.getStart() + stop));
                    }
                }
                done();
            }
        }

        var ToLowerCaseFilter = ROOT.ToLowerCaseFilter = function () {
            return function (token, iterator, done) {
                if (token) {
                    iterator(new Token(token.getText().toLowerCase(), token.getStart(), token.getStop()));
                }
                done();
            }
        }


        var StopWordsFilter = ROOT.StopWordsFilter = function (stopWords) {
            stopWords = stopWords || ['a', 'able', 'about', 'above', 'abroad', 'according', 'accordingly', 'across', 'actually', 'adj', 'after', 'afterwards', 'again', 'against', 'ago', 'ahead', 'aint', 'all', 'allow', 'allows', 'almost', 'alone', 'along', 'alongside', 'already', 'also', 'although', 'always', 'am', 'amid', 'amidst', 'among', 'amongst', 'an', 'and', 'another', 'any', 'anybody', 'anyhow', 'anyone', 'anything', 'anyway', 'anyways', 'anywhere', 'apart', 'appear', 'appreciate', 'appropriate', 'are', 'arent', 'around', 'as', 'as', 'aside', 'ask', 'asking', 'associated', 'at', 'available', 'away', 'awfully', 'b', 'back', 'backward', 'backwards', 'be', 'became', 'because', 'become', 'becomes', 'becoming', 'been', 'before', 'beforehand', 'begin', 'behind', 'being', 'believe', 'below', 'beside', 'besides', 'best', 'better', 'between', 'beyond', 'both', 'brief', 'but', 'by', 'c', 'came', 'can', 'cannot', 'cant', 'cant', 'caption', 'cause', 'causes', 'certain', 'certainly', 'changes', 'clearly', 'cmon', 'co', 'co.', 'com', 'come', 'comes', 'concerning', 'consequently', 'consider', 'considering', 'contain', 'containing', 'contains', 'corresponding', 'could', 'couldnt', 'course', 'cs', 'currently', 'd', 'dare', 'darent', 'definitely', 'described', 'despite', 'did', 'didnt', 'different', 'directly', 'do', 'does', 'doesnt', 'doing', 'done', 'dont', 'down', 'downwards', 'during', 'e', 'each', 'edu', 'eg', 'eight', 'eighty', 'either', 'else', 'elsewhere', 'end', 'ending', 'enough', 'entirely', 'especially', 'et', 'etc', 'even', 'ever', 'evermore', 'every', 'everybody', 'everyone', 'everything', 'everywhere', 'ex', 'exactly', 'example', 'except', 'f', 'fairly', 'far', 'farther', 'few', 'fewer', 'fifth', 'first', 'five', 'followed', 'following', 'follows', 'for', 'forever', 'former', 'formerly', 'forth', 'forward', 'found', 'four', 'from', 'further', 'furthermore', 'g', 'get', 'gets', 'getting', 'given', 'gives', 'go', 'goes', 'going', 'gone', 'got', 'gotten', 'greetings', 'h', 'had', 'hadnt', 'half', 'happens', 'hardly', 'has', 'hasnt', 'have', 'havent', 'having', 'he', 'hed', 'hell', 'hello', 'help', 'hence', 'her', 'here', 'hereafter', 'hereby', 'herein', 'heres', 'hereupon', 'hers', 'herself', 'hes', 'hi', 'him', 'himself', 'his', 'hither', 'hopefully', 'how', 'howbeit', 'however', 'hundred', 'i', 'id', 'ie', 'if', 'ignored', 'ill', 'im', 'immediate', 'in', 'inasmuch', 'inc', 'inc.', 'indeed', 'indicate', 'indicated', 'indicates', 'inner', 'inside', 'insofar', 'instead', 'into', 'inward', 'is', 'isnt', 'it', 'itd', 'itll', 'its', 'its', 'itself', 'ive', 'j', 'just', 'k', 'keep', 'keeps', 'kept', 'know', 'known', 'knows', 'l', 'last', 'lately', 'later', 'latter', 'latterly', 'least', 'less', 'lest', 'let', 'lets', 'like', 'liked', 'likely', 'likewise', 'little', 'look', 'looking', 'looks', 'low', 'lower', 'ltd', 'm', 'made', 'mainly', 'make', 'makes', 'many', 'may', 'maybe', 'maynt', 'me', 'mean', 'meantime', 'meanwhile', 'merely', 'might', 'mightnt', 'mine', 'minus', 'miss', 'more', 'moreover', 'most', 'mostly', 'mr', 'mrs', 'much', 'must', 'mustnt', 'my', 'myself', 'n', 'name', 'namely', 'nd', 'near', 'nearly', 'necessary', 'need', 'neednt', 'needs', 'neither', 'never', 'neverf', 'neverless', 'nevertheless', 'new', 'next', 'nine', 'ninety', 'no', 'nobody', 'non', 'none', 'nonetheless', 'noone', 'no-one', 'nor', 'normally', 'not', 'nothing', 'notwithstanding', 'novel', 'now', 'nowhere', 'o', 'obviously', 'of', 'off', 'often', 'oh', 'ok', 'okay', 'old', 'on', 'once', 'one', 'ones', 'ones', 'only', 'onto', 'opposite', 'or', 'other', 'others', 'otherwise', 'ought', 'oughtnt', 'our', 'ours', 'ourselves', 'out', 'outside', 'over', 'overall', 'own', 'p', 'particular', 'particularly', 'past', 'per', 'perhaps', 'placed', 'please', 'plus', 'possible', 'presumably', 'probably', 'provided', 'provides', 'q', 'que', 'quite', 'qv', 'r', 'rather', 'rd', 're', 'really', 'reasonably', 'recent', 'recently', 'regarding', 'regardless', 'regards', 'relatively', 'respectively', 'right', 'round', 's', 'said', 'same', 'saw', 'say', 'saying', 'says', 'second', 'secondly', 'see', 'seeing', 'seem', 'seemed', 'seeming', 'seems', 'seen', 'self', 'selves', 'sensible', 'sent', 'serious', 'seriously', 'seven', 'several', 'shall', 'shant', 'she', 'shed', 'shell', 'shes', 'should', 'shouldnt', 'since', 'six', 'so', 'some', 'somebody', 'someday', 'somehow', 'someone', 'something', 'sometime', 'sometimes', 'somewhat', 'somewhere', 'soon', 'sorry', 'specified', 'specify', 'specifying', 'still', 'sub', 'such', 'sup', 'sure', 't', 'take', 'taken', 'taking', 'tell', 'tends', 'th', 'than', 'thank', 'thanks', 'thanx', 'that', 'thatll', 'thats', 'thats', 'thatve', 'the', 'their', 'theirs', 'them', 'themselves', 'then', 'thence', 'there', 'thereafter', 'thereby', 'thered', 'therefore', 'therein', 'therell', 'therere', 'theres', 'theres', 'thereupon', 'thereve', 'these', 'they', 'theyd', 'theyll', 'theyre', 'theyve', 'thing', 'things', 'think', 'third', 'thirty', 'this', 'thorough', 'thoroughly', 'those', 'though', 'three', 'through', 'throughout', 'thru', 'thus', 'till', 'to', 'together', 'too', 'took', 'toward', 'towards', 'tried', 'tries', 'truly', 'try', 'trying', 'ts', 'twice', 'two', 'u', 'un', 'under', 'underneath', 'undoing', 'unfortunately', 'unless', 'unlike', 'unlikely', 'until', 'unto', 'up', 'upon', 'upwards', 'us', 'use', 'used', 'useful', 'uses', 'using', 'usually', 'v', 'value', 'various', 'versus', 'very', 'via', 'viz', 'vs', 'w', 'want', 'wants', 'was', 'wasnt', 'way', 'we', 'wed', 'welcome', 'well', 'well', 'went', 'were', 'were', 'werent', 'weve', 'what', 'whatever', 'whatll', 'whats', 'whatve', 'when', 'whence', 'whenever', 'where', 'whereafter', 'whereas', 'whereby', 'wherein', 'wheres', 'whereupon', 'wherever', 'whether', 'which', 'whichever', 'while', 'whilst', 'whither', 'who', 'whod', 'whoever', 'whole', 'wholl', 'whom', 'whomever', 'whos', 'whose', 'why', 'will', 'willing', 'wish', 'with', 'within', 'without', 'wonder', 'wont', 'would', 'wouldnt', 'x', 'y', 'yes', 'yet', 'you', 'youd', 'youll', 'your', 'youre', 'yours', 'yourself', 'yourselves', 'youve', 'z', 'zero'];
            return function (token, iterator, done) {
                if (token) {
                    if (stopWords.indexOf(token.getText()) === -1) {
                        iterator(token);
                    }
                }
                done();
            }
        }


        var LengthFilter = ROOT.LengthFilter = function (minLength) {
            minLength = minLength || 0;
            return function (token, iterator, done) {
                if (token) {
                    if (token.getText().length >= minLength) {
                        iterator(token);
                    }
                }
                done();
            }
        }

        var SuggestFilter = ROOT.SuggestFilter = function (minLength, maxLength) {
            return function (token, iterator, done) {
                if (token) {
                    var minLenght = minLength || 2;
                    var maxLength = Math.min(maxLength || token.getText().length, token.getText().length);
                    for (var i = minLenght; i <= maxLength; i++) {
                        iterator(new Token(token.getText().substr(0, i), token.getStart(), token.getStop()));
                    }
                }
                done();
            }
        }

        var SoundExFilter = ROOT.SoundExFilter = function (length) {
            var length = length || 4;
            var getSoundExCode = function (ch) {
                switch (ch) {
                case 'b':
                case 'f':
                case 'p':
                case 'v':
                    return '1';
                case 'c':
                case 'g':
                case 'j':
                case 'k':
                case 'q':
                case 's':
                case 'x':
                case 'z':
                    return '2';
                case 'd':
                case 't':
                    return '3';
                case 'l':
                    return '4';
                case 'm':
                case 'n':
                    return '5';
                case 'r':
                    return '6';
                }
                return null;
            };
            return function (token, iterator, done) {
                if (token) {
                    var str = token.getText().toLowerCase();
                    if (str.length > 0) {
                        var buf = [str[0]];
                        var pred = getSoundExCode(str[0]);
                        for (var i = 1; i < str.length; i++) {
                            var curr = getSoundExCode(str[i]);
                            if (curr) {
                                if (pred != curr) {
                                    buf.push(curr);
                                    pred = curr;
                                    if (buf.length >= length) {
                                        break;
                                    }
                                }
                            }
                        }
                        while (buf.length < length) {
                            buf.push('0');
                        }
                        iterator(new Token(buf.join(''), token.getStart(), token.getStop()));
                    }
                }
                done();
            }
        }

        var Type = ROOT.Type = {
            AUTO: [],
            SKIP: [SkipFilter()],
            KEYWORD: [KeywordTokenizer()],
            TEXT: [StandardTokenizerAlphabet(), LengthFilter(), ToLowerCaseFilter(), StopWordsFilter()],
            NUMBER: [NumberFilter()],
            BOOLEAN: [BooleanFilter()],
            DATE: [DateFilter()],
            SUGGEST: [KeywordTokenizer(), ToLowerCaseFilter(), SuggestFilter()],
            SOUNDEX: [StandardTokenizerAlphabet(), LengthFilter(), ToLowerCaseFilter(), StopWordsFilter(), SoundExFilter()],

            autoTypeDetect: function (data) {
                if (typeof data === "object") {
                    if (!isNaN(Date.parse(data))) {
                        return Type.DATE;
                    } else if (Array.isArray(data)) {
                        return Type.TEXT;
                    } else {
                        return Type.TEXT;
                    }
                } else if (typeof data === "string") {
                    return Type.TEXT;
                } else if (typeof data === "number") {
                    return Type.NUMBER;
                } else if (typeof data === "boolean") {
                    return Type.BOOLEAN;
                }
                return Type.TEXT;
            }
        }


        var Field = ROOT.Field = function (name) {
            if (this instanceof Field) {
                this.name = name;
                this.type = Type.AUTO;
                this.boost = Settings.DEFAULT_FIELD_BOOST
            } else {
                return new Field(name);
            }
        }

        Field.prototype.getName = function () {
            return this.name;
        }

        Field.prototype.setType = function (type) {
            this.type = type;
            return this;
        }

        Field.prototype.getType = function () {
            return this.type;
        }

        Field.prototype.setBoost = function (boost) {
            this.boost = boost;
            return this;
        }

        Field.prototype.getBoost = function () {
            return this.boost;
        }

        var Schema = ROOT.Schema = function () {
            this.schema = {};
        }

        Schema.prototype.getFields = function () {
            return Object.keys(this.schema);
        }

        Schema.prototype.addField = function (field) {
            this.schema[field.getName()] = field;
            return this;
        }

        Schema.prototype.getField = function (fieldName) {
            return this.schema[fieldName];
        }


        var Indexer = ROOT.Indexer = function () {
            var storage, schema;
            for (var i = 0; i < arguments.length; i++) {
                if (arguments[i] instanceof Schema) {
                    schema = arguments[i];
                } else if (arguments[i] instanceof KeyValueStorage) {
                    storage = arguments[i];
                }
            }
            if (!storage) {
                throw new Error('Storage is required.');
            }
            this.schema = schema || new Schema();
            this.documents = new Documents(storage);
            this.rindex = new RIndex(storage);
        }

        Indexer.prototype.getSchema = function () {
            return this.schema;
        }



        Indexer.prototype.addDocument = function (document, done) {
            var self = this;

            var docID;

            var _saveDocument = function (next) {
                self.documents.save(document, function (err, id) {
                    if (err) {
                        return done(err, null);
                    }
                    docID = id;
                    return next();
                });
            }
            var _addTerms = function (fieldName, fieldValue) {
                return function (next) {
                    if (!self.schema.getField(fieldName)) {
                        self.schema.addField(new Field(fieldName));
                    }

                    if (UTIL.isEmpty(self.schema.getField(fieldName).getType())) {
                        self.schema.getField(fieldName).setType(Type.autoTypeDetect(fieldValue));
                    }

                    var _addTerm = function (token) {
                        self.rindex.add(new Term(fieldName, token), docID);
                    }

                    var fieldType = self.schema.getField(fieldName).getType();
                    WAY.pipe(fieldType)
                        (
                            new Token(fieldValue),
                            _addTerm,
                            next
                        )
                }
            }
            var _updateBoost = function (next) {
                self.rindex.setDocumentBoost(docID, Settings.DEFAULT_DOCUMENT_BOOST, next);
            }
            var _done = function () {
                done(null, docID);
            }

            var handlers = [];
            handlers.push(_saveDocument);
            handlers.push(_updateBoost);
            for (var fieldName in document) {
                handlers.push(_addTerms(fieldName, document[fieldName]));
            }
            handlers.push(_done);

            WAY.chain(handlers);
        }

        Indexer.prototype.removeDocument = function (docID, done) {
            var self = this;

            var _removeDocument = function (done) {
                self.documents.remove(docID, done);
            }

            var _removeRIndex = function (done) {
                self.rindex.removeALL(docID, done);
            }

            WAY.parallel([_removeDocument, _removeRIndex], done);
        }

        Indexer.prototype.setDocumentBoost = function (docID, boost, done) {
            this.rindex.setDocumentBoost(docID, boost, done);
        }

        var Pair = ROOT.Pair = function (name, value) {
            this.name = name;
            this.value = value;
        }

        Pair.prototype.getName = function () {
            return this.name;
        }

        Pair.prototype.getValue = function () {
            return this.value;
        }

        Pair.prototype.toJSON = function () {
            var json = {};
            json[this.name] = this.value;
            return json;
        }

        Pair.prototype.fromJSON = function (json) {
            if (!UTIL.isEmpty(json)) {
                this.name = Object.keys(json)[0];
                this.value = json[this.name];
            }
        }

        Pair.prototype.toString = function () {
            return JSON.stringify(this.toJSON());
        }


        var Query = ROOT.Query = function (root) {
            this._root = root;
            this.type = null;
            this.terms = [];

            this.boost = null;

            this.paging = {
                offset: Settings.DEFAULT_OFFSET,
                limit: Settings.DEFAULT_LIMIT
            };

            this.facets = [];

            this.highlight = [];
            this.highlightTags = Settings.DEFAULT_HIGHLIGHT_TAGS;

            this.suggest = null;
        }

        Query.prototype.root = function () {
            return this._root || this;
        }

        Query.Type = {
            OR: 'OR',
            AND: 'AND',
            NOT: 'NOT'
        };

        Query.prototype.setBoost = function (boost) {
            this.root().boost = boost;
            return this;
        }

        Query.prototype.setFacets = function (facets) {
            this.root().facets = facets;
            return this;
        }

        Query.prototype.getFacets = function () {
            return this.facets;
        }

        Query.prototype.setHighlightTags = function (startTag, endTag) {
            this.root().highlightTags[0] = startTag;
            this.root().highlightTags[1] = endTag;
            return this;
        }

        Query.prototype.setHighlight = function (highlight) {
            this.root().highlight = highlight;
            return this;
        }

        Query.prototype.getHighlight = function () {
            return this.root().highlight;
        }

        Query.prototype.setSuggest = function (text, fields) {
            this.root().suggest = {
                text: text,
                fields: fields
            }
            return this;
        }

        Query.prototype.getSuggest = function () {
            return this.root().suggest;
        }

        Query.prototype.term = function (name, value) {
            this.terms.push(new Pair(name, value));
            return this;
        }

        Query.prototype.setType = function (type) {
            this.type = type;
        }

        Query.prototype.OR = function () {
            if (!this.type) {
                this.type = Query.Type.OR;
                return this;
            } else {
                var query = new Query(this);
                query.setType(Query.Type.OR);
                this.terms.push(query);
                return query;
            }
        }

        Query.prototype.AND = function () {
            if (!this.type) {
                this.type = Query.Type.AND;
                return this;
            } else {
                var query = new Query(this);
                query.setType(Query.Type.AND);
                this.terms.push(query);
                return query;
            }
        }

        Query.prototype.NOT = function () {
            if (!this.type) {
                this.type = Query.Type.NOT;
                return this;
            } else {
                var query = new Query(this);
                query.setType(Query.Type.NOT);
                this.terms.push(query);
                return query;
            }
        }


        Query.prototype.END = function () {
            return this.root();
        }

        Query.prototype.setPaging = function (offset, limit) {
            this.root().paging.offset = offset;
            this.root().paging.limit = limit;
            return this;
        }

        Query.prototype.getOffset = function () {
            return this.root().paging.offset;
        }

        Query.prototype.getLimit = function () {
            return this.root().paging.limit;
        }

        Query.prototype.toJSON = function () {
            var self = this;
            var json = {};
            if (!this._root) {
                if (this.boost) {
                    json.boost = this.boost;
                }
                json.paging = this.paging;
                json.facets = this.facets;
                json.highlight = this.highlight;
                json.highlightTags = this.highlightTags
                json.suggest = this.suggest;
            }
            json[this.type] = [];
            this.terms.forEach(function (term) {
                json[self.type].push(term.toJSON());
            });
            return json;
        }

        Query.prototype.fromJSON = function (json) {
            var self = this;
            if (!this._root) {
                this.boost = json.boost;
                this.paging = json.paging;
                this.facets = json.facets;
                this.highlight = json.highlight;
                this.highlightTags = json.highlightTags;
                this.suggest = json.suggest;
            }
            this.type = json.type;
            json.type.forEach(function (term) {
                var o;
                if ('type' in term) {
                    o = new Query();
                } else {
                    o = new Pair();
                }
                o.fromJSON(term);
                self.terms.push(o);
            });
        }

        Query.prototype.toString = function () {
            return JSON.stringify(this.toJSON());
        }

        var SearchResultDocument = function (docID, source, boost, highlight) {
            this._id = docID;
	    this._boost = boost;
            this._source = source;
            this._highlight = highlight;
        }

        SearchResultDocument.prototype.getSourceField = function (fieldName) {
            return this._source[fieldName];
        }

        SearchResultDocument.prototype.addHighlight = function (fieldName, text) {
            if (!this._highlight) {
                this._highlight = {};
            }
            if (this._highlight[fieldName]) {
                if (Array.isArray(this._highlight[fieldName])) {
                    this._highlight[fieldName].push(text);
                } else {
                    this._highlight[fieldName] = [this._highlight[fieldName], text];
                }
            } else {
                this._highlight[fieldName] = text;
            }
            return this;
        }

        SearchResultDocument.prototype.getHighlight = function (fieldName) {
            return this._highlight[fieldName];
        }

        SearchResultDocument.prototype.toJSON = function () {
            var json = {};
            json._id = this._id;
	    json._boost = this._boost;
            json._source = this._source;
            if (this._highlight) {
                json._highlight = this._highlight;
            }
            return json;
        }

        SearchResultDocument.prototype.fromJSON = function (json) {
            this._id = json._id;
	    this._boost = json._boost;
            this._source = json._source;
            if (json._highlight) {
                this._highlight = json._highlight;
            }
        }

        var SearchError = function (code, message, detail) {
            this.code = code;
            this.message = message;
            this.detail = detail;
        }

        SearchError.prototype.toJSON = function () {
            return {
                code: this.code,
                message: this.message,
                detail: this.detail
            }
        }

        var SearchResult = ROOT.SearchResult = function () {

            this.errors = null;

            this.startTime = new Date();
            this.stopTime = new Date();

            this.paging = {
                offset: 0,
                limit: 0
            };

            this.total = 0;
            this.documents = [];

            this.facets = null;
            this.suggests = null;
        }

        SearchResult.prototype.get = function () {
            this.stopTime = new Date();
            return this;
        }

        SearchResult.prototype.addError = function (error) {
            if (!this.errors) {
                this.errors = [];
            }
            this.errors.push(error);
            return this;
        }

        SearchResult.prototype.addDocument = function (searchResultDocument) {
            this.documents.push(searchResultDocument);
            return this;
        }

        SearchResult.prototype.getDocuments = function () {
            return this.documents;
        }

        SearchResult.prototype.setTotal = function (total) {
            this.total = total;
            return this;
        }

        SearchResult.prototype.setOffset = function (offset) {
            this.paging.offset = offset;
            return this;
        }

        SearchResult.prototype.setLimit = function (limit) {
            this.paging.limit = limit;
            return this;
        }

        SearchResult.prototype.setFacet = function (name, token, count) {
            if (!this.facets) {
                this.facets = {};
            }
            if (!this.facets[name]) {
                this.facets[name] = {};
            }
            this.facets[name][token] = +count;
        }

        SearchResult.prototype.addSuggest = function (fieldName, text) {
            if (!this.suggests) {
                this.suggests = {};
            }
            if (!this.suggests[fieldName]) {
                this.suggests[fieldName] = [text];
            } else {
                if (this.suggests[fieldName].indexOf(text) === -1) {
                    this.suggests[fieldName].push(text);
                }
            }
        }

        SearchResult.prototype.getSuggests = function () {
            return this.suggests;
        }

        SearchResult.prototype.getFacets = function () {
            return this.facets;
        }
	
	
        SearchResult.prototype.toJSON = function () {

            var array2JSON = function (array) {
                var json = [];
                array.forEach(function (element) {
                    json.push(element.toJSON());
                })
                return json;
            }

            var json = {};

            json.took = this.stopTime - this.startTime;

            json.total = this.total;
            json.documents = array2JSON(this.documents);

            if (this.errors) {
                json.errors = array2JSON(this.errors);
            }

            if (this.facets) {
                json.facets = this.facets;
            }

            if (this.suggests) {
                json.suggests = this.suggests;
            }

            return json;
        }

        SearchResult.prototype.toString = function () {
            return JSON.stringify(this.toJSON());
        }


        var Searcher = ROOT.Searcher = function () {
            var storage, schema;
            for (var i = 0; i < arguments.length; i++) {
                if (arguments[i] instanceof Schema) {
                    schema = arguments[i];
                } else if (arguments[i] instanceof KeyValueStorage) {
                    storage = arguments[i];
                }
            }
            if (!schema) {
                throw new Error('Schema is required.');
            }
            if (!storage) {
                throw new Error('Storage is required.');
            }
            this.schema = schema;
            this.documents = new Documents(storage);
            this.rindex = new RIndex(storage);
        }


        Searcher.prototype.search = function (query, done) {
                var self = this;

                var searchResult, bitSetResult, hitTokens = {},
                    docIDs = [], sumBoost = {};

                var _init = function (next) {
                    Logger().debug('REQUEST ' + query.toString());

                    searchResult = new SearchResult()
                        .setOffset(query.getOffset())
                        .setLimit(query.getLimit());

                    next();
                }

                var _search = function (next) {

                    var _getBitSetPair = function (pair, done) {

                        var tokens = [];

                        var _collectTokens = function (token) {
                            tokens.push(token);
                        }


                        var _calcDocSet = function () {
                            var end = UTIL.stopper(tokens.length);
                            var acc;
                            tokens.forEach(function (token) {
                                self.rindex.get(new Term(pair.getName(), token),
                                    function (err, bs) {
                                        if (err) {
                                            Logger().error(err);
                                            searchResult.addError(new SearchError('ERROR', 'search', err));
                                        }
                                        if (!err && bs) {
                                            if (!hitTokens[pair.getName()]) {
                                                hitTokens[pair.getName()] = [];
                                            }
                                            hitTokens[pair.getName()].push(token.getText());
                                            if (acc) {
                                                acc[Settings.DEFAULT_TERM_OPERATION](bs);
                                            } else {
                                                acc = bs;
                                            }
                                        }
                                        end(done)(acc);
                                    });
                            });
                        }

                        var fieldType = self.schema.getField(pair.getName()).getType();

                        WAY.pipe(fieldType)(new Token(pair.getValue()), _collectTokens, _calcDocSet);
                    }

                    var _getBitSetQuery = function (query, done) {

                        if (UTIL.isEmpty(query.terms)) {
                                return self.rindex.ALL(function (err, acc) {
                                    if (err) {
                                        Logger().error(err);
                                        searchResult.addError(new SearchError('ERROR', 'search', err));
                                    }
                                    done(acc);
                                });
                            }

                            var acc, end = UTIL.stopper(query.terms.length); query.terms.forEach(function (element) {
                                if (element instanceof Query) {
                                    switch (query.type) {
                                    case Query.Type.AND:
                                    case Query.Type.OR:
                                        _getBitSetQuery(element, function (bs) {
                                            acc = acc ? acc[query.type](bs) : bs;
                                            end(done)(acc);
                                        });
                                        break;
                                    case Query.Type.NOT:
                                        _getBitSetQuery(element, function (bs) {
                                            self.rindex.NOT(bs, function (err, bs) {
                                                acc = acc ? acc[query.type](bs) : bs;
                                                end(done)(acc);
                                            })
                                        });
                                        break;
                                    }
                                } else {
                                    _getBitSetPair(element, function (bs) {
                                        acc = acc ? acc[query.type](bs) : bs;
                                        end(done)(acc);
                                    });
                                }
                            });
                        }

                        _getBitSetQuery(query, function (bs) {
                            bitSetResult = bs;
                            return next();
                        });

                    }

                    var _sorting = function (next) {
                        Logger().debug('found #' + bitSetResult.length() + ' documents');

                        var fieldBoost = {},
                            docBoost = {};

                        var params = [];
                        bitSetResult.forEach(function (docID) {
                            fieldBoost[docID] = 0;
                            self.schema.getFields().forEach(function (fieldName) {
                                if (hitTokens[fieldName]) {
                                    hitTokens[fieldName].forEach(function (token) {
                                        params.push({
                                            docID: docID,
                                            fieldName: fieldName,
                                            token: token
                                        });
                                    })
                                }
                            })
                        })

                        var _getFieldsBoost = function (param, done) {
                            self.rindex.getToken(param.docID, param.fieldName, param.token, function (err, token) {
                                if (err) {
                                    Logger().error(err);
                                    searchResult.addError(new SearchError('ERROR', 'sorting', err));
                                }
                                if (!err && token) {
                                    fieldBoost[param.docID] += self.schema.getField(param.fieldName).getBoost();				    
                                }
                                done();
                            });
                        }

                        var _getDocumentsBoost = function (next) {
                            var docIDs = Object.keys(fieldBoost);
                            var _getDocumentBoost = function (docID, done) {
                                self.rindex.getDocumentBoost(docID, function (err, boost) {				  
                                    docBoost[docID] = boost;
				    done();
                                })
                            }			    
                            WAY.packet(_getDocumentBoost, docIDs, next);
                        }

                        var _getSortedIDs = function () {
                            var boostToDocIDs = {};
                            var boosts = [];
                            for (var docID in fieldBoost) {			      
                                var boost = sumBoost[docID] = fieldBoost[docID] * docBoost[docID];
                                Logger().debug('Document #' + docID + ' summery boost is ' + boost);
                                if (isNaN(query.boost) || (boost >= query.boost)) {
                                    if (boosts.indexOf(boost) === -1) {
                                        boosts.push(boost);
                                    }
                                    if (!boostToDocIDs[boost]) {
                                        boostToDocIDs[boost] = [];
                                    }
                                    boostToDocIDs[boost].push(docID);
                                }
                            }
                            boosts.sort(function(a,b) { return a<b; }).forEach(function (boost) {
                                docIDs = docIDs.concat(boostToDocIDs[boost]);
                            });			    
                            return next();
                        }

                        WAY.packet(_getFieldsBoost, params, function() { WAY.chain([_getDocumentsBoost], _getSortedIDs) });

                    }

                    var _facets = function (next) {
                        if (UTIL.isEmpty(query.getFacets())) {
                            Logger().debug('Facet section is skipped.');
                            return next();
                        }

                        var end = UTIL.stopper(query.getFacets().length);

                        query.getFacets().forEach(function (fieldName) {

                            var _filter = function (key) {
                                return (key.indexOf(Settings.DEFAULT_PREFIX_TERM + fieldName) === 0) && (key.split(Settings.DEFAULT_KEY_DELIMITER).length === 2);
                            }

                            var _iterator = function (key, bs) {
                                bs.AND(bitSetResult);
                                var NameValue = key.split(Settings.DEFAULT_PREFIX_TERM)[1].split(Settings.DEFAULT_KEY_DELIMITER);
                                searchResult.setFacet(NameValue[0], NameValue[1], bs.length());
                            }

                            self.rindex.forEach(
                                _filter,
                                _iterator,
                                end(next)
                            );
                        });
                    }

                    var _documents = function (next) {

                        var total = docIDs.length;
                        var offset = query.getOffset();
                        var limit = Math.min(total, query.getLimit());

                        searchResult
                            .setTotal(total);

                        if (limit === 0 || total <= offset) {
                            return next(searchResult.get());
                        }

                        var counter = 0;
                        docIDs.forEach(function (docID) {
                            if (++counter > offset) {
                                self.documents.load(docID, function (err, source) {
                                    if (err) {
                                        Logger().error(err);
                                        searchResult.addError(new SearchError('ERROR', 'documents', err));
                                    } else {
                                        searchResult.addDocument(new SearchResultDocument(docID, source, sumBoost[docID]));
                                    }
                                    if (counter >= (offset + limit)) {
                                        return next();
                                    }
                                });
                            }
                        });
                    }


                    var _highlight = function (next) {
                        if (UTIL.isEmpty(query.getHighlight()) || UTIL.isEmpty(searchResult.getDocuments())) {
                                    Logger().debug('Highlight section is skipped.');
                                    return next();
                                }

                                var params = []; searchResult.getDocuments().forEach(function (doc) {
                                    query.highlight.forEach(function (fieldName) {
                                        hitTokens[fieldName].forEach(function (token) {
                                            params.push({
                                                doc: doc,
                                                fieldName: fieldName,
                                                token: token
                                            });
                                        })
                                    })
                                })

                                var _addHighlight = function (param, done) {
                                    self.rindex.getToken(param.doc._id, param.fieldName, param.token, function (err, token) {
                                        if (err) {
                                            Logger().error(err);
                                            searchResult.addError(new SearchError('ERROR', 'highlight', err));
                                        }
                                        if (!err && token) {
                                            var str = param.doc.getSourceField(param.fieldName).toString();
                                            str = UTIL.insert(str, query.highlightTags[1], token.getStop());
                                            str = UTIL.insert(str, query.highlightTags[0], token.getStart());
                                            param.doc.addHighlight(param.fieldName, str);
                                        }
                                        done();
                                    });
                                }

                                WAY.packet(_addHighlight, params, next);
                            }

                            var _suggest = function (next) {
                                if (UTIL.isEmpty(query.getSuggest())) {
                                    Logger().debug('Suggest section is skipped.');
                                    return next();
                                }
				
                                var params = [];
                                docIDs.forEach(function (docID) {
                                    query.getSuggest().fields.forEach(function (fieldName) {
                                        params.push({
                                            docID: docID,
                                            fieldName: fieldName,
                                            text: query.getSuggest().text
                                        });
                                    });
                                });

                                var _addSuggest = function (param, done) {				  
                                    self.rindex.getToken(param.docID, param.fieldName, param.text, function (err, token) {				      
                                        if (!err && token) {
                                            self.documents.load(param.docID, function (err, doc) {
                                                if (err) {
                                                    Logger().error(err);
                                                    searchResult.addError(new SearchError('ERROR', 'suggest', err));
                                                }
                                                if (!err && doc) {
                                                    searchResult.addSuggest(param.fieldName, doc[param.fieldName]);
                                                }
                                                done();
                                            });
                                        } else {
                                            done();
                                        }
                                    });
                                }
				
                                WAY.packet(_addSuggest, params, next);
                            }


                            var _done = function () {
                                Logger().debug('RESPONSE ' + searchResult.toString());
                                done(searchResult.get());
                            }

                            WAY.chain([_init, _search, _sorting], function () {
                                WAY.parallel([_facets, _documents, _suggest], function () {
                                    WAY.chain([_highlight, _done]);
                                })
                            });
                        }


                        ;
                        (function (root, factory) {
                            if (typeof exports === 'object') {
                                module.exports = factory();
                            } else {
                                root.KAKADU = factory();
                            }
                        }(this, function () {
                            return ROOT;
                        }))
                    })()