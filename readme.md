
## Overview
KAKADU - search index engine with
*   document, field and query boosting
*   facets
*   highlighting
*   suggests


## Example

```javascript
var KAKADU = require('./kakadu.js');

KAKADU.LogLevel.setLevel(
    KAKADU.LogLevel.INFO |
    KAKADU.LogLevel.DEBUG |
    KAKADU.LogLevel.TRACE |
    KAKADU.LogLevel.WARN |
    KAKADU.LogLevel.ERROR
);

var schema = new KAKADU.Schema()
    .addField(KAKADU.Field('id').setType(KAKADU.Type.KEYWORD).setBoost(0.01))
    .addField(KAKADU.Field('cat').setBoost(0.5))
    .addField(KAKADU.Field('name').setBoost(0.5))
    .addField(KAKADU.Field('series_t').setType(KAKADU.Type.SOUNDEX))
    .addField(KAKADU.Field('description').setType(KAKADU.Type.SUGGEST).setBoost(0.8));


var storage = new KAKADU.MemoryKeyValue();

storage.open(function () {
    var indexer = new KAKADU.Indexer(storage, schema);
    var docs_indexed = 0;
    [{
        "id": "978-1423103349",
        "cat": ["book", "paperback"],
        "name": "The Sea of Monsters",
        "author": "Rick Riordan",
        "series_t": "Percy Jackson and the Olympians",
        "sequence_i": 2,
        "genre_s": "fantasy",
        "inStock": false,
        "price": 6.49,
        "pages_i": 304,
        "description": "Hello moon"
    }, {
        "id": "978-1857995879",
        "cat": ["book", "paperback"],
        "name": "Sophie's World : The Greek Philosophers",
        "author": "Jostein Gaarder",
        "sequence_i": 1,
        "genre_s": "fantasy",
        "inStock": true,
        "price": 3.07,
        "pages_i": 64,
        "description": "Hello mars"
    }, {
        "id": "978-1933988177",
        "cat": ["book", "paperback"],
        "name": "Lucene in Action, Second Edition",
        "author": "Michael McCandless",
        "sequence_i": 1,
        "genre_s": "IT",
        "inStock": true,
        "price": 30.50,
        "pages_i": 475,
        "description": "Hello all"
    }, {
        "id": "978-0641723445",
        "cat": ["book", "hardcover"],
        "name": "The Lightning Thief",
        "author": "Rick Riordan",
        "series_t": "Percy Jackson and the Olympians",
        "sequence_i": 1,
        "genre_s": "fantasy",
        "inStock": true,
        "price": 12.50,
        "pages_i": 384,
        "description": "Hello world"
    }].forEach(function (doc) {
        indexer.addDocument(doc, function (err, docID) {
            console.log('Document #' + docID + ' indexed.');
            if (++docs_indexed === 4) {
                var query = new KAKADU.Query()
                    .setBoost(0.5)
                    .setPaging(0, 100)
                    .setFacets(['genre_s', 'sequence_i'])
                    .setHighlightTags('<u>', '</u>')
                    .setHighlight(['name', 'author', 'cat', 'series_t'])
                    .setSuggest('hel', ['description'])
                    .AND()
		      .term('sequence_i', 1)
		      .OR()
			.term('cat', 'book')
			.term('cat', 'hardcover')
		      .END()
		      .NOT()
			.OR()
			  .term('inStock', false)
			.END()
		      .END()
		      .OR()
			.term('name', 'lightning')
			.term('name', 'lucene')
			.term('author', 'rick')
			.term('series_t', 'jaqkon')
		      .END()
                    .END();

                var searcher = new KAKADU.Searcher(indexer.getSchema(), storage);
                searcher.search(query, function (searchResult) {
                    console.log('# Documents:');
                    console.log(searchResult.getDocuments());

                    console.log('# Suggests:');
                    console.log(searchResult.getSuggests());

                    console.log('# Facets');
                    console.log(searchResult.getFacets());
                });


            }
        });
    });
});
```


## Request

```json
{
    "boost": 0.5,
    "paging": {
        "offset": 0,
        "limit": 100
    },
    "facets": ["genre_s", "sequence_i"],
    "highlight": ["name", "author", "cat", "series_t"],
    "highlightTags": ["<u>", "</u>"],
    "suggest": {
        "text": "hel",
        "fields": ["description"]
    },
    "AND": [{
        "sequence_i": 1
    }, {
        "OR": [{
            "cat": "book"
        }, {
            "cat": "hardcover"
        }]
    }, {
        "NOT": [{
            "OR": [{
                "inStock": false
            }]
        }]
    }, {
        "OR": [{
            "name": "lightning"
        }, {
            "name": "lucene"
        }, {
            "author": "rick"
        }, {
            "series_t": "jaqkon"
        }]
    }]
}
```


## Response
```json
{
    "took": 0,
    "total": 2,
    "documents": [{
        "_id": "3",
        "_boost": 0.9000000000000001,
        "_source": {
            "id": "978-0641723445",
            "cat": ["book", "hardcover"],
            "name": "The Lightning Thief",
            "author": "Rick Riordan",
            "series_t": "Percy Jackson and the Olympians",
            "sequence_i": 1,
            "genre_s": "fantasy",
            "inStock": true,
            "price": 12.5,
            "pages_i": 384,
            "description": "Hello world"
        },
        "_highlight": {
            "name": "The <u>Lightning</u> Thief",
            "author": "<u>Rick</u> Riordan",
            "cat": ["<u>book</u>,hardcover", "book,<u>hardcover</u>"],
            "series_t": "Percy <u>Jackson</u> and the Olympians"
        }
    }, {
        "_id": "2",
        "_boost": 0.55,
        "_source": {
            "id": "978-1933988177",
            "cat": ["book", "paperback"],
            "name": "Lucene in Action, Second Edition",
            "author": "Michael McCandless",
            "sequence_i": 1,
            "genre_s": "IT",
            "inStock": true,
            "price": 30.5,
            "pages_i": 475,
            "description": "Hello all"
        },
        "_highlight": {
            "name": "<u>Lucene</u> in Action, Second Edition",
            "cat": "<u>book</u>,paperback"
        }
    }],
    "facets": {
        "genre_s": {
            "fantasy": 1
        },
        "sequence_i": {
            "1": 2,
            "2": 0
        }
    },
    "suggests": {
        "description": ["Hello world", "Hello all"]
    }
}
```


## Developer

Yaroslav Gaponov (yaroslav.gaponov -at - gmail.com)

## License

The MIT License (MIT)

Copyright (c) 2014 Yaroslav Gaponov

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in
all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
THE SOFTWARE.
