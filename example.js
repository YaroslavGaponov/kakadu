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