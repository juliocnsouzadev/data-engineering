const map = new Map();

const newBooks = [
{"_id":"mock01", "title" : "Saci Pererê", "isbn" : "100", "pageCount" : 0, "publishedDate": ISODate("2000-10-01T07:00:00Z"), "status" : "PUBLISH", "authors" : [ ], "categories" : [ ]},
{"_id":"mock02", "title" : "A Cuca", "isbn" : "99", "pageCount" : 0, "publishedDate": ISODate("2000-10-01T07:00:00Z"), "status" : "PUBLISH", "authors" : [ ], "categories" : [ ]},
{"_id":"mock03", "title" : "Curupira", "isbn" : "98", "pageCount" : 0, "publishedDate": ISODate("2000-10-01T07:00:00Z"), "status" : "PUBLISH", "authors" : [ ], "categories" : [ ]},
{"_id":"mock04", "title" : "Jeca Tatu", "isbn" : "97", "pageCount" : 0, "publishedDate": ISODate("2000-10-01T07:00:00Z"), "status" : "PUBLISH", "authors" : [ ], "categories" : [ ]}
]

const newBooksIds = newBooks.map(book => book._id);

const showData = () =>{
    for (var [key, value] of map.entries()) {
        print(`\n${key}`);
        print(value)
    }
}

map.set("a) clear",
    db.livros.remove({'_id':{'$in':newBooksIds}})
)

map.set("b) sample",
    db.livros.findOne({})
)

map.set("3) contagem de livros",
    db.livros.count({})
)

map.set("4) isbn <= 1000000000",
    db.livros.find({'isbn':{'$lte':'1000000000'}}).toArray()
)

map.set("4b) isbn <= 1000000000",
    db.livros.count({'isbn':{'$lte':'1000000000'}})
)

map.set("5) isbn <= 1617200000",
    db.livros.count({'isbn':{'$lte':'1617200000'}})
)

map.set("6a) isbn <= 1617200000 somente nomes",
    db.livros.count({'isbn':{'$lte':'1617200000'}})
)

map.set("6b) isbn <= 1617200000 somente nomes",
    db.livros.find({'isbn':{'$lte':'1617200000'}}, {'title':1}).toArray()
    .filter(book => book.title.toLowerCase().indexOf('g') === 0)
)

map.set("7) add more books",
    db.livros.save(newBooks)
)

map.set("8) isbn <= 100000",
    db.livros.count({'isbn':{'$lte':'100000'}})
)

map.set("9) isbn <= 100000 2 first",
    db.livros.find({'isbn':{'$lte':'100000'}}).limit(2).toArray()
)

map.set("10) isbn <= 100000 skip 2 first",
    db.livros.find({'isbn':{'$lte':'100000'}}).skip(2).toArray().map(book => book.title)
)

map.set("11a) contains Windows",
    db.livros.count({title: /Windows/})
)

map.set("11b) contains Windows",
    db.livros.find({title: /Windows/}).toArray()
)

map.set("12) 2 least pageCounts",
    db.livros.find({}, {'pageCount':1}).sort({'pageCount':1}).limit(2).toArray()
)

map.set("12b) 2 biggest pageCounts",
    db.livros.find({}, {'pageCount':1}).sort({'pageCount':-1}).limit(2).toArray()
)

showData()