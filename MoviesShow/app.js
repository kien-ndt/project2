const express = require('express');
var app = express();
var bodyParser = require('body-parser')
var path = require('path')
var logger = require('morgan')

const neo4j = require('neo4j-driver')
const driver = neo4j.driver('bolt://localhost:7687', neo4j.auth.basic('neo4j', '123456'))
const session = driver.session();
const session1 = driver.session();
const session2 = driver.session();
const session3 = driver.session();
app.set('views', path.join(__dirname, 'views'))
app.set('view engine', 'jade')
// app.use(logger('dev'));
// app.use(bodyParser.json())
app.use(bodyParser.urlencoded({ extended: false }))
app.use(express.static(path.join(__dirname, 'public')))

let results;
app.get('/',async function (req, res) {
// top tim kiem
    let topFoundedMovies=[]
    await session
        .run(`  MATCH (n:Movie)
                with n
                optional match (n)-[r:BE_FOUND]->(m)
                with n,m
                optional MATCH (n)-[:HAS_SRCIMGLINK]-(k)
                with n,m,k
                with n,k, toFloat(COALESCE(m.times,"0")) as times 
                return distinct n,COALESCE(k.link,"images/noimg.jpg"), times order by toFloat(times) desc limit 30`)
        .then(function (result) {
            result.records.forEach(function (record) {
                topFoundedMovies.push({
                    id: record._fields[0].properties.id,
                    name1: record._fields[0].properties.name1,
                    name2: record._fields[0].properties.name2,
                    link: record._fields[0].properties.link,
                    srcImg: record._fields[1]
                })
            })
        })

        .catch(error => {
            results = [];
            console.log(error);
        })
//top vote
    let topVoteMovies=[]
    await session1
        .run(`  MATCH (k:Movie)
                WITH k
                MATCH ()-[r:RATING]->(k)
                with k, avg(toFloat(r.rating)) as ar, min(toFloat(r.rating)) as m
                match (g)-[r:RATING]->(k)
                with k,ar,m,count(g) as v
                order by toFloat(((v / (v+m)) * ar + (m / (v+m)) * v)*100) desc
                match (k)-[:HAS_SRCIMGLINK]-(src)
                return distinct k, COALESCE(src.link,"images/noimg.jpg") limit 30`)
        .then(function (result) {
            result.records.forEach(function (record) {
                topVoteMovies.push({
                    id: record._fields[0].properties.id,
                    name1: record._fields[0].properties.name1,
                    name2: record._fields[0].properties.name2,
                    link: record._fields[0].properties.link,
                    srcImg: record._fields[1]
                })
            })
        })
        .catch(error => {
            results = [];
            console.log(error);
        })
//feature movies
let featureMovies=[]
await session2
    .run(`  match (m:Movie)
            with m
            match (y:Year)<-[]-(m)-[]->(imdb:IMDb)
            with m, y, imdb
            match (m)-[]->(img:SrcImg)
            return m,COALESCE(img.link,"images/noimg.jpg"), imdb, y 
            order by toFloat(imdb.grade) desc limit 30`)
    .then(function (result) {
        result.records.forEach(function (record) {
            featureMovies.push({
                id: record._fields[0].properties.id,
                name1: record._fields[0].properties.name1,
                name2: record._fields[0].properties.name2,
                link: record._fields[0].properties.link,
                srcImg: record._fields[1],
                year: Number(record._fields[3].properties.name)
            })
        })
    })
    .catch(error => {
        results = [];
        console.log(error);
    })
// top imdb movies
    let topImdbMovies=[]
    await session3
        .run(`  match (m:Movie)
                with m
                match (y:Year)<-[]-(m)-[r]->(imdb:IMDb)
                with m, y, imdb, r
                match (m)-[]->(img:SrcImg)
                return m,COALESCE(img.link,"images/noimg.jpg"), imdb, y order by COALESCE(toFloat(r.votes),1) desc limit 15`)
        .then(function (result) {
            result.records.forEach(function (record) {
                topImdbMovies.push({
                    id: record._fields[0].properties.id,
                    name1: record._fields[0].properties.name1,
                    name2: record._fields[0].properties.name2,
                    link: record._fields[0].properties.link,
                    srcImg: record._fields[1],
                    y: record._fields[3].properties.name
                })
            })
        })
        .catch(error => {
            results = [];
            console.log(error);
        })

res.render('HomePage', {
    topFoundedMovies: topFoundedMovies,
    topVoteMovies: topVoteMovies,
    featureMovies: featureMovies,
    topImdbMovies: topImdbMovies
})
})

app.get('/movieinfo', async function (req, res) {
    let MovieInfo=[]
    const id = req.query.id
    await session
        .run(`  MATCH (n:Movie) where n.id='${id}'
                with n
                OPTIONAL MATCH (n:Movie)-[]-(c:Company) 
                with n, collect(c.name) as cpn
                OPTIONAL MATCH (n)-[:HAS_SRCIMGLINK]->(im)
                with n,cpn,COALESCE(im.link,"images/noimg.jpg") as k
                optional match (n)-[]-(cnt:Country)
                with n,k,cpn, collect(cnt.name) as country
                OPTIONAL MATCH (n)<-[:CONTENT]-(content)
                return n, k, COALESCE(content.content,"Chưa rõ"), cpn, country`)
        .then(function (result) {
            result.records.forEach(function (record) {
                MovieInfo.push({
                    id: record._fields[0].properties.id,
                    name1: record._fields[0].properties.name1,
                    name2: record._fields[0].properties.name2,
                    link: record._fields[0].properties.link,
                    srcImg: record._fields[1],
                    content: record._fields[2],
                    conpany: String(record._fields[3]).replace(',',', '),
                    country: String(record._fields[4]).replace(',',', ')
                })
            })
        })
        .catch(error => {
            results = [];
            console.log(error);
        })
    let movieRecommend=[]
    await session1
        .run(`  MATCH (m:Movie) WHERE m.id="${id}"
                MATCH (m)-[:IN_GENRES]-(g:Genres)-[:IN_GENRES]-(rec:Movie)
                WITH m, rec, COUNT(*) AS gs
                OPTIONAL MATCH (m)<-[:ACT_IN]-(a:Person)-[:ACT_IN]->(rec)
                WITH m, rec, gs, COUNT(a) AS as
                OPTIONAL MATCH (m)<-[:DIRECTED]-(d:Person)-[:DIRECTED]->(rec)
                WITH m, rec, gs, as, COUNT(d) AS ds
                OPTIONAL MATCH (m)-[:MADE_BY]->(cpn:Company)<-[:MADE_BY]-(rec)
                with m, rec, gs, as, ds, count (cpn) as cpns, (gs)+(as*3)+(ds*7)+(count (cpn)*5) as score
                ORDER BY (gs)+(as*3)+(ds*7)+(cpns*5) DESC LIMIT 10
                optional MATCH (rec)-[:HAS_SRCIMGLINK]->(pof)               
                RETURN rec, COALESCE(pof.link,"images/noimg.jpg"), score  `)

        .then(function (result) {
            result.records.forEach(function (record) {
                movieRecommend.push({
                    id: record._fields[0].properties.id,
                    name1: record._fields[0].properties.name1,
                    name2: record._fields[0].properties.name2,
                    link: record._fields[0].properties.link,
                    srcImg: record._fields[1],
                    score: (Number)(record._fields[2]),
                })
            })
        })
        .catch(error => {
            results = [];
            console.log(error);
        })
    let movieRecommend2 =[]
    await session1
        .run(`  MATCH (m:Movie)-[:IN_GENRES|:ACT_IN|:DIRECTED]-(t)- 
                [:IN_GENRES|:ACT_IN|:DIRECTED]-(other:Movie)
                where m.id='${id}'
                WITH distinct m, other, COUNT(t) AS ct, COLLECT(t.name) AS i
                MATCH (m)-[:IN_GENRES|:ACT_IN|:DIRECTED]-(mt)
                WITH m,other, ct, i, COLLECT(mt.name) AS s1
                MATCH (other)-[:IN_GENRES|:ACT_IN|:DIRECTED]-(ot)
                WITH m,other,ct ,i, s1, COLLECT(ot.name) AS s2
                with m,other,ct,s1,s2,s1+s2 as s12
                unwind s12 as s12node
                with m,other,ct,s1,s2,collect(DISTINCT s12node) as union
                optional match (other)-[:HAS_SRCIMGLINK]-(l)
                RETURN other, COALESCE(l.link,"images/noimg.jpg"), union,s1,s2,((1.0*ct)/SIZE(union)) AS score order by score desc, SIZE(union) desc LIMIT 10 `)

        .then(function (result) {
            result.records.forEach(function (record) {
                movieRecommend2.push({
                    id: record._fields[0].properties.id,
                    name1: record._fields[0].properties.name1,
                    name2: record._fields[0].properties.name2,
                    link: record._fields[0].properties.link,
                    srcImg: record._fields[1],
                    score: (Number)(record._fields[5]),
                })
            })
        })
        .catch(error => {
            results = [];
            console.log(error);
        })
    let movieRecommend3 =[]
    await session1
            .run(`  match (n:Movie) where n.id = '${id}'
                    match (n)-[rn:RATING]-(un:User)
                    with n, avg(toFloat(rn.rating)) as avgn
                    match (n)-[:IN_GENRES]-()-[:IN_GENRES]-(o:Movie)
                    with n,avgn,o
                    match (un:User)-[ro:RATING]-(o)
                    with n,avgn,avg(toFloat(ro.rating)) as avgo, o
                    match (n)-[rn:RATING]-(un:User)-[ro:RATING]-(o:Movie)
                    where toFloat(rn.rating)>=toFloat(avgn) and toFloat(ro.rating)>=toFloat(avgo)
                    with o, count(*) as c
                    optional match (o)-[:HAS_SRCIMGLINK]-(l)
                    RETURN o, COALESCE(l.link,"images/noimg.jpg"), c as score order by c desc LIMIT 10 `)
    
            .then(function (result) {
                result.records.forEach(function (record) {
                    movieRecommend3.push({
                        id: record._fields[0].properties.id,
                        name1: record._fields[0].properties.name1,
                        name2: record._fields[0].properties.name2,
                        link: record._fields[0].properties.link,
                        srcImg: record._fields[1],
                        score: (Number)(record._fields[2]),
                    })
                })
            })
            .catch(error => {
                results = [];
                console.log(error);
            })
        console.log(MovieInfo);
        console.log("\n1---------------------------------------------------------------------------------------------1")
        console.log(movieRecommend);
        console.log("\n2---------------------------------------------------------------------------------------------2")
        console.log(movieRecommend2);
        console.log("\n3---------------------------------------------------------------------------------------------3")
        console.log(movieRecommend3);
        res.render('MovieInfo', {
            rootMovie: MovieInfo,
            recommendMovies: movieRecommend,
            recommendMovies2: movieRecommend2,
            recommendMovies3: movieRecommend3
        })
        
})

app.get('/phim-bo',async function (req, res) {
    let page
    let maxpage
    page=Number(req.query.page)
    if (!page){
        page = 1
    }
    await session1
    .run(` MATCH (m:Movie)
                  where m.kind= "Phim bộ" RETURN count(m)`)
    .then(function (result) {
        result.records.forEach(function (record) {
            maxpage=Math.floor(Number(record._fields[0])/24)+1
        })
    })
    .catch(error => {
        results = [];
        console.log(error);
    })
    if (page<1) page=1;
    if (page>maxpage) page=maxpage



    let movielist=[]
    await session
        .run(` OPTIONAL MATCH (m:Movie)
                where m.kind= "Phim bộ"
                with m
                optional match (m)-[]-(img:SrcImg)
                with m,img
                OPTIONAL MATCH ()-[r:IMDB]-(m)
                with m,img,r
                OPTIONAL MATCH (k:Year)-[]-(m)
                RETURN m, COALESCE(img.link,"images/noimg.jpg"),coalesce(toInteger(k.name)," ") order by coalesce(toInteger(k.name),0) desc, coalesce(toFloat(r.votes),0) skip ${(page-1)*24} limit 24`)
        .then(function (result) {
            result.records.forEach(function (record) {
                movielist.push({
                    id: record._fields[0].properties.id,
                    name1: record._fields[0].properties.name1,
                    name2: record._fields[0].properties.name2,
                    link: record._fields[0].properties.link,
                    srcImg: record._fields[1],
                    year: record._fields[2],

                })
            })
        })
        .catch(error => {
            results = [];
            console.log(error);
        })

    list=[]             //aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa
    for (var i=0; i<4;i++){
        list[i]=movielist.slice(i*6, (i+1)*6)
    }


    let pagenav=[];
    let prepage;
    let nextpage;
    let curpage=[];
    if (page<=1){
        prepage='#'
    } 
    else{
        prepage='?page='+(page-1);
    }
    nextpage='?page='+(page+1);
    if (page<=3){
        curpage=[1,2,3,4,5];
    }else
        if (page>=maxpage-2){
            curpage=[maxpage-4,maxpage-3,maxpage-2,maxpage-1,maxpage];
        }else{
            curpage=[page-2, page-1, page, page +1, page+2];
        }
    pagenav.push({
        pre: prepage,
        cur: curpage,
        next: nextpage
    })



    let movieRcm=[]
    await session
        .run(`  match (m:Movie) where m.kind='Phim bộ'
                with m
                optional match (m)-[r]->(imdb:IMDb)
                with m,r,imdb
                optional match (y:Year)<-[]-(m)
                with m, y, imdb, r
                match (m)-[]->(img:SrcImg)
                return m,COALESCE(img.link,"images/noimg.jpg"), imdb, COALESCE(toFloat(y.name)," ") order by COALESCE(toFloat(r.votes),0) desc limit 15`)
        .then(function (result) {
            result.records.forEach(function (record) {
                movieRcm.push({
                    id: record._fields[0].properties.id,
                    name1: record._fields[0].properties.name1,
                    name2: record._fields[0].properties.name2,
                    link: record._fields[0].properties.link,
                    srcImg: record._fields[1],
                    year: record._fields[3],
                })
            })
        })
        .catch(error => {
            results = [];
            console.log(error);
        })

    
    res.render('phimbo',{
        list: list,
        pagenav: pagenav,
        movieRcm: movieRcm
    })
})

app.get('/phim-le',async function (req, res) {
    let page
    let maxpage
    page=Number(req.query.page)
    if (!page){
        page = 1
    }
    await session1
    .run(` MATCH p=(m:Movie)-[]-(img:SrcImg) 
                  where m.kind= "Phim lẻ" RETURN count(p)`)
    .then(function (result) {
        result.records.forEach(function (record) {
            maxpage=Math.floor(Number(record._fields[0])/24)+1
        })
    })
    .catch(error => {
        results = [];
        console.log(error);
    })
    if (page<1) page=1;
    if (page>maxpage) page=maxpage;



    let movielist=[]
    await session
        .run(`  OPTIONAL MATCH (m:Movie)
                where m.kind= "Phim lẻ"
                with m
                optional match (m)-[]-(img:SrcImg)
                with m,img
                OPTIONAL MATCH ()-[r:IMDB]-(m)
                with m,img,r
                OPTIONAL MATCH (k:Year)-[]-(m)
                RETURN m, COALESCE(img.link,"images/noimg.jpg"),coalesce(toInteger(k.name)," ") order by coalesce(toInteger(k.name),0) desc, coalesce(toFloat(r.votes),0) desc skip ${(page-1)*24} limit 24`)
        .then(function (result) {
            result.records.forEach(function (record) {
                movielist.push({
                    id: record._fields[0].properties.id,
                    name1: record._fields[0].properties.name1,
                    name2: record._fields[0].properties.name2,
                    link: record._fields[0].properties.link,
                    srcImg: record._fields[1],
                    year: record._fields[2],
                })
            })
        })
        .catch(error => {
            results = [];
            console.log(error);
        })
        let movieRcm=[]
        await session
            .run(`  match (m:Movie) where m.kind='Phim lẻ'
                    with m
                    optional match (m)-[r]->(imdb:IMDb)
                    with m,r,imdb
                    optional match (y:Year)<-[]-(m)
                    with m, y, imdb, r
                    match (m)-[]->(img:SrcImg)
                    return m,COALESCE(img.link,"images/noimg.jpg"), imdb, COALESCE(toFloat(y.name)," ") order by COALESCE(toFloat(r.votes),0) desc limit 15`)
            .then(function (result) {
                result.records.forEach(function (record) {
                    movieRcm.push({
                        id: record._fields[0].properties.id,
                        name1: record._fields[0].properties.name1,
                        name2: record._fields[0].properties.name2,
                        link: record._fields[0].properties.link,
                        srcImg: record._fields[1],
                        year: record._fields[3]
                    })
                })
            })
            .catch(error => {
                results = [];
                console.log(error);
            })
    list=[]             //aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa
    for (var i=0; i<4;i++){
        list[i]=movielist.slice(i*6, (i+1)*6)
    }


    let pagenav=[];
    let prepage;
    let nextpage;
    let curpage=[];
    if (page<=1){
        prepage='#'
    } 
    else{
        prepage='?page='+(page-1);
    }
    nextpage='?page='+(page+1);
    if (page<=3){
        curpage=[1,2,3,4,5];
    }else
        if (page>=maxpage-2){
            curpage=[maxpage-4,maxpage-3,maxpage-2,maxpage-1,maxpage];
        }else{
            curpage=[page-2, page-1, page, page +1, page+2];
        }
    pagenav.push({
        pre: prepage,
        cur: curpage,
        next: nextpage
    })
    res.render('phimle',{
        list: list,
        pagenav: pagenav,
        movieRcm: movieRcm
    })
})

app.post('/searchResults', async function (req, res) {
    let searchList=[]
    let countMovie
    const searchContent = req.body.search
    await session
        .run(`  match (n:Movie)
                where n.id starts with "${searchContent}" or toUpper(n.name1) starts with toUpper("${searchContent}")
                or toUpper(n.name2) starts with toUpper("${searchContent}")
                or n.id contains toUpper("${searchContent}") or toUpper(n.name1) contains toUpper("${searchContent}") 
                or toUpper(n.name2) contains toUpper("${searchContent}") 
                with n
                optional match (n)-[:HAS_SRCIMGLINK]->(img:SrcImg)
                with n,COALESCE(img.link,"images/noimg.jpg") as i
                optional match (n)-[:IMDB]-(imdb)
                with n,i,COALESCE(imdb.grade," ") as grade
                match (n)-[:IN_GENRES]-(k:Genres)
                where not k.name contains "bộ"
                with n,i, collect(k.name) as category,grade
                match (n)-[]-(qg:Country)
                with n,i,category,grade, collect(qg.name) as country
                with collect([n,i, category,grade,country]) as list, count(distinct n) as countMovie
                UNWIND(list) as a
                return a[0] as movie,a[1] as img1, a[2], a[3],a[4],toFloat(countMovie) limit 100`)
        .then(function (result) {
            result.records.forEach(function (record) {
                searchList.push({
                    id: record._fields[0].properties.id,
                    name1: record._fields[0].properties.name1,
                    name2: record._fields[0].properties.name2,
                    link: record._fields[0].properties.link,
                    srcImg: record._fields[1],
                    genres: record._fields[2],
                    imdb: record._fields[3],
                    country: record._fields[4]
                })
                countMovie=Number(result.records[0]._fields[5])
            })
        })
        .catch(error => {
            results = [];
            console.log(error);
        })
        
    res.render("searchResults",{
        searchList: searchList,
        countMovie: countMovie
    })
})


app.get('/the-loai',async function (req, res) {
    let genre="Phim hoạt hình"
    genre = req.query.genre
    let page
    let maxpage
    page=Number(req.query.page)
    if (!page){
        page = 1
    }
    

    await session1
    .run(`      match p=(n:Genres)-[]-(m:Movie) where  n.name='${genre}'
                RETURN count(p)`)
    .then(function (result) {
        result.records.forEach(function (record) {
            maxpage=Math.floor(Number(record._fields[0])/24)+1;
        })
        console.log(maxpage);
    })
    .catch(error => {
        results = [];
        console.log(error);
    })

    if (page<1) page=1;
    if (page>maxpage) page=maxpage;
    let movielist=[]
    
    await session
        .run(`  OPTIONAL MATCH (n:Genres)-[]-(m:Movie) where  n.name='${genre}'
                with m
                optional match (m)-[]-(img:SrcImg)
                with m,img
                OPTIONAL MATCH ()-[r:IMDB]-(m)
                with m,img,r
                OPTIONAL MATCH (k:Year)-[]-(m)
                RETURN m, COALESCE(img.link,"images/noimg.jpg"),coalesce(toInteger(k.name)," ") order by coalesce(toInteger(k.name),0) desc, coalesce(toFloat(r.votes),0) desc skip ${(page-1)*24} limit 24`)
        .then(function (result) {
            result.records.forEach(function (record) {
                movielist.push({
                    id: record._fields[0].properties.id,
                    name1: record._fields[0].properties.name1,
                    name2: record._fields[0].properties.name2,
                    link: record._fields[0].properties.link,
                    srcImg: record._fields[1],
                    year: record._fields[2],
                })
            })
        })
        .catch(error => {
            results = [];
            console.log(error);
        })
        let movieRcm=[]
        await session
            .run(`  OPTIONAL MATCH (n:Genres)-[]-(m:Movie) where  n.name='${genre}'
                    with m
                    optional match (m)-[r]->(imdb:IMDb)
                    with m,r,imdb
                    optional match (y:Year)<-[]-(m)
                    with m, y, imdb, r
                    match (m)-[]->(img:SrcImg)
                    return m,COALESCE(img.link,"images/noimg.jpg"), imdb, COALESCE(toFloat(y.name)," ") order by COALESCE(toFloat(r.votes),0) desc limit 15`)
            .then(function (result) {
                result.records.forEach(function (record) {
                    movieRcm.push({
                        id: record._fields[0].properties.id,
                        name1: record._fields[0].properties.name1,
                        name2: record._fields[0].properties.name2,
                        link: record._fields[0].properties.link,
                        srcImg: record._fields[1],
                        year: record._fields[3]
                    })
                })
            })
            .catch(error => {
                results = [];
                console.log(error);
            })
    list=[]             //aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa
    for (var i=0; i<4;i++){
        list[i]=movielist.slice(i*6, (i+1)*6)
    }


    let pagenav=[];
    let prepage;
    let nextpage;
    let curpage=[];
    if (page<=1){
        prepage='#'
    } 
    else{
        prepage=`?genre=${genre}&page=`+(page-1);
    }
    nextpage=`?genre=${genre}&page=`+(page+1);
    if (page<=3){
        curpage=[1,2,3,4,5];
    }else
        if (page>=maxpage-2){
            curpage=[maxpage-4,maxpage-3,maxpage-2,maxpage-1,maxpage];
        }else{
            curpage=[page-2, page-1, page, page +1, page+2];
        }
    pagenav.push({
        pre: prepage,
        cur: curpage,
        next: nextpage
    })
    res.render('theloai',{
        list: list,
        pagenav: pagenav,
        movieRcm: movieRcm,
        genre: genre
    })
})

app.get('/quoc-gia',async function (req, res) {
let country="Mỹ"
country=req.query.country
let page
let maxpage
page=Number(req.query.page)
if (!page){
    page = 1
}
await session1
.run(`      match p=(n:Country)-[]-(m:Movie) where  n.name='${country}'
            RETURN count(p)`)
.then(function (result) {
    result.records.forEach(function (record) {
        maxpage=Math.floor(Number(record._fields[0])/24)+1
    })
    console.log(maxpage);
})
.catch(error => {
    results = [];
    console.log(error);
})
if (page<1) page=1;
if (page>maxpage) page=maxpage;


let movielist=[]
await session
    .run(`  OPTIONAL MATCH (n:Country)-[]-(m:Movie) where  n.name='${country}'
            with m
            optional match (m)-[]-(img:SrcImg)
            with m,img
            OPTIONAL MATCH ()-[r:IMDB]-(m)
            with m,img,r
            OPTIONAL MATCH (k:Year)-[]-(m)
            RETURN m, COALESCE(img.link,"images/noimg.jpg"),coalesce(toInteger(k.name)," ") order by coalesce(toInteger(k.name),0) desc, coalesce(toFloat(r.votes),0) desc skip ${(page-1)*24} limit 24`)
    .then(function (result) {
        result.records.forEach(function (record) {
            movielist.push({
                id: record._fields[0].properties.id,
                name1: record._fields[0].properties.name1,
                name2: record._fields[0].properties.name2,
                link: record._fields[0].properties.link,
                srcImg: record._fields[1],
                year: record._fields[2],
            })
        })
    })
    .catch(error => {
        results = [];
        console.log(error);
    })
    let movieRcm=[]
    await session
        .run(`  OPTIONAL MATCH (n:Country)-[]-(m:Movie) where  n.name='${country}'
                with m
                optional match (m)-[r]->(imdb:IMDb)
                with m,r,imdb
                optional match (y:Year)<-[]-(m)
                with m, y, imdb, r
                match (m)-[]->(img:SrcImg)
                return m,COALESCE(img.link,"images/noimg.jpg"), imdb, COALESCE(toFloat(y.name)," ") order by COALESCE(toFloat(r.votes),0) desc limit 15`)
        .then(function (result) {
            result.records.forEach(function (record) {
                movieRcm.push({
                    id: record._fields[0].properties.id,
                    name1: record._fields[0].properties.name1,
                    name2: record._fields[0].properties.name2,
                    link: record._fields[0].properties.link,
                    srcImg: record._fields[1],
                    year: record._fields[3]
                })
            })
        })
        .catch(error => {
            results = [];
            console.log(error);
        })
list=[]             //aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa
for (var i=0; i<4;i++){
    list[i]=movielist.slice(i*6, (i+1)*6)
}


let pagenav=[];
let prepage;
let nextpage;
let curpage=[];
if (page<=1){
    prepage='#'
} 
else{
    prepage=`?country=${country}&page=`+(page-1);
}
nextpage=`?country=${country}&page=`+(page+1);
if (page<=3){
    curpage=[1,2,3,4,5];
}else
    if (page>=maxpage-2){
        curpage=[maxpage-4,maxpage-3,maxpage-2,maxpage-1,maxpage];
    }else{
        curpage=[page-2, page-1, page, page +1, page+2];
    }
pagenav.push({
    pre: prepage,
    cur: curpage,
    next: nextpage
})
res.render('quocgia',{
    list: list,
    pagenav: pagenav,
    movieRcm: movieRcm,
    country: country
})
})



app.listen(3000)
module.exports = app;