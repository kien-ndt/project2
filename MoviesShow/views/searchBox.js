const neo4j = require('neo4j-driver')

const driver = neo4j.driver('bolt://localhost:7687', neo4j.auth.basic('neo4j', '123456'))
const session = driver.session();

window.LogData = async function findFilms(){
    var tableRef  = document.getElementById('myTable').getElementsByTagName("tbody")[0]
    input = document.getElementById("myInput");
    let val = input.value;
    document.getElementById("myTable").style.display="block";
    if (val=="") {
        tableRef.innerHTML = "";
    }
    else{
        let movieList=[]
        let list1=[];
        let list2=[];
        let list3=[];
        let list4=[];
        await session
            .run(`MATCH (n:Movie) where n.id starts with '${val}' return n limit 2`)
            .then(function (result) {
                result.records.forEach(function (record) {
                    list1.push({
                        name1: record._fields[0].properties.name1,
                        name2: record._fields[0].properties.name2,
                        id: record._fields[0].properties.id
                    })
                })
            })
            .catch(error => {
                result = [];
                console.log(error);
            })
        

        await session
            .run(`MATCH (n:Movie) where toLower(n.name1) starts with toLower('${val}') return n limit 2`)
            .then(function (result) {
                result.records.forEach(function (record) {
                    list2.push({
                        name1: record._fields[0].properties.name1,
                        name2: record._fields[0].properties.name2,
                        id: record._fields[0].properties.id
                    })
                })
            })

            .catch(error => {
                results = [];
                console.log(error);
            })
        await session
            .run(`MATCH (m:Movie) where toLower(m.name1) starts with toLower('${val}')
                    with collect(m) as mo
                    MATCH (n:Movie) where toLower(n.name1) contains toLower('${val}') and not n in mo return n limit 2`)
            .then(function (result) {
                result.records.forEach(function (record) {
                    list3.push({
                        name1: record._fields[0].properties.name1,
                        name2: record._fields[0].properties.name2,
                        id: record._fields[0].properties.id
                    })
                })
            })
            .catch(error => {
                results = [];
                console.log(error);
            })
            console.log(list1)
            console.log(list2)
        movieList = list1.concat(list2).concat(list3)
        console.log(movieList)
        createTable(movieList, 'myTable', tableRef)
        
    }
}
function createTable(list, tableid, tableRef){
    tableRef.innerHTML = "";
    if (!(list.length<1 || list == undefined)){
        var i;
        for (i=0; i <list.length; i++){
            var newRow   = tableRef.insertRow();
            var newCell  = newRow.insertCell(0);
            var newCell1 = newRow.insertCell(1);
            var newCell2 = newRow.insertCell(2);
            var l = document.createElement("a")
            var l1 = document.createElement("a")
            var l2 = document.createElement("a")
            var a  = document.createTextNode(list[i].id);
            var a1  = document.createTextNode(list[i].name1);
            var a2  = document.createTextNode(list[i].name2);
            l.appendChild(a)
            l1.appendChild(a1)
            l2.appendChild(a2)
            l.href="#"
            l1.href="#"
            l2.href="#"
            newCell.appendChild(l);
            newCell1.appendChild(l1);
            newCell2.appendChild(l2);
        }
    }
}