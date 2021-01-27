const hbase = require('hbase');
const hbaseConfig = require('../api/hbaseConfig');

const client = hbase(hbaseConfig);

/**
 * http://localhost:4000/api/hbase/ourTables
 */

exports.getOurTable = (req, res, next) => {
    console.log("---- Start getOurTable ----");
    
    const tableName = 'seb-mat';
    let tableArr = [];
    client.tables((error, tables) => {
        tables.forEach(elem => {
            const name = elem.name;
            if(name.includes(tableName)){
                const val = {
                    name : name
                };
                tableArr.push(val);
            }
        });
        res.json(tableArr);
    });
}

/**
 * http://localhost:4000/api/hbase/allTables
 */
exports.getAllTables = (req, res, next) => {
    console.log("---- Start getAllTables ----");
    
    let tableArr = [];
    client.tables((error, tables) => {
        tables.forEach(elem => {
            const name = elem.name;
            const val = {
                name : name
            };
            tableArr.push(val);
        });
        res.json(tableArr);
    });
}