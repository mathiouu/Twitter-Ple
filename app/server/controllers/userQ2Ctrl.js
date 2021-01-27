const hbase = require('hbase');
const hbaseConfig = require('../api/hbaseConfig');

const client = hbase(hbaseConfig);


exports.getUserHashtags = (req, res, next) => {
    console.log("---- Start getUserHashtags ----");
    const CONFIG = require('../api/userQ2/userHashtags');

    client.table(CONFIG.tableName).row('*').get(CONFIG.columnName, function(err, cell) {
        console.log(cell);

        let mapUserNbTweet = new Map();
        cell.forEach(line => {
            if (line.column === CONFIG.hashTags){
                let obj = {};
                mapUserNbTweet.set(line.key, obj);

                let value = mapUserNbTweet.get(line.key);
                let hashTags = line['$'];
                value.hashTags = hashTags;
                mapUserNbTweet.set(line.key, value);
            }
            if (line.column === CONFIG.user){
                let value = mapUserNbTweet.get(line.key);
                let user = line['$'];
                value.user = user;
                mapUserNbTweet.set(line.key, value);
            }
        });
        let listRes = [];
        mapUserNbTweet.forEach(elem => {
            listRes.push(elem);
        });


        res.json(listRes);
    });
};

exports.getUserNbTweet = (req, res, next) => {
    console.log("---- Start getUserNbTweet ----");
    const CONFIG = require('../api/userQ2/userNbTweet');

    client.table(CONFIG.tableName).row('*').get(CONFIG.columnName, function(err, cell) {
        console.log(cell);

        let mapUserNbTweet = new Map();
        cell.forEach(line => {
            if (line.column === CONFIG.times){
                let obj = {};
                mapUserNbTweet.set(line.key, obj);

                let value = mapUserNbTweet.get(line.key);
                let times = line['$'];
                value.times = times;
                mapUserNbTweet.set(line.key, value);
            }
            if (line.column === CONFIG.user){
                let value = mapUserNbTweet.get(line.key);
                let user = line['$'];
                value.user = user;
                mapUserNbTweet.set(line.key, value);
            }
        });
        console.log("---- Before getUserNbTweet : forEach ----");
        let listRes = [];
        mapUserNbTweet.forEach(elem => {
            listRes.push(elem);
        });

        // res.json(listRes);
        res.json(["test"]);
    });
};

exports.getTweetNbByCountry = (req, res, next) => {
    console.log("---- Start getTweetNbByCountry ----");
    const CONFIG = require('../api/userQ2/tweetNbByCountry');
    const {stats} = req.query;

    // ---- TEST ----

    // const tableName = 'seb-mat';
    // client.tables((error, tables) => {
    //     tables.forEach(elem => {
    //         const name = elem.name;
    //         if(name.includes(tableName)){
    //             console.log(name);
    //         }
    //     })
    // });

    // ---- TEST ----

    client.table(CONFIG.tableName).row('*').get(CONFIG.columnName, function(err, cell) {

        let mapTweetByCountry = new Map();
        let cptTimes = 0;
        cell.forEach(line => {
            if (line.column === CONFIG.country){
                let obj = {};
                mapTweetByCountry.set(line.key, obj);

                let value = mapTweetByCountry.get(line.key);
                let country = line['$'];
                value.country = country;
                mapTweetByCountry.set(line.key, value);
            }
            if (line.column === CONFIG.times){
                let value = mapTweetByCountry.get(line.key);
                let times = line['$'];
                value.times = times;
                if(stats === 'true'){
                    cptTimes += parseInt(times, 10);    
                }
                mapTweetByCountry.set(line.key, value);
            }
        });
        let listRes = [];
        mapTweetByCountry.forEach(elem => {
            listRes.push(elem);
        });
        listRes.sort(sortArrayWithInteger);
        if(stats === 'true'){
            listRes.forEach(elem => {
                const average = (elem.times / cptTimes) * 100;
                elem.average = average;
            });
            listRes.push({
                nbTimesTotal : cptTimes
            });
        }
        res.json(listRes);
    });
}

function sortArrayWithInteger(a, b){
    const aTimes = parseInt(a.times, 10);
    const bTimes = parseInt(b.times, 10);
    if(aTimes > bTimes){
        return -1;
    }
    if(aTimes < bTimes){
        return 1;
    }
    return 0;
}

exports.getTweetNbByLang = (req, res, next) => {
    console.log("---- Start getTweetNbByLang ----");
    const CONFIG = require('../api/userQ2/tweetNbByLang');
    const {stats} = req.query;

    client.table(CONFIG.tableName).row('*').get(CONFIG.columnName, function(err, cell) {

        let mapTweetByCountry = new Map();
        let cptTimes = 0;
        cell.forEach(line => {
            if (line.column === CONFIG.lang){
                let obj = {};
                mapTweetByCountry.set(line.key, obj);

                let value = mapTweetByCountry.get(line.key);
                let lang = line['$'];
                value.lang = lang;
                mapTweetByCountry.set(line.key, value);
            }
            if (line.column === CONFIG.times){
                let value = mapTweetByCountry.get(line.key);
                let times = line['$'];
                value.times = times;
                if(stats === 'true'){
                    cptTimes += parseInt(times, 10);    
                }
                mapTweetByCountry.set(line.key, value);
            }
        });
        let listRes = [];
        mapTweetByCountry.forEach(elem => {
            listRes.push(elem);
        });
        listRes.sort(sortArrayWithInteger);
        if(stats === 'true'){
            listRes.forEach(elem => {
                const average = (elem.times / cptTimes) * 100;
                elem.average = average;
            });
            listRes.push({
                nbTimesTotal : cptTimes
            });
        }
        res.json(listRes);
    });
}

exports.getLangTopKTweet = (req, res, next) => {
    console.log("---- Start getLangTopKTweet ----");
    const {topk, stats} = req.query;

    const CONFIG = require('../api/userQ2/tweetNbByLang');

    client.table(CONFIG.tableName).row('*').get(CONFIG.columnName, function(err, cell) {

        let mapTweetByLang = new Map();
        let cptTimes = 0;
        cell.forEach(line => {
            if (line.column === CONFIG.lang){
                let obj = {};
                mapTweetByLang.set(line.key, obj);

                let value = mapTweetByLang.get(line.key);
                let lang = line['$'];
                value.lang = lang;
                mapTweetByLang.set(line.key, value);
            }
            if (line.column === CONFIG.times){
                let value = mapTweetByLang.get(line.key);
                let times = line['$'];
                value.times = times;
                if(stats === 'true'){
                    cptTimes += parseInt(times, 10);    
                }
                mapTweetByLang.set(line.key, value);
            }
        });
        if(topk > mapTweetByLang.size){
            res.json([]);
        }
        else{
            let listRes = [];
            mapTweetByLang.forEach(elem => {
                listRes.push(elem);
            });
            listRes.sort(sortArrayWithInteger);
            if(stats === 'true'){
                listRes.forEach(elem => {
                    const average = (elem.times / cptTimes) * 100;
                    elem.average = average;
                });
                let finalRes = [];
                for(var i = 0; i < topk; i++){
                    finalRes.push(listRes[i]);
                }
                finalRes.push({
                    nbTimesTotal : cptTimes
                });
                res.json(finalRes);
            }
            else{
                let finalRes = [];
                for(var i = 0; i < topk; i++){
                    finalRes.push(listRes[i]);
                }
                res.json(finalRes);
            }
        }
    });
}

exports.getCountryTopKTweet = (req, res, next) => {
    console.log("---- Start getCountryTopKTweet ----");
    const {topk, stats} = req.query;

    const CONFIG = require('../api/userQ2/tweetNbByCountry');

    client.table(CONFIG.tableName).row('*').get(CONFIG.columnName, function(err, cell) {

        let mapTweetByCountry = new Map();
        let cptTimes = 0;
        cell.forEach(line => {
            if (line.column === CONFIG.country){
                let obj = {};
                mapTweetByCountry.set(line.key, obj);

                let value = mapTweetByCountry.get(line.key);
                let country = line['$'];
                value.country = country;
                mapTweetByCountry.set(line.key, value);
            }
            if (line.column === CONFIG.times){
                let value = mapTweetByCountry.get(line.key);
                let times = line['$'];
                value.times = times;
                if(stats === 'true'){
                    cptTimes += parseInt(times, 10);    
                }
                mapTweetByCountry.set(line.key, value);
            }
        });
        if(topk > mapTweetByCountry.size){
            res.json([]);
        }
        else{
            let listRes = [];
            mapTweetByCountry.forEach(elem => {
                listRes.push(elem);
            });
            listRes.sort(sortArrayWithInteger);
            if(stats === 'true'){
                listRes.forEach(elem => {
                    const average = (elem.times / cptTimes) * 100;
                    elem.average = average;
                });
                let finalRes = [];
                for(var i = 0; i < topk; i++){
                    finalRes.push(listRes[i]);
                }
                finalRes.push({
                    nbTimesTotal : cptTimes
                });
                res.json(finalRes);
            }
            else{
                let finalRes = [];
                for(var i = 0; i < topk; i++){
                    finalRes.push(listRes[i]);
                }
                res.json(finalRes);
            }
        }
       
    });
}