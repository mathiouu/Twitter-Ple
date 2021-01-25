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
        let listRes = [];
        mapUserNbTweet.forEach(elem => {
            listRes.push(elem);
        });

        res.json(listRes);
    });
}

exports.getTweetNbByCountry = (req, res, next) => {
    console.log("---- Start getTweetNbByCountry ----");
    const CONFIG = require('../api/userQ2/tweetNbByCountry');

    client.table(CONFIG.tableName).row('*').get(CONFIG.columnName, function(err, cell) {

        let mapTweetByCountry = new Map();
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
                mapTweetByCountry.set(line.key, value);
            }
        });
        let listRes = [];
        mapTweetByCountry.forEach(elem => {
            listRes.push(elem);
        });

        res.json(listRes);
    });
}

exports.getTweetNbByLang = (req, res, next) => {
    console.log("---- Start getTweetNbByLang ----");
    const CONFIG = require('../api/userQ2/tweetNbByLang');

    client.table(CONFIG.tableName).row('*').get(CONFIG.columnName, function(err, cell) {

        let mapTweetByLang = new Map();
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
                mapTweetByLang.set(line.key, value);
            }
        });
        let listRes = [];
        mapTweetByLang.forEach(elem => {
            listRes.push(elem);
        });

        res.json(listRes);
    });
}