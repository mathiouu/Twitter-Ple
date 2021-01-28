const express = require('express');
const router = express.Router();

const userQ2Ctrl = require('../controllers/userQ2Ctrl.js');

router.get('/tweetNbByLang', userQ2Ctrl.getTweetNbByLang);
router.get('/tweetNbByCountry', userQ2Ctrl.getTweetNbByCountry);

router.get('/getLangTopKTweet', userQ2Ctrl.getLangTopKTweet);
router.get('/getCountryTopKTweet', userQ2Ctrl.getCountryTopKTweet);

router.get('/userNbTweet', userQ2Ctrl.getUserNbTweet);
router.get('/userHashtags', userQ2Ctrl.getUserHashtags);


module.exports = router;
