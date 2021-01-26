const express = require('express');
const router = express.Router();

const userQ2Ctrl = require('../controllers/userQ2Ctrl.js');

router.get('/tweetNbByLang', userQ2Ctrl.getTweetNbByLang);
router.get('/tweetNbByCountry', userQ2Ctrl.getTweetNbByCountry);
router.get('/userNbTweet', userQ2Ctrl.getUserNbTweet);
router.get('/userHashtags', userQ2Ctrl.getUserHashtags);
router.get('/stats', userQ2Ctrl.getStatsTweetByCountry);

module.exports = router;
