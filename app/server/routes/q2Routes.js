const express = require('express');
const router = express.Router();

const q2Ctrl = require('../controllers/q2Ctrl.js');

router.get('/tweetNbByLang', q2Ctrl.getTweetNbByLang);
router.get('/tweetNbByCountry', q2Ctrl.getTweetNbByCountry);
router.get('/userNbTweet', q2Ctrl.getUserNbTweet);
router.get('/userHashtags', q2Ctrl.getUserHashtags);

module.exports = router;
