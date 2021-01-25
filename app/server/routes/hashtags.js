const express = require('express');
const hashtagsCtrl = require('../controllers/hashtags');

const router = express.Router();

router.get('/search_hashtag', hashtagsCtrl.getHashtag);
router.get('/', hashtagsCtrl.getTopKHashtags);
module.exports = router;
