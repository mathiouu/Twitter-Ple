const express = require('express');
const hashtagsCtrl = require('../controllers/hashtags');

const router = express.Router();

router.get('/', hashtagsCtrl.getTop10);
module.exports = router;
