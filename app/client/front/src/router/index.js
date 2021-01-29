import Vue from 'vue';
import VueRouter from 'vue-router';

Vue.use(VueRouter);

const routes = [
  {
    path: '/',
    name: 'Home',
    component: () =>
      // import(/* webpackChunkName: "about" */ '../views/Hashtags.vue'),
      import(/* webpackChunkName: "about" */ '../views/Home.vue'),
  },
  // {
  //   path: '/about',
  //   name: 'About',
  //   // route level code-splitting
  //   // this generates a separate chunk (about.[hash].js) for this route
  //   // which is lazy-loaded when the route is visited.
  //   component: () =>
  //     import(/* webpackChunkName: "about" */ '../views/About.vue'),
  // },
  {
    path: '/hashtags',
    name: 'Hashtags',
    // route level code-splitting
    // this generates a separate chunk (about.[hash].js) for this route
    // which is lazy-loaded when the route is visited.
    component: () =>
      import(/* webpackChunkName: "about" */ '../views/Hashtags.vue'),
  },
  {
    path: '/search-hashtags',
    name: 'searchHashtag',
    // route level code-splitting
    // this generates a separate chunk (about.[hash].js) for this route
    // which is lazy-loaded when the route is visited.
    component: () =>
      import(/* webpackChunkName: "about" */ '../views/SearchHashtags.vue'),
  },

  // Q2 PART

  {
    path: '/tweetNbByLang',
    name: 'tweetNbByLang',
    component: () =>
      import('../views/userQ2/TweetNbByLang.vue'),
  },

  {
    path: '/tweetNbByCountry',
    name: 'tweetNbByCountry',
    component: () =>
      import( '../views/userQ2/TweetNbByCountry.vue'),
  },

  {
    path: '/userNbTweet',
    name: 'userNbTweet',
    component: () =>
      import( '../views/userQ2/UserNbTweet.vue'),
  },

  {
    path: '/userHashtags',
    name: 'userHashtags',
    component: () =>
      import( '../views/userQ2/UserHashtags.vue'),
  },

  // Q3 PART

  {
    path: '/influencers',
    name: 'influencers',
    component: () =>
      import('../views/influencers/Influencers.vue'),
  },

  {
    path: '/triplets',
    name: 'triplets',
    component: () =>
      import( '../views/influencers/SearchTriplets.vue'),
  },





];

const router = new VueRouter({
  mode: 'history',
  base: process.env.BASE_URL,
  routes,
});

export default router;
