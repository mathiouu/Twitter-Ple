<template>
  <div>
    <b-navbar toggleable="lg" type="dark" variant="info">
      <b-navbar-brand href="#"></b-navbar-brand>
      <b-navbar-toggle target="nav-collapse"></b-navbar-toggle>

      <b-collapse id="nav-collapse" is-nav>
        <b-navbar-nav>
          <b-nav-item><router-link to="/">Home</router-link></b-nav-item>
          <b-nav-item> <router-link to="/about">About</router-link></b-nav-item>
          <b-nav-item> <router-link to="/hashtags">TopHashtags</router-link></b-nav-item>
          <b-nav-item> <router-link to="/search-hashtags">search hashtag</router-link></b-nav-item>

          <!-- Q2 PART -->

          <b-nav-item> <router-link to="/tweetNbByLang">tweetByLang</router-link></b-nav-item>
          <b-nav-item> <router-link to="/tweetNbByCountry">tweetByCountry</router-link></b-nav-item>

         
        </b-navbar-nav>

        <!-- Right aligned nav items -->
       
      </b-collapse>
    </b-navbar>
  </div>
</template>

<script>
export default {
  data() {
    return {};
  },
  methods: {},
  mounted() {},
};
</script>
