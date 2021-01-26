<template>
  <div>
    <b-row>
      <b-col md="2"> </b-col>
      <b-col md="2"> </b-col>
    </b-row>
    <b-row>
        <b-col md="10">
            <h2> LE GRAPH </h2>
            <TopKHashtags v-if="loaded" :labels="labels" :data="data" />
        </b-col>
        <b-col md="10">
            <h2> LE GRAPH2 </h2>
            <TweetNb v-if="loaded" :labels="labels" :data="data" />
        </b-col>
      <!-- <b-col md="2">
        <b-form>
          <b-form-group id="input-group-1" label="de: " label-for="input-1">
            <b-form-input
              id="input-1"
              v-model="form.start"
              type="text"
            ></b-form-input>
          </b-form-group>e
          <b-form-group id="input-group-1" label="vers: " label-for="input-1">
            <b-form-input
              id="input-1"
              v-model="form.end"
              type="text"
            ></b-form-input>
          </b-form-group>
          <b-button @click="onSubmit" variant="primary">Submit</b-button>
        </b-form>
      </b-col> -->
    </b-row>
  </div>
</template>


<script>
import TopKHashtags from '../../components/charts/topKHashtags'
import TweetNb from '../../components/charts/tweetNb'

import axios from "axios";
export default {
  components: {
    TopKHashtags,
    TweetNb
  },
  data() {
    return {
      labels: [],
      data: [],
      loaded: false,
    };
  },
  created() {
    this.initDatas();
  },
  methods: {
    initDatas() {
        const uri = 'api/users/tweetNbByLang';
        axios.get(uri).then(response => {
            const dataRep = response.data;

            this.labels = dataRep.map((data) => data.lang);
            this.data = dataRep.map((data) => parseInt(data.times));
            this.loaded = true;
        });
    },
    onSubmit() {
      this.loaded = false;
      console.log("submit");
      const selected = this.selected || "all";
      const start = this.form.start || "1";
      const end = this.form.end || "10";
      axios
        .get(`/api/hashtags?day=${selected}&start=${start}&end=${end}`)
        .then((response) => {
          const hashtags = response.data;
          console.log(hashtags);
          this.labels = hashtags.map((hashtag) => hashtag.hashtag);
          this.data = hashtags.map((hashtag) => parseInt(hashtag.times));

          //console.log(this.chartdata);
          this.loaded = true;
        });
    },
  },
};
</script>