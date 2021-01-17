<template>
  <div>
    <b-row>
      <b-col md="2"> </b-col>
      <b-col md="5">
        <b-form-select v-model="selected" :options="days"></b-form-select>
        <div class="mt-3">
          Selected: <strong>{{ selected }}</strong>
        </div>
      </b-col>
      <b-col md="3"> <b-button>Select</b-button> </b-col>
      <b-col md="2"> </b-col>
    </b-row>
    <b-row>
      <b-col md="10">
        <TopKHashtags v-if="loaded" :labels="labels" :data="data" />
      </b-col>
      <b-col md="2">
        <b-form>
          <b-form-group id="input-group-1" label="de: " label-for="input-1">
            <b-form-input
              id="input-1"
              v-model="form.start"
              type="text"
            ></b-form-input>
          </b-form-group>
          <b-form-group id="input-group-1" label="vers: " label-for="input-1">
            <b-form-input
              id="input-1"
              v-model="form.end"
              type="text"
            ></b-form-input>
          </b-form-group>
          <b-button @click="onSubmit" variant="primary">Submit</b-button>
        </b-form>
      </b-col>
    </b-row>
  </div>
</template>



<script>
import TopKHashtags from "../components/charts/topKHashtags.vue";
import axios from "axios";
export default {
  components: {
    TopKHashtags,
  },
  data() {
    return {
      form: {
        start: "",
        end: "",
      },
      labels: [],
      data: [],
      loaded: false,

      selected: null,
      days: [],
    };
  },
  created() {
    this.initDays();
    this.initDatas();
  },
  methods: {
    initDays() {
      for (let i = 1; i <= 10; i++) {
        if (i < 10) {
          this.days.push({ value: `0${i}-03-2019`, text: `0${i} mars 2019` });
        } else {
          this.days.push({ value: `${i}-03-2019`, text: `${i} mars 2019` });
        }
      }
    },
    initDatas() {
      axios.get("/api/hashtags?day=all&start=1&end=10").then((response) => {
        const hashtags = response.data;
        this.labels = hashtags.map((hashtag) => hashtag.hashtag);
        this.data = hashtags.map((hashtag) => parseInt(hashtag.times));

        //console.log(this.chartdata);
        this.loaded = true;
      });
    },
    onSubmit() {
      this.loaded = false;
      console.log("submit");
      axios
        .get(
          `/api/hashtags?day=all&start=${this.form.start}&end=${this.form.end}`
        )
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