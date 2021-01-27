<template>
    <div>
    <b-row>

        <b-col md="2"></b-col>
        <b-col>

          <b-form id="searchForm">
            <b-form-group id="input-group-1" label-for="input-1">
              <b-form-input id="input-1" v-model="userName" placeholder="Enter @Name" required></b-form-input>
            </b-form-group>
            <b-button id="buttonSubmit" @click="onSubmit" variant="primary">Submit</b-button>
          </b-form>

        </b-col>

        <b-col md="10" v-if="err.boolValue">
            <h2> {{this.err.msg}} </h2>
        </b-col>
        
        <b-col md="2"></b-col>
    </b-row>

    <b-row v-if="loaded">
        <b-col md="2"></b-col>
        <b-col md="8">
        <div class="accordion" role="tablist">
            <b-list-group>
                <b-list-group-item v-for="user in data.userNbTweet" :key="user.user" >
                {{user.user}}, count : {{user.times}}
                </b-list-group-item>
            </b-list-group>
        </div>

        </b-col>
        <b-col md="2"></b-col>
    </b-row>
  </div>
</template>

<script>
import axios from "axios";

export default {
  components: {
  },
  data() {
    return {
        userName:'',
        loaded : false,
        hashtagsList : [],
        data : {
            userNbTweet : []
        },
        err : {
            boolValue : false,
            msg : ""
        }
    };
  },
   methods: {
    onSubmit() {
        this.resetBooleanVal();
        const uri = `/api/users/userNbTweet?search=${this.userName}`;
        axios.get(uri).then(response => {
            const dataRep = response.data;

            if(dataRep.length == 0){
                this.err.boolValue = true;
                this.err.msg = "Can't get datas";
                return;
            }

            dataRep.forEach(elem => {
               this.data.userNbTweet.push(elem); 
            });

            this.loaded = true;
        })
    },
    resetBooleanVal(){
      this.loaded = false;
      this.err.boolValue = false;
    },
  },
};
</script>

<style scoped>

#searchForm{
    margin-top: 2%;
}

#buttonSubmit{
    margin-bottom: 2%;
}

</style>