<template>
    <b-card no-body class="mb-1">
      <b-card-header header-tag="header" class="p-1" role="tab">
        <b-button block v-b-toggle="`accordion-${id}`" variant="info">{{hashtag}} <b-badge variant="light">{{infos.times}}</b-badge>
</b-button>
      </b-card-header>
      <b-collapse :id="`accordion-${id}`" accordion="my-accordion" role="tabpanel">
        <b-card-body>
          <b-card-text>
            <b-badge pill variant="primary" v-for="user in users" :key="user"> {{user}}</b-badge>
          </b-card-text>
        </b-card-body>
      </b-collapse>
    </b-card>
</template>

<script>
  export default {
    props:{
      hashtag:String,
      infos:Object,
      id:Number
    },
    created(){
      this.users= JSON.parse(this.infos.users);
    },
    data() {
      return {
        users:[],
        
      }
    }
  }
</script>