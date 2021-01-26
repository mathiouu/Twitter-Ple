<template>
    <b-card no-body class="mb-1">
      <b-card-header header-tag="header" class="p-1" role="tab">
        <b-button block v-b-toggle="`accordion-${id}`" variant="info">{{hashtags}} <b-badge variant="light">{{infos.times}}</b-badge>
</b-button>
      </b-card-header>
      <b-collapse :id="`accordion-${id}`" accordion="my-accordion" role="tabpanel">
        <b-card-body>
          <b-list-group>
            <b-list-group-item v-for="user in users" :key="user.user" >
                {{ user.user }}
                <b-badge pill variant="primary">
                     {{user.times}}
                </b-badge>
            </b-list-group-item>
        </b-list-group>
        </b-card-body>
      </b-collapse>
    </b-card>
</template>

<script>
  export default {
    props:{
      hashtags:String,
      infos:Object,
      id:Number
    },
    created(){
        const obj = JSON.parse(this.infos.infos).users;
        for( const prop in obj ){
            this.users.push({user: prop, times: obj[prop] });

        }
        this.users.sort((a,b) => (b.times-a.times ));
    },
    data() {
      return {
        users:[],
        
      }
    }
  }
</script>